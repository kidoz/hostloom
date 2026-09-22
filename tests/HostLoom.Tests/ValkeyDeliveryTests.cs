using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using HostLoom.Caching;
using HostLoom.Locking;
using HostLoom.Valkey;
using ValkeyDotNet;
using Xunit;

namespace HostLoom.Tests;

public sealed class ValkeyDeliveryTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AmbiguousLockWrite_IsNotReplayedAndNextOperationRecovers(bool extend)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        deadline.CancelAfter(TimeSpan.FromSeconds(10));
        var token = deadline.Token;
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var serving = ServeAsync();
        await using var connection = new ValkeyConnection(
            new ValkeyOptions
            {
                Connection = new ValkeyClientOptions { Host = "127.0.0.1", Port = port },
            }
        );
        var provider = new ValkeyLockProvider(connection);
        try
        {
            var failure = await Assert.ThrowsAsync<LockProviderException>(() =>
                extend
                    ? provider
                        .ExtendAsync("catalog", "owner", TimeSpan.FromSeconds(10), token)
                        .AsTask()
                    : provider
                        .TryAcquireAsync("catalog", "owner", TimeSpan.FromSeconds(10), token)
                        .AsTask()
            );
            Assert.Equal(LockFailureKind.Unavailable, failure.Kind);
            Assert.Equal(
                ValkeyCommandDeliveryStatus.MayHaveBeenSent,
                Assert
                    .IsAssignableFrom<IValkeyCommandFailure>(failure.InnerException)
                    .DeliveryStatus
            );
            Assert.True((await provider.CheckHealthAsync(token)).IsHealthy);
            await serving;
        }
        finally
        {
            await deadline.CancelAsync();
            try
            {
                await serving;
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        }

        async Task ServeAsync()
        {
            using (var first = await listener.AcceptTcpClientAsync(token))
            {
                var stream = first.GetStream();
                await HandshakeAsync(stream, token);
                var write = await ReadCommandAsync(stream, token);
                Assert.Equal(extend ? "EVALSHA" : "SET", write[0]);
                // The server read the write but closes before replying: delivery is ambiguous.
            }
            using var next = await listener.AcceptTcpClientAsync(token);
            var recovered = next.GetStream();
            await HandshakeAsync(recovered, token);
            var command = await ReadCommandAsync(recovered, token);
            Assert.Equal("PING", Assert.Single(command));
            await recovered.WriteAsync("+PONG\r\n"u8.ToArray(), token);
        }
    }

    [Fact]
    public async Task First_subscription_flushes_and_acked_flapping_connections_keep_backoff()
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        deadline.CancelAfter(TimeSpan.FromSeconds(15));
        var token = deadline.Token;
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var clock = new TestClock();
        var acknowledgements = System.Threading.Channels.Channel.CreateUnbounded<int>();
        var allowFirstAck = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var subscribing = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var flushes = 0;
        var serving = ServeAsync();
        await using var connection = new ValkeyConnection(
            new ValkeyOptions
            {
                Connection = new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = ((IPEndPoint)listener.LocalEndpoint).Port,
                },
                InvalidationProbeInterval = TimeSpan.Zero,
            }
        );
        await using var channel = new ValkeyCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = "catalog" },
            null,
            clock
        );
        using var subscription = channel.Subscribe(message =>
        {
            if (message.FlushAll)
                Interlocked.Increment(ref flushes);
        });
        try
        {
            var starting = channel.StartAsync(token);
            await subscribing.Task.WaitAsync(token);
            Assert.False(starting.IsCompleted);
            Assert.Equal(0, Volatile.Read(ref flushes));
            allowFirstAck.SetResult();
            await starting;
            Assert.Equal(1, Volatile.Read(ref flushes));
            for (var attempt = 0; attempt < 4; attempt++)
            {
                Assert.Equal(attempt, await acknowledgements.Reader.ReadAsync(token));
                while (clock.PendingTimers == 0)
                    await Task.Delay(1, token);
                clock.Advance(TimeSpan.FromMilliseconds((100 * (1 << attempt)) - 1));
                Assert.Equal(1, clock.PendingTimers);
                clock.Advance(TimeSpan.FromMilliseconds(1));
            }
            Assert.Equal(4, await acknowledgements.Reader.ReadAsync(token));
        }
        finally
        {
            await deadline.CancelAsync();
            try
            {
                await serving;
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        }

        async Task ServeAsync()
        {
            for (var attempt = 0; attempt < 5; attempt++)
            {
                using var socket = await listener.AcceptTcpClientAsync(token);
                var stream = socket.GetStream();
                await HandshakeAsync(stream, token);
                var command = await ReadCommandAsync(stream, token);
                Assert.Equal("SUBSCRIBE", command[0], ignoreCase: true);
                if (attempt == 0)
                {
                    subscribing.SetResult();
                    await allowFirstAck.Task.WaitAsync(token);
                }
                var name = command[1];
                await stream.WriteAsync(
                    Encoding.UTF8.GetBytes(
                        $">3\r\n$9\r\nsubscribe\r\n${Encoding.UTF8.GetByteCount(name)}\r\n{name}\r\n:1\r\n"
                    ),
                    token
                );
                await acknowledgements.Writer.WriteAsync(attempt, token);
                // Closing after each acknowledgement reproduces an apparently successful flap.
                while (Volatile.Read(ref flushes) <= attempt)
                    await Task.Delay(1, token);
            }
        }
    }

    private static async Task HandshakeAsync(NetworkStream stream, CancellationToken token)
    {
        Assert.Equal("HELLO", (await ReadCommandAsync(stream, token))[0]);
        await stream.WriteAsync("%1\r\n$5\r\nproto\r\n:3\r\n"u8.ToArray(), token);
    }

    private static async Task<string[]> ReadCommandAsync(
        NetworkStream stream,
        CancellationToken token
    )
    {
        var header = await ReadLineAsync(stream, token);
        Assert.StartsWith("*", header, StringComparison.Ordinal);
        var count = int.Parse(header.AsSpan(1), CultureInfo.InvariantCulture);
        Assert.InRange(count, 1, 20);
        var arguments = new string[count];
        for (var index = 0; index < count; index++)
        {
            var lengthLine = await ReadLineAsync(stream, token);
            Assert.StartsWith("$", lengthLine, StringComparison.Ordinal);
            var length = int.Parse(lengthLine.AsSpan(1), CultureInfo.InvariantCulture);
            Assert.InRange(length, 0, 4096);
            var bytes = new byte[length];
            await stream.ReadExactlyAsync(bytes, token);
            arguments[index] = Encoding.UTF8.GetString(bytes);
            Assert.Empty(await ReadLineAsync(stream, token));
        }
        return arguments;
    }

    private static async Task<string> ReadLineAsync(NetworkStream stream, CancellationToken token)
    {
        var bytes = new List<byte>();
        var next = new byte[1];
        while (bytes.Count < 100)
        {
            await stream.ReadExactlyAsync(next, token);
            if (next[0] == '\n')
            {
                Assert.Equal((byte)'\r', bytes[^1]);
                return Encoding.ASCII.GetString(bytes.ToArray(), 0, bytes.Count - 1);
            }
            bytes.Add(next[0]);
        }
        throw new InvalidDataException("Test protocol header exceeds its bound.");
    }
}
