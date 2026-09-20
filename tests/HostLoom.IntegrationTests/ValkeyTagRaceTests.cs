using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using HostLoom.Valkey;
using ValkeyDotNet;
using Xunit;

namespace HostLoom.IntegrationTests;

public sealed class ValkeyTagRaceTests
{
    public static bool Available => ValkeyAvailability.Available;

    [Theory(Skip = ValkeyAvailability.Skip, SkipUnless = nameof(Available))]
    [InlineData(1)]
    [InlineData(501)]
    public async Task Concurrent_tag_members_survive_snapshot_cleanup_and_are_removed_next_time(
        int memberCount
    )
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        deadline.CancelAfter(TimeSpan.FromSeconds(30));
        var token = deadline.Token;
        await using var direct = new ValkeyConnection(ValkeyAvailability.Options());
        var writer = new ValkeyCacheStore(direct);
        var prefix = "tag-race-" + Guid.NewGuid().ToString("N");
        var tag = prefix + ":tag";
        var newKey = prefix + ":new";
        var keys = Enumerable.Range(0, memberCount).Select(i => prefix + ":" + i).ToArray();
        foreach (var key in keys)
            await writer.SetAsync(key, "old"u8.ToArray(), TimeSpan.FromMinutes(1), [tag], token);
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var servingStop = CancellationTokenSource.CreateLinkedTokenSource(token);
        var serving = Proxy();
        try
        {
            await using var proxy = new ValkeyConnection(
                new ValkeyOptions
                {
                    Connection = new ValkeyClientOptions
                    {
                        Host = "127.0.0.1",
                        Port = ((IPEndPoint)listener.LocalEndpoint).Port,
                    },
                }
            );
            var remover = new ValkeyCacheStore(proxy);
            await remover.RemoveByTagAsync(tag, token);
            Assert.NotNull(await writer.GetAsync(newKey, token));
            Assert.Empty(await writer.GetManyAsync(keys, token));
            await remover.RemoveByTagAsync(tag, token);
            Assert.Null(await writer.GetAsync(newKey, token));
        }
        finally
        {
            await servingStop.CancelAsync();
            try
            {
                await serving;
            }
            catch (OperationCanceledException) { }
            catch (EndOfStreamException) { }
            await writer.RemoveAsync([tag, newKey, .. keys], TestContext.Current.CancellationToken);
        }
        async Task Proxy()
        {
            var ct = servingStop.Token;
            using var client = await listener.AcceptTcpClientAsync(ct);
            using var server = new TcpClient();
            await server.ConnectAsync("127.0.0.1", ValkeyAvailability.Port, ct);
            var inbound = client.GetStream();
            var outbound = server.GetStream();
            bool injected = false;
            while (!ct.IsCancellationRequested)
            {
                var request = await ReadFrame(inbound, ct);
                await outbound.WriteAsync(request, ct);
                var reply = await ReadFrame(outbound, ct);
                if (
                    !injected
                    && Encoding
                        .UTF8.GetString(request)
                        .Contains("SMEMBERS", StringComparison.Ordinal)
                )
                {
                    injected = true;
                    await writer.SetAsync(
                        newKey,
                        "new"u8.ToArray(),
                        TimeSpan.FromMinutes(1),
                        [tag],
                        ct
                    );
                }
                await inbound.WriteAsync(reply, ct);
            }
        }
    }

    static async Task<byte[]> ReadFrame(NetworkStream stream, CancellationToken token)
    {
        var first = new byte[1];
        await stream.ReadExactlyAsync(first, token);
        using var output = new MemoryStream();
        output.Write(first);
        byte kind = first[0];
        var line = new List<byte>();
        do
        {
            await stream.ReadExactlyAsync(first, token);
            line.Add(first[0]);
        } while (first[0] != 10);
        output.Write(line.ToArray());
        string countText = Encoding.ASCII.GetString(line.ToArray()).TrimEnd('\r', '\n');
        if (kind is (byte)'$' or (byte)'=' or (byte)'!')
        {
            int count = int.Parse(countText, CultureInfo.InvariantCulture);
            if (count >= 0)
            {
                var payload = new byte[count + 2];
                await stream.ReadExactlyAsync(payload, token);
                output.Write(payload);
            }
        }
        else if (kind is (byte)'*' or (byte)'%' or (byte)'~' or (byte)'>' or (byte)'|')
        {
            int count = int.Parse(countText, CultureInfo.InvariantCulture);
            if (kind is (byte)'%' or (byte)'|')
                count *= 2;
            for (int i = 0; i < count; i++)
                output.Write(await ReadFrame(stream, token));
        }
        return output.ToArray();
    }
}
