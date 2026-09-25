using System.Diagnostics;
using System.Globalization;
using System.Net.Http.Headers;
using System.Net.NetworkInformation;
using System.Text.Json;
using HostLoom.Transport.RabbitMq;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace HostLoom.IntegrationTests;

public sealed class RabbitMqDurabilityTests
{
    public static bool Enabled =>
        Environment.GetEnvironmentVariable("HOSTLOOM_RABBITMQ_DURABILITY") == "1";

    [Fact(
        Skip = "Set HOSTLOOM_RABBITMQ_DURABILITY=1 to start and restart an owned disposable RabbitMQ container.",
        SkipUnless = nameof(Enabled)
    )]
    public async Task Confirmed_backlog_survives_restart_and_can_be_drained_before_queue_migration()
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        deadline.CancelAfter(TimeSpan.FromMinutes(4));
        var token = deadline.Token;
        var name = "hostloom-durability-" + Guid.NewGuid().ToString("N");
        // Only the container created here is ever restarted/stopped; no shared endpoint fallback.
        var id = await Docker(
            token,
            "run",
            "--detach",
            "--rm",
            "--name",
            name,
            "--label",
            "hostloom.durability-owner=" + name,
            "--publish",
            "127.0.0.1::5672",
            "--health-cmd",
            "rabbitmq-diagnostics -q check_port_connectivity",
            "--health-interval",
            "2s",
            "--health-timeout",
            "5s",
            "--health-retries",
            "30",
            "rabbitmq:4-management"
        );
        try
        {
            await Healthy(id, token);
            var mapping = await Docker(token, "port", id, "5672/tcp");
            var port = int.Parse(mapping.Split(':')[^1], CultureInfo.InvariantCulture);
            var uri = new Uri($"amqp://guest:guest@127.0.0.1:{port}/");
            const string topic = "catalog";
            const string subscription = "inventory";
            await using (
                var old = new RabbitMqRequestBroker(
                    Options.Create(
                        new RabbitMqOptions { Uri = uri, QueueNaming = RabbitMqQueueNaming.Legacy }
                    )
                )
            )
            {
                var stopped = await old.SubscribeAsync(
                    topic,
                    subscription,
                    (_, _) => ValueTask.CompletedTask,
                    token
                );
                await stopped.DisposeAsync();
                await old.PublishAsync(topic, "before-restart"u8.ToArray(), token);
            }
            Assert.Equal(
                name,
                await Docker(
                    token,
                    "inspect",
                    "--format",
                    "{{index .Config.Labels \"hostloom.durability-owner\"}}",
                    id
                )
            );
            await Docker(token, "restart", id);
            await Healthy(id, token);
            // Docker may reassign an ephemeral host port when restarting the container.
            mapping = await Docker(token, "port", id, "5672/tcp");
            port = int.Parse(mapping.Split(':')[^1], CultureInfo.InvariantCulture);
            uri = new Uri($"amqp://guest:guest@127.0.0.1:{port}/");
            await using (var connection = await ConnectWhenReady(uri, token))
            await using (
                var channel = await connection.CreateChannelAsync(cancellationToken: token)
            )
            {
                var legacyQueue = RabbitMqQueueNames.Subscription(
                    topic,
                    subscription,
                    RabbitMqQueueNaming.Legacy
                );
                var backlog = await channel.BasicGetAsync(legacyQueue, autoAck: true, token);
                Assert.NotNull(backlog);
                Assert.True(backlog.BasicProperties.Persistent);
                Assert.Equal("before-restart"u8.ToArray(), backlog.Body.ToArray());
                Assert.Null(await channel.BasicGetAsync(legacyQueue, autoAck: true, token));
                // This queue belongs only to this fixture and has been drained before migration.
                await channel.QueueDeleteAsync(
                    legacyQueue,
                    ifEmpty: true,
                    cancellationToken: token
                );
            }
            await using var current = new RabbitMqRequestBroker(
                Options.Create(new RabbitMqOptions { Uri = uri })
            );
            var delivered = new TaskCompletionSource<byte[]>(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            await using var listener = await current.SubscribeAsync(
                topic,
                subscription,
                (bytes, _) =>
                {
                    delivered.TrySetResult(bytes.ToArray());
                    return ValueTask.CompletedTask;
                },
                token
            );
            await current.PublishAsync(topic, "after-migration"u8.ToArray(), token);
            Assert.Equal("after-migration"u8.ToArray(), await delivered.Task.WaitAsync(token));
        }
        finally
        {
            // Cleanup has its own deadline even after the experiment times out.
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await Docker(cleanup.Token, "stop", "--time", "5", id);
        }
    }

    [Fact(
        Skip = "Set HOSTLOOM_RABBITMQ_DURABILITY=1 to start and pause an owned disposable RabbitMQ container.",
        SkipUnless = nameof(Enabled)
    )]
    public async Task Disposal_against_a_paused_broker_returns_within_its_bound_and_leaves_no_connection()
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        deadline.CancelAfter(TimeSpan.FromMinutes(4));
        var token = deadline.Token;
        var run = Guid.NewGuid().ToString("N");
        // Only the container created here is ever paused; no shared endpoint fallback.
        var id = await Docker(
            token,
            "run",
            "--detach",
            "--name",
            "hostloom-chaos-" + run,
            "--label",
            "hostloom.chaos=" + run,
            "--publish",
            "127.0.0.1::5672",
            "--publish",
            "127.0.0.1::15672",
            "--health-cmd",
            "rabbitmq-diagnostics -q check_port_connectivity",
            "--health-interval",
            "2s",
            "--health-timeout",
            "5s",
            "--health-retries",
            "30",
            "rabbitmq:4-management"
        );
        var paused = false;
        try
        {
            await Healthy(id, token);
            var amqp = await Port(id, "5672/tcp", token);
            using var management = new HttpClient
            {
                BaseAddress = new Uri($"http://127.0.0.1:{await Port(id, "15672/tcp", token)}/"),
            };
            management.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
                "Basic",
                Convert.ToBase64String("guest:guest"u8)
            );
            var client = "hostloom-paused-" + run;
            // Disposed explicitly below, where it is timed; disposing again is a no-op.
            await using var broker = new RabbitMqRequestBroker(
                Options.Create(
                    new RabbitMqOptions
                    {
                        Uri = new Uri($"amqp://guest:guest@127.0.0.1:{amqp}/"),
                        ClientProvidedName = client,
                    }
                )
            );
            var listener = await broker.ListenAsync(
                "invoices",
                (frame, _) => ValueTask.FromResult(frame),
                token
            );
            var subscription = await broker.SubscribeAsync(
                "shipments",
                "audit",
                (_, _) => ValueTask.CompletedTask,
                token
            );
            // Every kind of channel is open: two consumers, the reply path, and a publisher.
            await broker.RequestAsync(
                "invoices",
                "invoice-1"u8.ToArray(),
                Guid.NewGuid(),
                TimeSpan.FromSeconds(10),
                token
            );
            await broker.PublishAsync("shipments", "shipment-1"u8.ToArray(), token);
            await ConnectionsNamed(management, client, expected: 1, token);

            await Docker(token, "pause", id);
            paused = true;
            var started = Stopwatch.GetTimestamp();
            await listener.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(60), token);
            await subscription.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(60), token);
            var stopped = Stopwatch.GetElapsedTime(started);
            await broker.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(90), token);
            var disposed = Stopwatch.GetElapsedTime(started) - stopped;
            var took = string.Create(
                CultureInfo.InvariantCulture,
                $"Against the paused broker the consumers stopped after {stopped.TotalSeconds:F2} s and the transport was disposed {disposed.TotalSeconds:F2} s later."
            );
            TestContext.Current.TestOutputHelper?.WriteLine(took);

            await Docker(token, "unpause", id);
            paused = false;
            // Resumed, the broker reads the close the transport sent and drops the connection;
            // nothing of it outlives the disposal.
            await ConnectionsNamed(management, client, expected: 0, token);
            var established = IPGlobalProperties
                .GetIPGlobalProperties()
                .GetActiveTcpConnections()
                .Count(connection =>
                    connection.RemoteEndPoint.Port == amqp
                    && connection.State == TcpState.Established
                );
            Assert.Equal(0, established);

            Assert.True(stopped < TimeSpan.FromSeconds(2), took);
            // Five seconds for closing channels, two for the connection, and slack for the host.
            Assert.True(disposed < TimeSpan.FromSeconds(5 + 2 + 2), took);
        }
        finally
        {
            // Cleanup has its own deadline even after the experiment times out.
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            if (paused)
            {
                await Docker(cleanup.Token, "unpause", id);
            }

            await Docker(cleanup.Token, "rm", "--force", "--volumes", id);
        }
    }

    /// <summary>
    /// Waits until the broker's management API lists exactly <paramref name="expected"/>
    /// connections named <paramref name="client"/>. The list is refreshed every few seconds, so
    /// a change takes that long to appear.
    /// </summary>
    private static async Task ConnectionsNamed(
        HttpClient management,
        string client,
        int expected,
        CancellationToken token
    )
    {
        using var bound = CancellationTokenSource.CreateLinkedTokenSource(token);
        bound.CancelAfter(TimeSpan.FromSeconds(60));
        var seen = -1;
        while (true)
        {
            try
            {
                using var document = JsonDocument.Parse(
                    await management.GetStringAsync(
                        new Uri("api/connections", UriKind.Relative),
                        bound.Token
                    )
                );
                seen = document
                    .RootElement.EnumerateArray()
                    .Count(connection =>
                        connection.TryGetProperty("client_properties", out var properties)
                        && properties.TryGetProperty("connection_name", out var name)
                        && name.GetString() == client
                    );
                if (seen == expected)
                {
                    return;
                }
            }
            catch (HttpRequestException)
            {
                // The management plugin starts after the AMQP listener the health check probes.
            }
            catch (OperationCanceledException) when (bound.IsCancellationRequested)
            {
                Assert.Fail(
                    $"The broker listed {seen} connections named {client}, not {expected}."
                );
            }

            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(500), bound.Token);
            }
            catch (OperationCanceledException) when (bound.IsCancellationRequested)
            {
                Assert.Fail(
                    $"The broker listed {seen} connections named {client}, not {expected}."
                );
            }
        }
    }

    private static async Task<int> Port(string id, string containerPort, CancellationToken token)
    {
        var mapping = await Docker(token, "port", id, containerPort);
        return int.Parse(mapping.Split('\n')[0].Split(':')[^1], CultureInfo.InvariantCulture);
    }

    private static async Task<IConnection> ConnectWhenReady(Uri uri, CancellationToken token)
    {
        while (true)
        {
            try
            {
                return await new ConnectionFactory
                {
                    Uri = uri,
                    RequestedConnectionTimeout = TimeSpan.FromSeconds(2),
                }.CreateConnectionAsync(token);
            }
            catch (BrokerUnreachableException)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100), token);
            }
        }
    }

    private static async Task Healthy(string id, CancellationToken token)
    {
        while (
            await Docker(token, "inspect", "--format", "{{.State.Health.Status}}", id) != "healthy"
        )
            await Task.Delay(TimeSpan.FromMilliseconds(500), token);
    }

    private static async Task<string> Docker(CancellationToken token, params string[] arguments)
    {
        var start = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var argument in arguments)
            start.ArgumentList.Add(argument);
        using var process =
            Process.Start(start) ?? throw new InvalidOperationException("Cannot start Docker.");
        var output = process.StandardOutput.ReadToEndAsync(token);
        var error = process.StandardError.ReadToEndAsync(token);
        try
        {
            await process.WaitForExitAsync(token);
        }
        catch (OperationCanceledException)
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
            throw;
        }
        var diagnostic = await error;
        Assert.True(process.ExitCode == 0, diagnostic);
        return (await output).Trim();
    }
}
