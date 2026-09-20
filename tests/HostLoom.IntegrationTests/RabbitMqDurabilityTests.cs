using System.Diagnostics;
using System.Globalization;
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
