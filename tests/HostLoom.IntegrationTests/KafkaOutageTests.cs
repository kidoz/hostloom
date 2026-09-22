using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Transport.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Opt-in outage experiments, run with HOSTLOOM_KAFKA_CHAOS=1: each test starts its own
/// disposable single-node Kafka and freezes it with <c>docker pause</c>. The compose broker
/// cannot be faulted through a proxy, because clients bootstrap through it and then connect to
/// the advertised localhost:9092 directly; a container that advertises its own published port
/// has no such bypass. Only the container a test created is paused, resumed, or removed, and
/// tmpfs covers every volume the image declares, so no anonymous volume outlives it.
/// </summary>
[Collection(nameof(KafkaOutageTests))]
[CollectionDefinition(nameof(KafkaOutageTests), DisableParallelization = true)]
public sealed class KafkaOutageTests
{
    private const string Skip =
        "Set HOSTLOOM_KAFKA_CHAOS=1 to pause and resume an owned disposable Kafka container.";

    private const string ClientTag = "hostloom.kafka.client";
    private const string DestinationTag = "messaging.destination.name";
    private const string KindTag = "hostloom.kafka.kind";
    private const string OutcomeTag = "hostloom.kafka.outcome";
    private const string StageTag = "hostloom.kafka.stage";
    private const string Produced = "hostloom.kafka.produced";
    private const string LoopFaults = "hostloom.kafka.loop.faults";
    private const string Initializations = "hostloom.kafka.reply_consumer.initializations";
    private const string PendingRequests = "hostloom.kafka.requests.pending";

    /// <summary>The timeout of every request made while the broker is paused.</summary>
    private static readonly TimeSpan OutageTimeout = TimeSpan.FromSeconds(8);

    /// <summary>
    /// How long a failing request may take beyond its timeout before it counts as hanging.
    /// </summary>
    private static readonly TimeSpan Slack = TimeSpan.FromSeconds(10);

    /// <summary>Bound on each recovery step once the broker answers again.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);

    public static bool Enabled => Environment.GetEnvironmentVariable("HOSTLOOM_KAFKA_CHAOS") == "1";

    /// <summary>
    /// Exercises the delayed-assignment path, not the watermark query: the broker freezes before
    /// the first request, so the reply consumer is built during the outage and never reaches its
    /// assignment callback, where the watermark query runs, until the broker answers again.
    /// Then an outage after initialization: requests still time out, but the probe, which never
    /// contacts the broker, keeps reporting healthy.
    /// </summary>
    [Fact(Skip = Skip, SkipUnless = nameof(Enabled))]
    public async Task A_paused_broker_fails_requests_within_their_timeout_and_the_host_recovers()
    {
        using var deadline = WholeTest();
        var token = deadline.Token;
        await using var kafka = await OwnedKafka.StartAsync(
            nameof(A_paused_broker_fails_requests_within_their_timeout_and_the_host_recovers),
            token
        );
        var suffix = Guid.NewGuid().ToString("N");
        var address = "catalog-" + suffix;
        var responses = "catalog-replies-" + suffix;
        await kafka.CreateTopicsAsync([address, responses], token);
        var clientId = "outage-" + suffix;
        using var client = new MeterCapture(KafkaDiagnostics.MeterName, ClientTag, clientId);
        using var replyLoop = new MeterCapture(
            KafkaDiagnostics.MeterName,
            DestinationTag,
            responses
        );
        using var host = await StartHostAsync(
            hostLoom =>
                hostLoom.UseKafka(options => Configure(options, kafka, clientId, responses)),
            address,
            token
        );
        var requests = host.Services.GetRequiredService<IRequestClient<Greet, Greeting>>();
        var probe = (IBrokerHealthProbe)host.Services.GetRequiredService<IRequestBroker>();
        // Nothing has started the reply consumer yet, so there is no failure to report.
        Assert.True((await probe.CheckHealthAsync(token)).IsHealthy);

        await kafka.PauseAsync(token);
        try
        {
            var timedOut = await FailsWithinTimeoutAsync<RequestTimeoutException>(
                () => requests.GetResponseAsync(address, new Greet("Ada"), OutageTimeout, token),
                token
            );
            Assert.Contains(
                "was not assigned any partition",
                timedOut.InnerException?.Message,
                StringComparison.Ordinal
            );
            var waiting = await probe.CheckHealthAsync(token);
            Assert.False(waiting.IsHealthy);
            Assert.Contains("awaiting assignment", waiting.Description, StringComparison.Ordinal);
            // Initialization is still pending, neither succeeded nor failed, and the request was
            // never published because the reply consumer was not ready.
            Assert.Equal(0, client.Sum(Initializations));
            Assert.Equal(0, client.Sum(Produced, (KindTag, "request")));
        }
        finally
        {
            await kafka.UnpauseAsync();
        }

        // The initialization the timed-out request started is still waiting and completes once
        // the broker answers: no new host and no new request are needed for it.
        using (var recovery = Step(token))
        {
            await client.WaitForAsync(
                Initializations,
                1,
                recovery.Token,
                (OutcomeTag, "succeeded")
            );
        }

        var greeting = await requests.GetResponseAsync(address, new Greet("Ada"), Bound, token);
        Assert.Equal("Hello, Ada!", greeting.Text);
        Assert.True((await probe.CheckHealthAsync(token)).IsHealthy);

        await kafka.PauseAsync(token);
        try
        {
            var stalled = await FailsWithinTimeoutAsync<RequestTimeoutException>(
                () => requests.GetResponseAsync(address, new Greet("Grace"), OutageTimeout, token),
                token
            );
            Assert.Contains(
                "while producing the request or waiting for its reply",
                stalled.InnerException?.Message,
                StringComparison.Ordinal
            );
            // The probe reports the reply consumer's local state and never contacts the broker,
            // so an outage that begins after initialization does not make it unhealthy.
            Assert.True((await probe.CheckHealthAsync(token)).IsHealthy);
        }
        finally
        {
            await kafka.UnpauseAsync();
        }

        var recovered = await requests.GetResponseAsync(address, new Greet("Lin"), Bound, token);
        Assert.Equal("Hello, Lin!", recovered.Text);
        Assert.Equal(1, client.Sum(Initializations, (OutcomeTag, "succeeded")));
        Assert.Equal(0, client.Sum(Initializations, (OutcomeTag, "failed")));
        Assert.Equal(0, client.Observe(PendingRequests));
        // Whether the client library reports anything to the reply loop during a pause this short
        // depends on its internal request timers, so the count is logged rather than asserted.
        Log($"Reply-loop faults across both outages: {replyLoop.Sum(LoopFaults)}.");
    }

    /// <summary>
    /// Exercises the watermark-query path. Pausing from outside cannot land between assignment
    /// and the query, so the reply consumer is built through the transport's consumer-factory
    /// seam, exactly as the transport builds it, and the broker is paused from inside its first
    /// assignment callback. The real watermark query then times out against the frozen broker,
    /// initialization fails, and the next request on the same host builds a fresh consumer.
    /// </summary>
    [Fact(Skip = Skip, SkipUnless = nameof(Enabled))]
    public async Task A_watermark_query_failed_by_an_outage_is_retried_by_the_next_request()
    {
        using var deadline = WholeTest();
        var token = deadline.Token;
        await using var kafka = await OwnedKafka.StartAsync(
            nameof(A_watermark_query_failed_by_an_outage_is_retried_by_the_next_request),
            token
        );
        var suffix = Guid.NewGuid().ToString("N");
        var address = "inventory-" + suffix;
        var responses = "inventory-replies-" + suffix;
        await kafka.CreateTopicsAsync([address, responses], token);
        var clientId = "outage-" + suffix;
        using var client = new MeterCapture(KafkaDiagnostics.MeterName, ClientTag, clientId);
        using var replyLoop = new MeterCapture(
            KafkaDiagnostics.MeterName,
            DestinationTag,
            responses
        );
        var fault = new PauseOnFirstReplyAssignment(kafka);
        var options = new KafkaOptions();
        Configure(options, kafka, clientId, responses);
        using var host = await StartHostAsync(
            hostLoom =>
                hostLoom.Services.AddSingleton<IRequestBroker>(services => new KafkaRequestBroker(
                    Options.Create(options),
                    services.GetService<ILogger<KafkaRequestBroker>>(),
                    producer: null,
                    consumerFactory: fault.Build
                )),
            address,
            token
        );
        var requests = host.Services.GetRequiredService<IRequestClient<Greet, Greeting>>();
        var probe = (IBrokerHealthProbe)host.Services.GetRequiredService<IRequestBroker>();

        // The timeout outlasts assignment, the five-second watermark query, and the release of
        // the failed consumer, so the request reports the initialization failure itself.
        var started = Stopwatch.GetTimestamp();
        var first = requests.GetResponseAsync(address, new Greet("Ada"), Bound, token).AsTask();
        try
        {
            using var outage = Step(token);
            await fault.Paused.WaitAsync(outage.Token);
            await client.WaitForAsync(Initializations, 1, outage.Token, (OutcomeTag, "failed"));
            // Confluent rethrows the assignment callback's exception from Consume, so the reply
            // loop survives it as a consume fault on every run, not by timing.
            await replyLoop.WaitForAsync(LoopFaults, 1, outage.Token, (StageTag, "consume"));

            // Still paused. Releasing the failed consumer waits out its leave-group request, then
            // the caller receives the client library's timeout long before its own deadline: the
            // request fails during the outage instead of hanging, and nothing was published.
            var watermark = await Assert.ThrowsAsync<KafkaException>(() =>
                first.WaitAsync(outage.Token)
            );
            Assert.Equal(ErrorCode.Local_TimedOut, watermark.Error.Code);
            Log($"The first request failed after {Stopwatch.GetElapsedTime(started)}.");
            Assert.Equal(0, client.Sum(Produced, (KindTag, "request")));
            var failed = await probe.CheckHealthAsync(token);
            Assert.False(failed.IsHealthy);
            Assert.Contains("unavailable", failed.Description, StringComparison.Ordinal);
            Assert.Equal(0, client.Sum(Initializations, (OutcomeTag, "succeeded")));
        }
        finally
        {
            await kafka.UnpauseAsync();
        }

        var greeting = await requests.GetResponseAsync(address, new Greet("Ada"), Bound, token);

        Assert.Equal("Hello, Ada!", greeting.Text);
        Assert.Equal(2, fault.ReplyConsumersBuilt);
        Assert.Equal(1, client.Sum(Initializations, (OutcomeTag, "failed")));
        Assert.Equal(1, client.Sum(Initializations, (OutcomeTag, "succeeded")));
        Assert.Equal(1, client.Sum(Produced, (KindTag, "request")));
        Assert.True((await probe.CheckHealthAsync(token)).IsHealthy);
        Assert.Equal(0, client.Observe(PendingRequests));
    }

    private static void Configure(
        KafkaOptions options,
        OwnedKafka kafka,
        string clientId,
        string responses
    )
    {
        options.BootstrapServers = kafka.BootstrapServers;
        options.ConsumerGroup = clientId;
        options.ClientId = clientId;
        options.ResponseTopic = responses;
        // A request abandoned during an outage must not linger for the default five minutes, but
        // its delivery must outlast the request, or a produce error would race the timeout.
        options.ConfigureClient = config =>
        {
            if (config is ProducerConfig producer)
            {
                producer.MessageTimeoutMs = (int)(OutageTimeout * 3).TotalMilliseconds;
            }
        };
    }

    private static async Task<IHost> StartHostAsync(
        Action<HostLoomBuilder> transport,
        string address,
        CancellationToken token
    )
    {
        var builder = Host.CreateApplicationBuilder();
        var hostLoom = builder.Services.AddHostLoom();
        transport(hostLoom);
        hostLoom.AddHandler<Greet, Greeting, GreetHandler>(address);
        var host = builder.Build();
        await host.StartAsync(token);
        return host;
    }

    private static async Task<TException> FailsWithinTimeoutAsync<TException>(
        Func<ValueTask<Greeting>> request,
        CancellationToken token
    )
        where TException : Exception
    {
        var started = Stopwatch.GetTimestamp();
        // A hang fails on this bound, as a TimeoutException, not on the whole-test deadline.
        var failure = await Assert.ThrowsAsync<TException>(() =>
            request().AsTask().WaitAsync(OutageTimeout + Slack, token)
        );
        var elapsed = Stopwatch.GetElapsedTime(started);
        Log($"{typeof(TException).Name} after {elapsed} against a timeout of {OutageTimeout}.");
        Assert.True(
            elapsed >= OutageTimeout - TimeSpan.FromSeconds(1),
            $"Failed early: {elapsed}."
        );
        return failure;
    }

    private static CancellationTokenSource WholeTest()
    {
        var deadline = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        deadline.CancelAfter(TimeSpan.FromMinutes(4));
        return deadline;
    }

    private static CancellationTokenSource Step(CancellationToken token)
    {
        var step = CancellationTokenSource.CreateLinkedTokenSource(token);
        step.CancelAfter(Bound);
        return step;
    }

    private static void Log(string message) =>
        TestContext.Current.TestOutputHelper?.WriteLine(message);

    /// <summary>
    /// Builds consumers the way the transport does, and freezes the broker from inside the first
    /// assignment callback of a reply consumer, the only consumer given one, before handing the
    /// assignment on to the transport's own callback and its watermark query.
    /// </summary>
    private sealed class PauseOnFirstReplyAssignment(OwnedKafka kafka)
    {
        private readonly TaskCompletionSource _paused = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private int _armed = 1;
        private int _replyConsumers;

        public Task Paused => _paused.Task;

        public int ReplyConsumersBuilt => Volatile.Read(ref _replyConsumers);

        public IConsumer<string, byte[]> Build(
            ConsumerConfig config,
            PartitionsAssignedHandler? partitionsAssigned
        )
        {
            var builder = new ConsumerBuilder<string, byte[]>(config);
            if (partitionsAssigned is not null)
            {
                Interlocked.Increment(ref _replyConsumers);
                builder.SetPartitionsAssignedHandler(
                    (consumer, partitions) =>
                    {
                        if (Interlocked.Exchange(ref _armed, 0) == 1)
                        {
                            kafka.Pause();
                            _paused.TrySetResult();
                        }

                        return partitionsAssigned(consumer, partitions);
                    }
                );
            }

            return builder.Build();
        }
    }

    /// <summary>
    /// A single-node KRaft broker in a container this test created, listening and advertising on
    /// one reserved loopback port, so every client connection runs to the container itself.
    /// </summary>
    private sealed class OwnedKafka : IAsyncDisposable
    {
        private const string Image = "apache/kafka:3.9.0";

        private OwnedKafka(string id, int port)
        {
            Id = id;
            BootstrapServers = $"127.0.0.1:{port}";
        }

        public string Id { get; }

        public string BootstrapServers { get; }

        public static async Task<OwnedKafka> StartAsync(string test, CancellationToken token)
        {
            var port = ReservePort();
            var name = "kafka-outage-" + Guid.NewGuid().ToString("N");
            // The compose environment with 9092 replaced by the reserved port; the controller
            // stays on an internal port that is never published.
            var id = await DockerAsync(
                token,
                "run",
                "--detach",
                "--rm",
                "--name",
                name,
                "--label",
                $"hostloom.outage-test={nameof(KafkaOutageTests)}.{test}",
                // The image declares these three volumes; tmpfs keeps each from becoming an
                // anonymous volume.
                "--tmpfs",
                "/var/lib/kafka/data",
                "--tmpfs",
                "/etc/kafka/secrets",
                "--tmpfs",
                "/mnt/shared/config",
                "--publish",
                $"127.0.0.1:{port}:{port}",
                "--env",
                "KAFKA_NODE_ID=1",
                "--env",
                "KAFKA_PROCESS_ROLES=broker,controller",
                "--env",
                $"KAFKA_LISTENERS=PLAINTEXT://:{port},CONTROLLER://:9093",
                "--env",
                $"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{port}",
                "--env",
                "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                "--env",
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                "--env",
                "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
                "--env",
                "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
                "--env",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
                "--env",
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
                "--env",
                "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
                "--env",
                "KAFKA_AUTO_CREATE_TOPICS_ENABLE=true",
                "--env",
                "KAFKA_NUM_PARTITIONS=1",
                "--env",
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
                Image
            );
            var kafka = new OwnedKafka(id, port);
            try
            {
                await kafka.WaitUntilServingAsync(token);
                await kafka.AssertNoVolumesAsync(token);
                return kafka;
            }
            catch
            {
                await kafka.DisposeAsync();
                throw;
            }
        }

        public async Task CreateTopicsAsync(IEnumerable<string> topics, CancellationToken token)
        {
            using var admin = Admin();
            await admin
                .CreateTopicsAsync(
                    topics.Select(topic => new TopicSpecification
                    {
                        Name = topic,
                        NumPartitions = 1,
                        ReplicationFactor = 1,
                    }),
                    new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }
                )
                .WaitAsync(TimeSpan.FromSeconds(15), token);
        }

        public async Task PauseAsync(CancellationToken token) =>
            await DockerAsync(token, "pause", Id);

        /// <summary>Pauses from a client-library callback thread, which must not await.</summary>
        public void Pause() => Docker(TimeSpan.FromSeconds(30), "pause", Id);

        /// <summary>Resumes on its own bound, so a finally block restores the broker even after
        /// the test's deadline has passed.</summary>
        public async Task UnpauseAsync()
        {
            using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await DockerAsync(bound.Token, "unpause", Id);
        }

        /// <summary>Only tmpfs, which inspect does not list as a mount, and no volume at all.</summary>
        private async Task AssertNoVolumesAsync(CancellationToken token) =>
            Assert.Equal(
                "0",
                await DockerAsync(token, "inspect", "--format", "{{len .Mounts}}", Id)
            );

        public async ValueTask DisposeAsync()
        {
            // A forced removal bypasses --rm's own cleanup, so anonymous volumes go explicitly.
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await DockerAsync(cleanup.Token, "rm", "--force", "--volumes", Id);
        }

        private IAdminClient Admin() =>
            new AdminClientBuilder(
                new AdminClientConfig { BootstrapServers = BootstrapServers }
            ).Build();

        /// <summary>
        /// Metadata with a live broker is the readiness signal; each attempt blocks up to its own
        /// timeout while the client library reconnects, so nothing here sleeps.
        /// </summary>
        private async Task WaitUntilServingAsync(CancellationToken token)
        {
            using var admin = Admin();
            using var ready = CancellationTokenSource.CreateLinkedTokenSource(token);
            ready.CancelAfter(TimeSpan.FromSeconds(90));
            while (true)
            {
                ready.Token.ThrowIfCancellationRequested();
                try
                {
                    var metadata = await Task.Run(
                            () => admin.GetMetadata(TimeSpan.FromSeconds(2)),
                            ready.Token
                        )
                        .WaitAsync(ready.Token);
                    if (metadata.Brokers.Count > 0)
                    {
                        return;
                    }
                }
                catch (KafkaException) { }
            }
        }

        private static int ReservePort()
        {
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
    }

    private static ProcessStartInfo DockerCommand(string[] arguments)
    {
        var start = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var argument in arguments)
        {
            start.ArgumentList.Add(argument);
        }

        return start;
    }

    private static async Task<string> DockerAsync(
        CancellationToken token,
        params string[] arguments
    )
    {
        using var process =
            Process.Start(DockerCommand(arguments))
            ?? throw new InvalidOperationException("Cannot start Docker.");
        var output = process.StandardOutput.ReadToEndAsync(token);
        var error = process.StandardError.ReadToEndAsync(token);
        try
        {
            await process.WaitForExitAsync(token);
        }
        catch (OperationCanceledException)
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }

            throw;
        }

        var diagnostic = await error;
        Assert.True(process.ExitCode == 0, diagnostic);
        return (await output).Trim();
    }

    private static void Docker(TimeSpan bound, params string[] arguments)
    {
        using var process =
            Process.Start(DockerCommand(arguments))
            ?? throw new InvalidOperationException("Cannot start Docker.");
        if (!process.WaitForExit(bound))
        {
            process.Kill(entireProcessTree: true);
            throw new TimeoutException($"docker {string.Join(' ', arguments)} did not finish.");
        }

        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException(process.StandardError.ReadToEnd());
        }
    }
}
