using System.Collections.Concurrent;
using HostLoom.Pipelines;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;
using static HostLoom.Tests.PublishSubscribeTests;

namespace HostLoom.Tests;

public sealed class InboxTests
{
    [Fact]
    public async Task The_in_memory_store_remembers_a_key_for_the_window()
    {
        var clock = new TestClock();
        var store = new InMemoryInboxStore(clock);

        Assert.True(
            await store.TryRecordAsync(
                "k",
                TimeSpan.FromHours(1),
                TestContext.Current.CancellationToken
            )
        );
        Assert.False(
            await store.TryRecordAsync(
                "k",
                TimeSpan.FromHours(1),
                TestContext.Current.CancellationToken
            )
        );
        Assert.Equal(1, store.Count);
        clock.Advance(TimeSpan.FromHours(1));
        Assert.True(
            await store.TryRecordAsync(
                "k",
                TimeSpan.FromHours(1),
                TestContext.Current.CancellationToken
            )
        );
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await store.TryRecordAsync("k", TimeSpan.Zero, TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task A_redelivered_event_runs_its_handlers_once_per_subscription()
    {
        var received = new Received();
        var observed = new List<string>();
        using var host = await StartAsync(
            received,
            hostLoom =>
                hostLoom
                    .ConfigureReceivePipeline(pipe =>
                        pipe.Use(
                            async (context, next) =>
                            {
                                await next.SendAsync(context);
                                observed.Add(
                                    context.TryGetPayload<InboxDuplicate>(out var duplicate)
                                        ? $"duplicate {duplicate!.Key}"
                                        : "handled"
                                );
                            },
                            "observer"
                        )
                    )
                    .UseInMemoryInbox(TimeSpan.FromHours(1))
                    .AddSubscriber<OrderPlaced, AuditHandler>("orders", "audit")
                    .AddSubscriber<OrderPlaced, ShippingHandler>("orders", "shipping")
        );
        var broker = Broker(host);

        await host
            .Services.GetRequiredService<IPublishEndpoint>()
            .PublishAsync("orders", new OrderPlaced("A-1"), TestContext.Current.CancellationToken);
        await broker.RedeliverAsync(0, TestContext.Current.CancellationToken);

        Assert.Equal(["audit:A-1", "shipping:A-1"], received.Sorted());
        Assert.Equal(2, observed.Count(o => o == "handled"));
        Assert.Equal(
            2,
            observed.Count(o => o.StartsWith("duplicate 6:orders:", StringComparison.Ordinal))
        );
        Assert.Contains(
            observed,
            o => o.StartsWith("duplicate 6:orders:5:audit:", StringComparison.Ordinal)
        );
        Assert.Contains(
            observed,
            o => o.StartsWith("duplicate 6:orders:8:shipping:", StringComparison.Ordinal)
        );
        Assert.Equal(2, host.Services.GetRequiredService<InMemoryInboxStore>().Count);
    }

    [Fact]
    public async Task An_unavailable_store_lets_the_handlers_run_and_says_so()
    {
        var received = new Received();
        var skipped = new List<InboxSkipped>();
        using var host = await StartAsync(
            received,
            hostLoom =>
                hostLoom
                    .ConfigureReceivePipeline(pipe =>
                        pipe.Use(
                            async (context, next) =>
                            {
                                await next.SendAsync(context);
                                if (context.TryGetPayload<InboxSkipped>(out var payload))
                                {
                                    skipped.Add(payload!);
                                }
                            },
                            "observer"
                        )
                    )
                    .UseInbox(
                        static _ =>
                            InboxStore.FromClaim(
                                static (_, _, _) =>
                                    throw new InvalidOperationException("store down")
                            ),
                        TimeSpan.FromHours(1)
                    )
                    .AddSubscriber<OrderPlaced, AuditHandler>("orders", "audit")
        );

        await host
            .Services.GetRequiredService<IPublishEndpoint>()
            .PublishAsync("orders", new OrderPlaced("A-2"), TestContext.Current.CancellationToken);
        await Broker(host).RedeliverAsync(0, TestContext.Current.CancellationToken);

        // At-least-once is the safe side of an outage: both deliveries ran, both were flagged.
        Assert.Equal(["audit:A-2", "audit:A-2"], received.Sorted());
        Assert.Equal(2, skipped.Count);
        Assert.All(skipped, s => Assert.Equal("store down", s.Failure.Message));
    }

    [Fact]
    public async Task The_in_memory_store_forgets_a_released_key()
    {
        var store = new InMemoryInboxStore(new TestClock());

        Assert.True(
            await store.TryRecordAsync(
                "k",
                TimeSpan.FromHours(1),
                TestContext.Current.CancellationToken
            )
        );
        await store.ReleaseAsync("k", TestContext.Current.CancellationToken);
        await store.ReleaseAsync("absent", TestContext.Current.CancellationToken);

        Assert.Equal(0, store.Count);
        Assert.True(
            await store.TryRecordAsync(
                "k",
                TimeSpan.FromHours(1),
                TestContext.Current.CancellationToken
            )
        );
    }

    [Fact]
    public async Task A_failed_run_releases_its_key_so_the_redelivery_runs_the_handlers_again()
    {
        var received = new Received();
        using var host = await StartAsync(
            received,
            hostLoom =>
            {
                hostLoom.Services.AddSingleton<FlakyHandler.Attempts>();
                hostLoom
                    .UseInMemoryInbox(TimeSpan.FromHours(1))
                    .AddSubscriber<OrderPlaced, FlakyHandler>("orders", "flaky");
            }
        );
        var broker = Broker(host);

        // The handler fails twice and then succeeds, as a broker's redeliveries would find it.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await PublishAsync(host, new OrderPlaced("A-3"))
        );
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await broker.RedeliverAsync(0, TestContext.Current.CancellationToken)
        );
        await broker.RedeliverAsync(0, TestContext.Current.CancellationToken);
        await broker.RedeliverAsync(0, TestContext.Current.CancellationToken);

        // Only the redelivery after the run that completed is a duplicate.
        Assert.Equal(["flaky:1", "flaky:2", "flaky:3"], received.Sorted());
        Assert.Equal(1, host.Services.GetRequiredService<InMemoryInboxStore>().Count);
    }

    [Fact]
    public async Task A_cancelled_run_releases_its_key_with_a_token_of_its_own()
    {
        var received = new Received();
        var gate = new StallingHandler.Gate();
        var inner = new InMemoryInboxStore();
        var released = new List<(string Key, bool Cancelled)>();
        using var host = await StartAsync(
            received,
            hostLoom =>
            {
                hostLoom.Services.AddSingleton(gate);
                hostLoom
                    .UseInbox(
                        _ =>
                            InboxStore.FromClaim(
                                inner.TryRecordAsync,
                                (key, token) =>
                                {
                                    released.Add((key, token.IsCancellationRequested));
                                    return inner.ReleaseAsync(key, token);
                                }
                            ),
                        TimeSpan.FromHours(1)
                    )
                    .AddSubscriber<OrderPlaced, StallingHandler>("orders", "audit");
            }
        );
        using var delivery = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );

        // A consumer shutting down mid-handler, as a rolling deploy does: the broker requeues.
        var publish = host
            .Services.GetRequiredService<IPublishEndpoint>()
            .PublishAsync("orders", new OrderPlaced("A-4"), delivery.Token)
            .AsTask();
        await gate.Started.Task.WaitAsync(TestContext.Current.CancellationToken);
        await delivery.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => publish);

        // The delivery's token was cancelled; the release had a live one, so the key went.
        var release = Assert.Single(released);
        Assert.StartsWith("6:orders:5:audit:", release.Key, StringComparison.Ordinal);
        Assert.False(release.Cancelled);
        Assert.Equal(0, inner.Count);

        await Broker(host).RedeliverAsync(0, TestContext.Current.CancellationToken);

        Assert.Equal(["audit:A-4"], received.Sorted());
        Assert.Equal(1, inner.Count);
    }

    [Fact]
    public async Task A_retry_registered_before_the_inbox_runs_every_attempt()
    {
        var received = new Received();
        using var host = await StartAsync(
            received,
            hostLoom =>
            {
                hostLoom.Services.AddSingleton<FlakyHandler.Attempts>();
                hostLoom
                    .ConfigureReceivePipeline(pipe => pipe.UseRetry(RetryPolicy.Immediate(3)))
                    .UseInMemoryInbox(TimeSpan.FromHours(1))
                    .AddSubscriber<OrderPlaced, FlakyHandler>("orders", "flaky");
            }
        );

        await PublishAsync(host, new OrderPlaced("A-5"));
        await Broker(host).RedeliverAsync(0, TestContext.Current.CancellationToken);

        // The retry wraps the inbox, so every attempt records the key; each failed attempt
        // released it, so the next one ran rather than being taken for a duplicate.
        Assert.Equal(["flaky:1", "flaky:2", "flaky:3"], received.Sorted());
        Assert.Equal(1, host.Services.GetRequiredService<InMemoryInboxStore>().Count);
    }

    [Fact]
    public async Task A_release_that_fails_is_logged_and_the_handlers_exception_surfaces()
    {
        var logger = new RecordingLogger<InboxFilter>();
        var inner = new InMemoryInboxStore();
        var duplicates = 0;
        using var host = await StartAsync(
            new Received(),
            hostLoom =>
            {
                hostLoom.Services.AddSingleton<ILogger<InboxFilter>>(logger);
                hostLoom
                    .ConfigureReceivePipeline(pipe =>
                        pipe.Use(
                            async (context, next) =>
                            {
                                await next.SendAsync(context);
                                if (context.TryGetPayload<InboxDuplicate>(out _))
                                {
                                    duplicates++;
                                }
                            },
                            "observer"
                        )
                    )
                    .UseInbox(
                        _ =>
                            InboxStore.FromClaim(
                                inner.TryRecordAsync,
                                static (_, _) => throw new InvalidOperationException("release down")
                            ),
                        TimeSpan.FromHours(1)
                    )
                    .AddSubscriber<OrderPlaced, ExplodingHandler>("orders", "broken");
            }
        );

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await PublishAsync(host, new OrderPlaced("A-6"))
        );

        Assert.Equal("subscriber is broken", exception.Message);
        var warning = Assert.Single(
            logger.Entries,
            entry => entry.Event.Id == InboxEvents.ReleaseFailed.Id
        );
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Equal("release down", warning.Exception?.Message);

        // The key stayed, which is the documented cost: the redelivery is taken for a duplicate.
        await Broker(host).RedeliverAsync(0, TestContext.Current.CancellationToken);
        Assert.Equal(1, duplicates);
    }

    [Theory]
    [InlineData(nameof(RecordOnlyStore))]
    [InlineData(nameof(InboxStore.FromClaim))]
    public async Task A_store_without_a_release_keeps_a_failed_runs_key(string store)
    {
        var received = new Received();
        var keys = new ConcurrentDictionary<string, byte>(StringComparer.Ordinal);
        IInboxStore inbox =
            store == nameof(RecordOnlyStore)
                ? new RecordOnlyStore(keys)
                : InboxStore.FromClaim((key, _, _) => ValueTask.FromResult(keys.TryAdd(key, 0)));
        using var host = await StartAsync(
            received,
            hostLoom =>
            {
                hostLoom.Services.AddSingleton<FlakyHandler.Attempts>();
                hostLoom
                    .UseInbox(_ => inbox, TimeSpan.FromHours(1))
                    .AddSubscriber<OrderPlaced, FlakyHandler>("orders", "flaky");
            }
        );

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await PublishAsync(host, new OrderPlaced("A-7"))
        );
        await Broker(host).RedeliverAsync(0, TestContext.Current.CancellationToken);

        // The default release does nothing, so such a store behaves as the one-member contract
        // did: the failed run's key stays, and its redelivery is dropped as a duplicate.
        Assert.Equal(["flaky:1"], received.Sorted());
        Assert.Single(keys);
    }

    [Fact]
    public async Task A_claim_store_with_a_release_delegate_releases_a_failed_runs_key()
    {
        var received = new Received();
        var inner = new InMemoryInboxStore();
        var released = new List<string>();
        using var host = await StartAsync(
            received,
            hostLoom =>
            {
                hostLoom.Services.AddSingleton<FlakyHandler.Attempts>();
                hostLoom
                    .UseInbox(
                        _ =>
                            InboxStore.FromClaim(
                                inner.TryRecordAsync,
                                (key, token) =>
                                {
                                    released.Add(key);
                                    return inner.ReleaseAsync(key, token);
                                }
                            ),
                        TimeSpan.FromHours(1)
                    )
                    .AddSubscriber<OrderPlaced, FlakyHandler>("orders", "flaky");
            }
        );
        var broker = Broker(host);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await PublishAsync(host, new OrderPlaced("A-8"))
        );
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await broker.RedeliverAsync(0, TestContext.Current.CancellationToken)
        );
        await broker.RedeliverAsync(0, TestContext.Current.CancellationToken);

        Assert.Equal(["flaky:1", "flaky:2", "flaky:3"], received.Sorted());
        Assert.Equal(2, released.Count);
        Assert.All(
            released,
            key => Assert.StartsWith("6:orders:5:flaky:", key, StringComparison.Ordinal)
        );
        Assert.Equal(1, inner.Count);
        Assert.Throws<ArgumentNullException>(() =>
            InboxStore.FromClaim(inner.TryRecordAsync, null!)
        );
    }

    [Fact]
    public async Task Requests_pass_through_the_inbox_untouched()
    {
        using var host = await StartAsync(
            new Received(),
            hostLoom =>
                hostLoom
                    .UseInMemoryInbox(TimeSpan.FromHours(1))
                    .AddHandler<Echo, Echoed, EchoHandler>("echo")
        );
        var client = host.Services.GetRequiredService<IRequestClient<Echo, Echoed>>();

        var first = await client.GetResponseAsync(
            "echo",
            new Echo("a"),
            cancellationToken: TestContext.Current.CancellationToken
        );
        var replayed = await Broker(host)
            .ReplayLastRequestAsync(TestContext.Current.CancellationToken);

        Assert.Equal("a", first.Value);
        Assert.False(replayed.IsEmpty);
        Assert.Equal(0, host.Services.GetRequiredService<InMemoryInboxStore>().Count);
    }

    [Fact]
    public async Task The_inbox_appears_in_the_receive_pipeline_probe_and_validates_its_window()
    {
        using var host = await StartAsync(
            new Received(),
            hostLoom =>
                hostLoom
                    .UseInMemoryInbox(TimeSpan.FromMinutes(30))
                    .AddSubscriber<OrderPlaced, AuditHandler>("orders", "audit")
        );

        var probe = host
            .Services.GetRequiredService<HostLoomProbe>()
            .ReceivePipeline(TestContext.Current.CancellationToken);

        var inbox = Assert.Single(Flatten(probe), result => result.Name == "inbox");
        Assert.Equal(TimeSpan.FromMinutes(30), inbox.Properties["window"]);
        Assert.Equal(nameof(InMemoryInboxStore), inbox.Properties["store"]);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new InboxFilter(new InMemoryInboxStore(), TimeSpan.Zero)
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ServiceCollection().AddHostLoom().UseInMemoryInbox(TimeSpan.FromSeconds(-1))
        );
    }

    [Theory]
    [InlineData("in-memory", "in-memory")]
    [InlineData("in-memory", "typed")]
    [InlineData("typed", "delegate")]
    [InlineData("delegate", "in-memory")]
    public void Enabling_the_inbox_twice_is_refused(string first, string second)
    {
        var hostLoom = new ServiceCollection().AddHostLoom();
        Enable(hostLoom, first);

        // A second filter over the same store would take every delivery the first one recorded
        // for a duplicate, so no handler would ever run. Another builder over the same services
        // composes the same receive pipeline and is refused too.
        var exception = Assert.Throws<InvalidOperationException>(() =>
            Enable(hostLoom.Services.AddHostLoom(), second)
        );

        Assert.Contains("UseInbox", exception.Message, StringComparison.Ordinal);
        using var composed = hostLoom.Services.BuildServiceProvider();
        var probe = composed
            .GetRequiredService<HostLoomProbe>()
            .ReceivePipeline(TestContext.Current.CancellationToken);
        Assert.Single(Flatten(probe), result => result.Name == "inbox");

        static void Enable(HostLoomBuilder builder, string kind) =>
            _ = kind switch
            {
                "in-memory" => builder.UseInMemoryInbox(TimeSpan.FromHours(1)),
                "typed" => builder.UseInbox<InMemoryInboxStore>(TimeSpan.FromHours(1)),
                _ => builder.UseInbox(_ => new InMemoryInboxStore(), TimeSpan.FromHours(1)),
            };
    }

    private static IEnumerable<ProbeResult> Flatten(ProbeResult result)
    {
        yield return result;
        foreach (var child in result.Children)
        {
            foreach (var nested in Flatten(child))
            {
                yield return nested;
            }
        }
    }

    private static async Task<IHost> StartAsync(
        Received received,
        Action<HostLoomBuilder> configure
    )
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received);
        builder.Services.AddSingleton<RedeliveringBroker>();
        configure(builder.Services.AddHostLoom().UseTransport<RedeliveringBroker>());
        var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        return host;
    }

    private static RedeliveringBroker Broker(IHost host) =>
        (RedeliveringBroker)host.Services.GetRequiredService<IRequestBroker>();

    private static ValueTask PublishAsync(IHost host, OrderPlaced @event) =>
        host
            .Services.GetRequiredService<IPublishEndpoint>()
            .PublishAsync("orders", @event, TestContext.Current.CancellationToken);

    /// <summary>Holds its first delivery until that delivery is cancelled; later ones complete.</summary>
    public sealed class StallingHandler(Received received, StallingHandler.Gate gate)
        : IEventHandler<OrderPlaced>
    {
        public async ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken)
        {
            if (gate.Started.TrySetResult())
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }

            received.Add($"audit:{@event.Reference}");
        }

        public sealed class Gate
        {
            public TaskCompletionSource Started { get; } =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }

    /// <summary>A store written against the one-member contract: it records and never forgets.</summary>
    private sealed class RecordOnlyStore(ConcurrentDictionary<string, byte> keys) : IInboxStore
    {
        public ValueTask<bool> TryRecordAsync(
            string key,
            TimeSpan window,
            CancellationToken cancellationToken = default
        ) => ValueTask.FromResult(keys.TryAdd(key, 0));
    }

    public sealed record Echo(string Value) : IRequest<Echoed>;

    public sealed record Echoed(string Value);

    public sealed class EchoHandler : IRequestHandler<Echo, Echoed>
    {
        public ValueTask<Echoed> HandleAsync(Echo request, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new Echoed(request.Value));
    }

    /// <summary>An in-process transport that keeps every frame so a test can deliver it again.</summary>
    public sealed class RedeliveringBroker : IRequestBroker, IEventBroker
    {
        private readonly Dictionary<RequestAddress, RequestFrameHandler> _handlers = [];
        private readonly Dictionary<RequestAddress, List<EventFrameHandler>> _topics = [];
        private readonly List<(RequestAddress Topic, ReadOnlyMemory<byte> Frame)> _events = [];
        private (RequestAddress Address, ReadOnlyMemory<byte> Frame)? _lastRequest;

        public ValueTask<IAsyncDisposable> ListenAsync(
            RequestAddress address,
            RequestFrameHandler handler,
            CancellationToken cancellationToken
        )
        {
            _handlers[address] = handler;
            return ValueTask.FromResult<IAsyncDisposable>(Nothing.Instance);
        }

        public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(
            RequestAddress address,
            ReadOnlyMemory<byte> request,
            Guid requestId,
            TimeSpan timeout,
            CancellationToken cancellationToken
        )
        {
            _lastRequest = (address, request);
            return await _handlers[address](request, cancellationToken);
        }

        public ValueTask<IAsyncDisposable> SubscribeAsync(
            RequestAddress topic,
            string subscription,
            EventFrameHandler handler,
            CancellationToken cancellationToken
        )
        {
            if (!_topics.TryGetValue(topic, out var handlers))
            {
                handlers = [];
                _topics[topic] = handlers;
            }

            handlers.Add(handler);
            return ValueTask.FromResult<IAsyncDisposable>(Nothing.Instance);
        }

        public async ValueTask PublishAsync(
            RequestAddress topic,
            ReadOnlyMemory<byte> frame,
            CancellationToken cancellationToken
        )
        {
            _events.Add((topic, frame));
            await DeliverAsync(topic, frame, cancellationToken);
        }

        public ValueTask RedeliverAsync(int index, CancellationToken cancellationToken)
        {
            var (topic, frame) = _events[index];
            return DeliverAsync(topic, frame, cancellationToken);
        }

        public ValueTask<ReadOnlyMemory<byte>> ReplayLastRequestAsync(
            CancellationToken cancellationToken
        )
        {
            var (address, frame) = _lastRequest!.Value;
            return _handlers[address](frame, cancellationToken);
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private async ValueTask DeliverAsync(
            RequestAddress topic,
            ReadOnlyMemory<byte> frame,
            CancellationToken cancellationToken
        )
        {
            foreach (var handler in _topics.GetValueOrDefault(topic) ?? [])
            {
                await handler(frame, cancellationToken);
            }
        }

        private sealed class Nothing : IAsyncDisposable
        {
            public static readonly Nothing Instance = new();

            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }
}
