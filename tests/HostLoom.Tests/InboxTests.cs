using HostLoom.Pipelines;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
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
