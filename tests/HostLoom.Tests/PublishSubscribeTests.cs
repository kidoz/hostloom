using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.Tests;

public sealed class PublishSubscribeTests
{
    [Fact]
    public async Task One_published_event_reaches_every_subscription_on_the_topic()
    {
        var received = new Received();
        using var host = await StartAsync(received, hostLoom => hostLoom
            .AddSubscriber<OrderPlaced, AuditHandler>("orders", "audit")
            .AddSubscriber<OrderPlaced, ShippingHandler>("orders", "shipping"));

        await PublisherOf(host).PublishAsync(
            "orders",
            new OrderPlaced("A-1"),
            TestContext.Current.CancellationToken);

        Assert.Equal(["audit:A-1", "shipping:A-1"], received.Sorted());
    }

    [Fact]
    public async Task Handlers_sharing_a_subscription_all_run_for_one_delivery()
    {
        var received = new Received();
        using var host = await StartAsync(received, hostLoom => hostLoom
            .AddSubscriber<OrderPlaced, AuditHandler>("orders", "combined")
            .AddSubscriber<OrderPlaced, ShippingHandler>("orders", "combined"));

        await PublisherOf(host).PublishAsync(
            "orders",
            new OrderPlaced("A-2"),
            TestContext.Current.CancellationToken);

        Assert.Equal(["audit:A-2", "shipping:A-2"], received.Sorted());
    }

    [Fact]
    public async Task A_subscription_ignores_event_types_it_did_not_register()
    {
        var received = new Received();
        using var host = await StartAsync(received, hostLoom => hostLoom
            .AddSubscriber<OrderPlaced, AuditHandler>("orders", "audit"));

        // Published to the same topic, but no subscription handles this contract.
        await PublisherOf(host).PublishAsync(
            "orders",
            new OrderCancelled("A-3"),
            TestContext.Current.CancellationToken);

        Assert.Empty(received.Sorted());
    }

    [Fact]
    public async Task Subscriptions_on_another_topic_are_not_delivered_to()
    {
        var received = new Received();
        using var host = await StartAsync(received, hostLoom => hostLoom
            .AddSubscriber<OrderPlaced, AuditHandler>("orders", "audit")
            .AddSubscriber<OrderPlaced, ShippingHandler>("invoices", "shipping"));

        await PublisherOf(host).PublishAsync(
            "orders",
            new OrderPlaced("A-4"),
            TestContext.Current.CancellationToken);

        Assert.Equal(["audit:A-4"], received.Sorted());
    }

    [Fact]
    public async Task A_failing_subscription_does_not_stop_the_others_receiving()
    {
        var received = new Received();
        using var host = await StartAsync(received, hostLoom => hostLoom
            .AddSubscriber<OrderPlaced, ExplodingHandler>("orders", "broken")
            .AddSubscriber<OrderPlaced, AuditHandler>("orders", "audit"));

        var failure = await Assert.ThrowsAsync<AggregateException>(async () =>
            await PublisherOf(host).PublishAsync(
                "orders",
                new OrderPlaced("A-5"),
                TestContext.Current.CancellationToken));

        // The healthy subscription still saw the event; the broken one is reported, not hidden.
        Assert.Equal(["audit:A-5"], received.Sorted());
        Assert.Single(failure.InnerExceptions);
    }

    [Fact]
    public async Task Publishing_through_a_request_only_transport_fails_loudly()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddHostLoom().UseTransport<RequestOnlyBroker>();

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);

        var exception = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await PublisherOf(host).PublishAsync(
                "orders",
                new OrderPlaced("A-6"),
                TestContext.Current.CancellationToken));

        Assert.Contains(nameof(IEventBroker), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Registering_a_subscription_on_a_request_only_transport_fails_at_startup()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(new Received());
        builder.Services
            .AddHostLoom()
            .UseTransport<RequestOnlyBroker>()
            .AddSubscriber<OrderPlaced, AuditHandler>("orders", "audit");

        using var host = builder.Build();

        // Better to fail the host than to start looking subscribed while nothing is delivered.
        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await host.StartAsync(TestContext.Current.CancellationToken));
    }

    private static async Task<IHost> StartAsync(Received received, Action<HostLoomBuilder> configure)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received);
        configure(builder.Services.AddHostLoom().UseInMemory());

        var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        return host;
    }

    private static IPublishEndpoint PublisherOf(IHost host) =>
        host.Services.GetRequiredService<IPublishEndpoint>();

    public sealed record OrderPlaced(string Reference) : IEvent;

    public sealed record OrderCancelled(string Reference) : IEvent;

    public sealed class Received
    {
        private readonly List<string> _entries = [];
        private readonly Lock _gate = new();

        public void Add(string entry)
        {
            lock (_gate)
            {
                _entries.Add(entry);
            }
        }

        public List<string> Sorted()
        {
            lock (_gate)
            {
                return _entries.Order(StringComparer.Ordinal).ToList();
            }
        }
    }

    public sealed class AuditHandler(Received received) : IEventHandler<OrderPlaced>
    {
        public ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken)
        {
            received.Add($"audit:{@event.Reference}");
            return ValueTask.CompletedTask;
        }
    }

    public sealed class ShippingHandler(Received received) : IEventHandler<OrderPlaced>
    {
        public ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken)
        {
            received.Add($"shipping:{@event.Reference}");
            return ValueTask.CompletedTask;
        }
    }

    public sealed class ExplodingHandler : IEventHandler<OrderPlaced>
    {
        public ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("subscriber is broken");
    }

    /// <summary>A transport with no publish/subscribe support, to prove the capability check.</summary>
    public sealed class RequestOnlyBroker : IRequestBroker
    {
        public ValueTask<IAsyncDisposable> ListenAsync(
            RequestAddress address,
            RequestFrameHandler handler,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException("not used in these tests");

        public ValueTask<ReadOnlyMemory<byte>> RequestAsync(
            RequestAddress address,
            ReadOnlyMemory<byte> request,
            Guid requestId,
            TimeSpan timeout,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException("not used in these tests");

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
