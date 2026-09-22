using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace HostLoom.Conformance;

/// <summary>
/// What legitimately differs between transports. Each flag selects which of two behaviours a
/// scenario asserts, so a transport that changes must change its profile as well; no flag turns
/// an assertion off.
/// </summary>
public sealed record TransportProfile
{
    /// <summary>The transport's name, for failure messages.</summary>
    public required string Name { get; init; }

    /// <summary>The broker implements <see cref="IEventBroker"/>.</summary>
    public required bool PublishSubscribe { get; init; }

    /// <summary>The broker implements <see cref="IBrokerHealthProbe"/>.</summary>
    public required bool HealthProbe { get; init; }

    /// <summary>
    /// Broker instances reach one another, so a listener on one answers requests sent through
    /// another. False for a process-local transport, where every instance is its own world.
    /// </summary>
    public required bool SharedAcrossInstances { get; init; }

    /// <summary>Events published one after another reach a subscription in publish order.</summary>
    public required bool OrderedWithinSubscription { get; init; }

    /// <summary>
    /// A second listener on an address is accepted and shares the address with the first, so it
    /// keeps answering once the first leaves; otherwise the second listen is refused.
    /// </summary>
    public required bool CompetingListeners { get; init; }
}

/// <summary>
/// The clock that bounds requests. The in-memory transport runs on a manual clock, so a scenario
/// that waits for a timeout drives it there; a real broker's timeouts run on the system clock and
/// the same scenario simply waits.
/// </summary>
public abstract class TransportClock
{
    /// <summary>The provider elapsed time is measured on.</summary>
    public abstract TimeProvider Provider { get; }

    /// <summary>How much earlier than its due time, measured on <see cref="Provider"/>, a timer may fire.</summary>
    public abstract TimeSpan Resolution { get; }

    /// <summary>
    /// Lets <paramref name="delta"/> pass for an operation that arms a timer on this clock. A
    /// manual clock waits for the timer to be armed, then advances; the system clock returns at
    /// once, because its time passes on its own.
    /// </summary>
    public abstract Task AdvanceAsync(TimeSpan delta, CancellationToken cancellationToken);
}

/// <summary>The wall clock: nothing to drive, time passes by itself.</summary>
public sealed class SystemTransportClock : TransportClock
{
    private SystemTransportClock() { }

    public static SystemTransportClock Instance { get; } = new();

    /// <inheritdoc />
    public override TimeProvider Provider => TimeProvider.System;

    /// <inheritdoc />
    public override TimeSpan Resolution => TimeSpan.FromMilliseconds(50);

    /// <inheritdoc />
    public override Task AdvanceAsync(TimeSpan delta, CancellationToken cancellationToken) =>
        Task.CompletedTask;
}

/// <summary>
/// What a transport scenario needs: brokers for one run, names no other run uses, the transport's
/// host wiring, its capabilities, and the budgets its requests and deliveries are held to. Every
/// broker and host created through it is released when the fixture is disposed, including after a
/// scenario that failed part way.
/// </summary>
public sealed class TransportConformanceFixture : IAsyncDisposable
{
    private readonly Lock _gate = new();
    private readonly List<IRequestBroker> _brokers = [];
    private readonly List<IHost> _hosts = [];

    /// <summary>The capabilities the scenarios select their assertions by.</summary>
    public required TransportProfile Profile { get; init; }

    /// <summary>Identifies this run; the runner derives every name, group, and client id from it.</summary>
    public required string RunId { get; init; }

    /// <summary>Creates a broker, like one more service instance of this run.</summary>
    public required Func<IRequestBroker> BrokerFactory { get; init; }

    /// <summary>
    /// Mints a request address no other run uses from a prefix, provisions what the transport
    /// needs behind it, and records it for cleanup.
    /// </summary>
    public required Func<
        string,
        CancellationToken,
        ValueTask<RequestAddress>
    > AddressFactory { get; init; }

    /// <summary>
    /// Mints a topic no other run uses from a prefix and the subscription names a scenario will
    /// attach, provisions it, and records the topic and its subscriptions for cleanup.
    /// </summary>
    public required Func<
        string,
        IReadOnlyList<string>,
        CancellationToken,
        ValueTask<RequestAddress>
    > TopicFactory { get; init; }

    /// <summary>Registers this transport on a host, the way an application would.</summary>
    public required Action<HostLoomBuilder> UseTransport { get; init; }

    /// <summary>The budget for a request that is expected to be answered.</summary>
    public required TimeSpan RequestTimeout { get; init; }

    /// <summary>The budget for a request that is expected to time out.</summary>
    public required TimeSpan TimeoutBudget { get; init; }

    /// <summary>How late past its budget, on <see cref="Clock"/>, a timeout may surface.</summary>
    public required TimeSpan Slack { get; init; }

    /// <summary>
    /// The wall-clock bound on anything a scenario waits for: a reply, a delivery, a handler
    /// being entered. Longer than <see cref="RequestTimeout"/>, so a request's own timeout is
    /// what a slow transport reports.
    /// </summary>
    public required TimeSpan Bound { get; init; }

    /// <summary>The clock request timeouts run on.</summary>
    public required TransportClock Clock { get; init; }

    /// <summary>The test's cancellation token.</summary>
    public CancellationToken CancellationToken { get; init; }

    /// <summary>Creates a broker the fixture disposes at the end of the run.</summary>
    public IRequestBroker CreateBroker()
    {
        var broker = BrokerFactory();
        lock (_gate)
        {
            _brokers.Add(broker);
        }

        return broker;
    }

    /// <summary>Mints a request address for this run.</summary>
    public ValueTask<RequestAddress> CreateAddressAsync(string prefix) =>
        AddressFactory(prefix, CancellationToken);

    /// <summary>Mints a topic for this run, with the subscription names that will attach to it.</summary>
    public ValueTask<RequestAddress> CreateTopicAsync(
        string prefix,
        params string[] subscriptions
    ) => TopicFactory(prefix, subscriptions, CancellationToken);

    /// <summary>
    /// Builds and starts a host on this transport, with <paramref name="configure"/> adding the
    /// handlers and subscribers. The fixture stops and disposes it at the end of the run.
    /// </summary>
    public async Task<IHost> StartHostAsync(
        Action<HostLoomBuilder> configure,
        Action<IServiceCollection>? services = null
    )
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = Host.CreateApplicationBuilder(
            new HostApplicationBuilderSettings { DisableDefaults = true }
        );
        services?.Invoke(builder.Services);
        var hostLoom = builder.Services.AddHostLoom(options =>
            options.RequestTimeout = RequestTimeout
        );
        UseTransport(hostLoom);
        configure(hostLoom);

        var host = builder.Build();
        lock (_gate)
        {
            _hosts.Add(host);
        }

        await host.StartAsync(CancellationToken).ConfigureAwait(false);
        return host;
    }

    /// <summary>
    /// Stops and disposes every host, then disposes every broker. Disposal is idempotent on every
    /// transport, so a broker a scenario already disposed is disposed again harmlessly.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        IHost[] hosts;
        IRequestBroker[] brokers;
        lock (_gate)
        {
            hosts = [.. _hosts];
            brokers = [.. _brokers];
            _hosts.Clear();
            _brokers.Clear();
        }

        List<Exception>? failures = null;
        for (var i = hosts.Length - 1; i >= 0; i--)
        {
            var host = hosts[i];
            try
            {
                using var stopping = new CancellationTokenSource(Bound);
                await host.StopAsync(stopping.Token).ConfigureAwait(false);
                if (host is IAsyncDisposable asynchronous)
                {
                    await asynchronous.DisposeAsync().ConfigureAwait(false);
                }
                else
                {
                    host.Dispose();
                }
            }
            catch (Exception exception)
            {
                (failures ??= []).Add(exception);
            }
        }

        for (var i = brokers.Length - 1; i >= 0; i--)
        {
            try
            {
                await brokers[i].DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                (failures ??= []).Add(exception);
            }
        }

        if (failures is not null)
        {
            throw new AggregateException("Releasing the transport fixture failed.", failures);
        }
    }
}
