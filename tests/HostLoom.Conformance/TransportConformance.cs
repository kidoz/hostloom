using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Conformance;

/// <summary>
/// Transport-neutral request/response and publish/subscribe scenarios. The unit suite runs them on
/// the in-memory transport; the integration suite runs the same methods on RabbitMQ and Kafka.
/// Most drive <see cref="IRequestBroker"/> and <see cref="IEventBroker"/> with raw frames, the
/// contract a transport implements. The two whose behaviour the runtime above the transport
/// completes, fault replies and handlers sharing a subscription, run through a host.
/// </summary>
/// <remarks>
/// Two behaviours are left out on purpose, because the transports are changing them: the
/// exception type a transport failure surfaces as, and what a requester sees when the listener
/// serving it stops while its handler is still running. Scenarios that stop a listener mid-request
/// only require that the request ends. Both belong here once they settle.
/// </remarks>
public static class TransportConformance
{
    private static readonly TimeSpan Tick = TimeSpan.FromMilliseconds(1);

    /// <summary>Every scenario by name, so a test project can enumerate them as theory data.</summary>
    public static IReadOnlyDictionary<
        string,
        Func<TransportConformanceFixture, Task>
    > Scenarios { get; } =
        new Dictionary<string, Func<TransportConformanceFixture, Task>>(StringComparer.Ordinal)
        {
            [nameof(RoundTrip_ReturnsTheHandlersReply)] = RoundTrip_ReturnsTheHandlersReply,
            [nameof(ConcurrentRequests_EachReceiveTheirOwnReply)] =
                ConcurrentRequests_EachReceiveTheirOwnReply,
            [nameof(HandlerFault_ReachesTheCallerAsRemoteRequestException)] =
                HandlerFault_ReachesTheCallerAsRemoteRequestException,
            [nameof(UnboundAddress_TimesOutWithinTheBudget)] =
                UnboundAddress_TimesOutWithinTheBudget,
            [nameof(SilentHandler_IsBoundedByTheRequestTimeout)] =
                SilentHandler_IsBoundedByTheRequestTimeout,
            [nameof(CallerCancellation_ReachesTheCallerAndSparesTheHandler)] =
                CallerCancellation_ReachesTheCallerAndSparesTheHandler,
            [nameof(ListenerDisposal_IsIdempotentAndCancelsTheHandler)] =
                ListenerDisposal_IsIdempotentAndCancelsTheHandler,
            [nameof(BrokerDisposal_EndsAPendingRequest)] = BrokerDisposal_EndsAPendingRequest,
            [nameof(CallsAfterDisposal_ThrowObjectDisposed)] =
                CallsAfterDisposal_ThrowObjectDisposed,
            [nameof(Publish_FansOutToEveryNamedSubscription)] =
                Publish_FansOutToEveryNamedSubscription,
            [nameof(HandlersSharingASubscription_ShareOneDelivery)] =
                HandlersSharingASubscription_ShareOneDelivery,
            [nameof(Publish_WithNoSubscribers_Succeeds)] = Publish_WithNoSubscribers_Succeeds,
            [nameof(HealthProbe_NeverThrowsAndReportsDisposal)] =
                HealthProbe_NeverThrowsAndReportsDisposal,
            [nameof(Subscription_ReceivesEventsInPublishOrder)] =
                Subscription_ReceivesEventsInPublishOrder,
            [nameof(SecondListener_IsRefusedOrTakesOver)] = SecondListener_IsRefusedOrTakesOver,
        };

    public static async Task RoundTrip_ReturnsTheHandlersReply(TransportConformanceFixture fixture)
    {
        var broker = fixture.CreateBroker();
        var address = await fixture.CreateAddressAsync("round-trip");
        var received = new TaskCompletionSource<string>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        await using var listener = await ListenAsync(
            fixture,
            broker,
            address,
            (request, _) =>
            {
                received.TrySetResult(request);
                return ValueTask.FromResult($"reply:{request}");
            }
        );

        var reply = await RequestAsync(fixture, broker, address, "catalog:item-42");

        Assert.Equal("reply:catalog:item-42", reply);
        Assert.Equal(
            "catalog:item-42",
            await received.Task.WaitAsync(fixture.Bound, fixture.CancellationToken)
        );
    }

    public static async Task ConcurrentRequests_EachReceiveTheirOwnReply(
        TransportConformanceFixture fixture
    )
    {
        const int Count = 100;
        var broker = fixture.CreateBroker();
        var address = await fixture.CreateAddressAsync("concurrent");
        await using var listener = await ListenAsync(
            fixture,
            broker,
            address,
            (request, _) => ValueTask.FromResult($"reply:{request}")
        );

        // Every reply comes back on one shared reply path, so correlation alone keeps them apart;
        // a correlation fault shows up here and never in a serial round trip.
        var replies = await Task.WhenAll(
            Enumerable
                .Range(0, Count)
                .Select(i => RequestAsync(fixture, broker, address, $"orders:{i:D3}"))
        );

        for (var i = 0; i < Count; i++)
        {
            Assert.Equal($"reply:orders:{i:D3}", replies[i]);
        }
    }

    public static async Task HandlerFault_ReachesTheCallerAsRemoteRequestException(
        TransportConformanceFixture fixture
    )
    {
        var address = await fixture.CreateAddressAsync("stock");
        var host = await fixture.StartHostAsync(hostLoom =>
            hostLoom.AddHandler<ReserveStock, StockReserved, ReserveStockHandler>(address)
        );
        var client = host.Services.GetRequiredService<
            IRequestClient<ReserveStock, StockReserved>
        >();

        var declared = await Assert.ThrowsAsync<RemoteRequestException>(() =>
            ReserveAsync(ReserveStockHandler.UnknownSku)
        );
        Assert.Equal(typeof(RemoteFaultException).FullName, declared.ErrorType);
        Assert.Contains(
            "unknown-sku is not in the catalog.",
            declared.Message,
            StringComparison.Ordinal
        );

        // Any other exception crosses the transport as an anonymous handler fault.
        var hidden = await Assert.ThrowsAsync<RemoteRequestException>(() => ReserveAsync("sku-7"));
        Assert.Equal("HandlerFault", hidden.ErrorType);
        Assert.DoesNotContain(
            ReserveStockHandler.InternalDetail,
            hidden.Message,
            StringComparison.Ordinal
        );
        Assert.DoesNotContain(
            nameof(InvalidOperationException),
            hidden.Message,
            StringComparison.Ordinal
        );

        Task<StockReserved> ReserveAsync(string sku) =>
            client
                .GetResponseAsync(
                    address,
                    new ReserveStock(sku),
                    fixture.RequestTimeout,
                    fixture.CancellationToken
                )
                .AsTask()
                .WaitAsync(fixture.Bound, fixture.CancellationToken);
    }

    public static async Task UnboundAddress_TimesOutWithinTheBudget(
        TransportConformanceFixture fixture
    )
    {
        var broker = fixture.CreateBroker();
        var address = await fixture.CreateAddressAsync("unbound");
        var budget = fixture.TimeoutBudget;

        var started = fixture.Clock.Provider.GetTimestamp();
        var pending = broker
            .RequestAsync(
                address,
                Frame("inventory:unbound"),
                Guid.NewGuid(),
                budget,
                fixture.CancellationToken
            )
            .AsTask();
        await ElapseBudgetAsync(fixture, pending, budget);

        await SettleAsync(fixture, pending, "The unbound request");
        var timeout = await Assert.ThrowsAsync<RequestTimeoutException>(() => pending);
        AssertTimedOutWithinBudget(fixture, started, budget, timeout, address);
    }

    public static async Task SilentHandler_IsBoundedByTheRequestTimeout(
        TransportConformanceFixture fixture
    )
    {
        var broker = fixture.CreateBroker();
        var address = await fixture.CreateAddressAsync("silent");
        var parking = new Parking();
        await using var listener = await ListenAsync(fixture, broker, address, parking.HandleAsync);
        // Warm the reply path and the listener, so the budget below is spent on the handler alone.
        Assert.Equal("pong", await RequestAsync(fixture, broker, address, Parking.Ping));

        var budget = fixture.TimeoutBudget;
        var started = fixture.Clock.Provider.GetTimestamp();
        var pending = broker
            .RequestAsync(
                address,
                Frame("inventory:hold"),
                Guid.NewGuid(),
                budget,
                fixture.CancellationToken
            )
            .AsTask();
        var handlerToken = await parking.Entered.Task.WaitAsync(
            fixture.Bound,
            fixture.CancellationToken
        );
        await ElapseBudgetAsync(fixture, pending, budget);

        await SettleAsync(fixture, pending, "The request to a silent handler");
        var timeout = await Assert.ThrowsAsync<RequestTimeoutException>(() => pending);
        AssertTimedOutWithinBudget(fixture, started, budget, timeout, address);
        // The caller stopped waiting; the work it started is still the listener's to finish.
        Assert.False(handlerToken.IsCancellationRequested);

        parking.Release.SetResult();
        await parking.Returned.Task.WaitAsync(fixture.Bound, fixture.CancellationToken);
    }

    public static async Task CallerCancellation_ReachesTheCallerAndSparesTheHandler(
        TransportConformanceFixture fixture
    )
    {
        var broker = fixture.CreateBroker();
        var address = await fixture.CreateAddressAsync("cancelled");
        var parking = new Parking();
        await using var listener = await ListenAsync(fixture, broker, address, parking.HandleAsync);
        using var caller = new CancellationTokenSource();

        var pending = broker
            .RequestAsync(
                address,
                Frame("inventory:hold"),
                Guid.NewGuid(),
                fixture.RequestTimeout,
                caller.Token
            )
            .AsTask();
        var handlerToken = await parking.Entered.Task.WaitAsync(
            fixture.Bound,
            fixture.CancellationToken
        );
        await caller.CancelAsync();

        // Not a timeout and not a remote fault: the caller walked away, and that is what it sees.
        await SettleAsync(fixture, pending, "The cancelled request");
        var cancelled = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        if (fixture.Profile.CancellationCarriesCallerToken)
        {
            Assert.Equal(caller.Token, cancelled.CancellationToken);
        }
        else
        {
            // RabbitMQ lets the cancellation of the token it linked the caller's to escape as
            // is, so the exception names that linked token instead of the caller's own.
            Assert.True(caller.IsCancellationRequested);
            Assert.True(cancelled.CancellationToken.IsCancellationRequested);
            Assert.NotEqual(caller.Token, cancelled.CancellationToken);
        }

        // The handler's token belongs to the listener, so the caller leaving does not reach it,
        // and the accepted work runs to completion.
        Assert.False(handlerToken.IsCancellationRequested);
        parking.Release.SetResult();
        await parking.Returned.Task.WaitAsync(fixture.Bound, fixture.CancellationToken);
        Assert.False(handlerToken.IsCancellationRequested);

        // The abandoned request left the listener serving.
        Assert.Equal("pong", await RequestAsync(fixture, broker, address, Parking.Ping));
    }

    public static async Task ListenerDisposal_IsIdempotentAndCancelsTheHandler(
        TransportConformanceFixture fixture
    )
    {
        var broker = fixture.CreateBroker();
        var address = await fixture.CreateAddressAsync("stopping");
        var parking = new Parking();
        var listener = await ListenAsync(fixture, broker, address, parking.HandleAsync);
        using var caller = new CancellationTokenSource();
        var pending = broker
            .RequestAsync(
                address,
                Frame("orders:hold"),
                Guid.NewGuid(),
                fixture.RequestTimeout,
                caller.Token
            )
            .AsTask();
        var handlerToken = await parking.Entered.Task.WaitAsync(
            fixture.Bound,
            fixture.CancellationToken
        );
        var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = handlerToken.Register(() => stopped.TrySetResult());
        Assert.False(handlerToken.IsCancellationRequested);

        await listener.DisposeAsync().AsTask().WaitAsync(fixture.Bound, fixture.CancellationToken);

        await stopped.Task.WaitAsync(fixture.Bound, fixture.CancellationToken);
        Assert.True(handlerToken.IsCancellationRequested);
        // Disposing again is a no-op, not a second teardown or an exception.
        await listener.DisposeAsync().AsTask().WaitAsync(fixture.Bound, fixture.CancellationToken);

        // What the requester sees when its listener stops mid-request is not asserted here (see
        // the remarks on this class); the caller walks away, and the request only has to end.
        await caller.CancelAsync();
        await SettleAsync(fixture, pending, "The request whose listener stopped");
    }

    public static async Task BrokerDisposal_EndsAPendingRequest(TransportConformanceFixture fixture)
    {
        var requester = fixture.CreateBroker();
        var address = await fixture.CreateAddressAsync("disposal");
        var parking = new Parking();
        // Where instances reach each other, a listener on another instance holds the request, so
        // it is provably pending, and disposing the requester stops no listener. A process-local
        // instance reaches no other, and a listener on it would stop with it, so there the
        // request goes to an address nobody listens on.
        await using var listener = fixture.Profile.SharedAcrossInstances
            ? await ListenAsync(fixture, fixture.CreateBroker(), address, parking.HandleAsync)
            : null;
        var budget = fixture.Profile.DisposalFailsPendingRequests
            ? fixture.RequestTimeout
            : fixture.TimeoutBudget;

        var started = fixture.Clock.Provider.GetTimestamp();
        var pending = requester
            .RequestAsync(
                address,
                Frame("invoices:pending"),
                Guid.NewGuid(),
                budget,
                fixture.CancellationToken
            )
            .AsTask();
        if (listener is not null)
        {
            await parking.Entered.Task.WaitAsync(fixture.Bound, fixture.CancellationToken);
        }

        await requester.DisposeAsync().AsTask().WaitAsync(fixture.Bound, fixture.CancellationToken);

        if (fixture.Profile.DisposalFailsPendingRequests)
        {
            await SettleAsync(fixture, pending, "The request pending at disposal");
            await Assert.ThrowsAsync<ObjectDisposedException>(() => pending);
            // Ended by the disposal, not by running out its budget.
            Assert.True(fixture.Clock.Provider.GetElapsedTime(started) < budget);
        }
        else
        {
            // The in-memory transport documents that an unbound request waits for its timeout,
            // and disposal does not cut the wait short: the request still ends at its budget.
            await ElapseBudgetAsync(fixture, pending, budget);
            await SettleAsync(fixture, pending, "The request pending at disposal");
            var timeout = await Assert.ThrowsAsync<RequestTimeoutException>(() => pending);
            AssertTimedOutWithinBudget(fixture, started, budget, timeout, address);
        }

        if (listener is not null)
        {
            // The held handler answers into a reply path that is gone, then its listener stops.
            parking.Release.SetResult();
            await parking.Returned.Task.WaitAsync(fixture.Bound, fixture.CancellationToken);
        }
    }

    public static async Task CallsAfterDisposal_ThrowObjectDisposed(
        TransportConformanceFixture fixture
    )
    {
        var broker = fixture.CreateBroker();
        var address = await fixture.CreateAddressAsync("disposed");
        var topic = await fixture.CreateTopicAsync("disposed", "audit");
        // Used first, so disposal tears down live connections and consumers, not an idle instance.
        await using (
            await ListenAsync(
                fixture,
                broker,
                address,
                (request, _) => ValueTask.FromResult(request)
            )
        )
        {
            Assert.Equal(
                "orders:warm",
                await RequestAsync(fixture, broker, address, "orders:warm")
            );
        }

        await broker.DisposeAsync().AsTask().WaitAsync(fixture.Bound, fixture.CancellationToken);
        // Idempotent: a second disposal neither throws nor hangs.
        await broker.DisposeAsync().AsTask().WaitAsync(fixture.Bound, fixture.CancellationToken);

        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            Bounded(
                fixture,
                broker
                    .RequestAsync(
                        address,
                        Frame("orders:late"),
                        Guid.NewGuid(),
                        fixture.RequestTimeout,
                        fixture.CancellationToken
                    )
                    .AsTask()
            )
        );
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            Bounded(
                fixture,
                broker
                    .ListenAsync(
                        address,
                        (frame, _) => ValueTask.FromResult(frame),
                        fixture.CancellationToken
                    )
                    .AsTask()
            )
        );
        if (EventsOf(fixture, broker) is { } events)
        {
            await Assert.ThrowsAsync<ObjectDisposedException>(() =>
                Bounded(
                    fixture,
                    events
                        .SubscribeAsync(
                            topic,
                            "audit",
                            (_, _) => ValueTask.CompletedTask,
                            fixture.CancellationToken
                        )
                        .AsTask()
                )
            );
            await Assert.ThrowsAsync<ObjectDisposedException>(() =>
                Bounded(
                    fixture,
                    events
                        .PublishAsync(topic, Frame("orders:late"), fixture.CancellationToken)
                        .AsTask()
                )
            );
        }
    }

    public static async Task Publish_FansOutToEveryNamedSubscription(
        TransportConformanceFixture fixture
    )
    {
        if (EventsOf(fixture, fixture.CreateBroker()) is not { } events)
        {
            return;
        }

        var topic = await fixture.CreateTopicAsync("fan-out", "audit", "shipping");
        var audit = new FrameRecorder();
        var shipping = new FrameRecorder();
        await using var auditing = await events.SubscribeAsync(
            topic,
            "audit",
            audit.Handler,
            fixture.CancellationToken
        );
        await using var shipments = await events.SubscribeAsync(
            topic,
            "shipping",
            shipping.Handler,
            fixture.CancellationToken
        );

        await Bounded(
            fixture,
            events.PublishAsync(topic, Frame("orders:A-1"), fixture.CancellationToken).AsTask()
        );

        Assert.Equal(
            ["orders:A-1"],
            await audit.WaitForAsync(1, fixture.Bound, fixture.CancellationToken)
        );
        Assert.Equal(
            ["orders:A-1"],
            await shipping.WaitForAsync(1, fixture.Bound, fixture.CancellationToken)
        );
    }

    public static async Task HandlersSharingASubscription_ShareOneDelivery(
        TransportConformanceFixture fixture
    )
    {
        // A host with subscribers does not start on a transport without publish/subscribe.
        if (!fixture.Profile.PublishSubscribe)
        {
            return;
        }

        var topic = await fixture.CreateTopicAsync("invoices", "billing");
        var journal = new InvoiceJournal();
        var host = await fixture.StartHostAsync(
            hostLoom =>
                hostLoom
                    .AddSubscriber<InvoiceIssued, LedgerHandler>(topic, "billing")
                    .AddSubscriber<InvoiceIssued, ReminderHandler>(topic, "billing"),
            services => services.AddSingleton(journal).AddScoped<DeliveryScope>()
        );
        var publisher = host.Services.GetRequiredService<IPublishEndpoint>();

        await Bounded(
            fixture,
            publisher
                .PublishAsync(topic, new InvoiceIssued("INV-1"), fixture.CancellationToken)
                .AsTask()
        );
        await Bounded(
            fixture,
            publisher
                .PublishAsync(topic, new InvoiceIssued("INV-2"), fixture.CancellationToken)
                .AsTask()
        );

        var runs = await journal.WaitForAsync(4, fixture.Bound, fixture.CancellationToken);
        Assert.Equal(
            ["ledger:INV-1", "ledger:INV-2", "reminder:INV-1", "reminder:INV-2"],
            runs.Select(run => $"{run.Handler}:{run.Number}").Order(StringComparer.Ordinal)
        );
        // One delivery per event, shared by both handlers: they ran in one scope per event.
        var first = Assert.Single(ScopesOf("INV-1"));
        var second = Assert.Single(ScopesOf("INV-2"));
        Assert.NotEqual(first, second);

        IEnumerable<Guid> ScopesOf(string number) =>
            runs.Where(run => string.Equals(run.Number, number, StringComparison.Ordinal))
                .Select(run => run.Scope)
                .Distinct();
    }

    public static async Task Publish_WithNoSubscribers_Succeeds(TransportConformanceFixture fixture)
    {
        if (EventsOf(fixture, fixture.CreateBroker()) is not { } events)
        {
            return;
        }

        var topic = await fixture.CreateTopicAsync("unheard");

        // Nobody is subscribed: nothing to deliver, and nothing to fail. The second publish shows
        // the first left the publishing path usable.
        await Bounded(
            fixture,
            events.PublishAsync(topic, Frame("catalog:unheard"), fixture.CancellationToken).AsTask()
        );
        await Bounded(
            fixture,
            events
                .PublishAsync(topic, Frame("catalog:unheard-again"), fixture.CancellationToken)
                .AsTask()
        );
    }

    public static async Task HealthProbe_NeverThrowsAndReportsDisposal(
        TransportConformanceFixture fixture
    )
    {
        var broker = fixture.CreateBroker();
        Assert.Equal(fixture.Profile.HealthProbe, broker is IBrokerHealthProbe);
        if (broker is not IBrokerHealthProbe probe)
        {
            return;
        }

        // Before any use it answers, whatever it reports, rather than throwing.
        var fresh = await CheckAsync();
        Assert.False(string.IsNullOrWhiteSpace(fresh.Description));

        var address = await fixture.CreateAddressAsync("health");
        await using (
            await ListenAsync(
                fixture,
                broker,
                address,
                (request, _) => ValueTask.FromResult(request)
            )
        )
        {
            Assert.Equal(
                "customers:ping",
                await RequestAsync(fixture, broker, address, "customers:ping")
            );
        }

        var serving = await CheckAsync();
        Assert.True(serving.IsHealthy, serving.Description);

        await broker.DisposeAsync().AsTask().WaitAsync(fixture.Bound, fixture.CancellationToken);

        var disposed = await CheckAsync();
        Assert.False(disposed.IsHealthy, disposed.Description);
        Assert.False(string.IsNullOrWhiteSpace(disposed.Description));

        Task<BrokerHealth> CheckAsync() =>
            probe
                .CheckHealthAsync(fixture.CancellationToken)
                .AsTask()
                .WaitAsync(fixture.Bound, fixture.CancellationToken);
    }

    public static async Task Subscription_ReceivesEventsInPublishOrder(
        TransportConformanceFixture fixture
    )
    {
        if (EventsOf(fixture, fixture.CreateBroker()) is not { } events)
        {
            return;
        }

        var topic = await fixture.CreateTopicAsync("sequence", "sequence");
        var recorder = new FrameRecorder();
        await using var subscription = await events.SubscribeAsync(
            topic,
            "sequence",
            recorder.Handler,
            fixture.CancellationToken
        );
        string[] published = [.. Enumerable.Range(0, 16).Select(i => $"orders:S-{i:D2}")];

        // Each publish completes before the next starts, so publish order is well defined.
        foreach (var frame in published)
        {
            await Bounded(
                fixture,
                events.PublishAsync(topic, Frame(frame), fixture.CancellationToken).AsTask()
            );
        }

        var received = await recorder.WaitForAsync(
            published.Length,
            fixture.Bound,
            fixture.CancellationToken
        );
        if (fixture.Profile.OrderedWithinSubscription)
        {
            Assert.Equal(published, received);
        }
        else
        {
            // Without an ordering guarantee every event still arrives, once.
            Assert.Equal(published, received.Order(StringComparer.Ordinal));
        }
    }

    public static async Task SecondListener_IsRefusedOrTakesOver(
        TransportConformanceFixture fixture
    )
    {
        var broker = fixture.CreateBroker();
        var address = await fixture.CreateAddressAsync("competing");
        await using var first = await ListenAsync(
            fixture,
            broker,
            address,
            (request, _) => ValueTask.FromResult($"first:{request}")
        );

        if (!fixture.Profile.CompetingListeners)
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                Bounded(
                    fixture,
                    ListenAsync(
                            fixture,
                            broker,
                            address,
                            (request, _) => ValueTask.FromResult($"second:{request}")
                        )
                        .AsTask()
                )
            );
            // The refusal left the first listener in place.
            Assert.Equal(
                "first:orders:1",
                await RequestAsync(fixture, broker, address, "orders:1")
            );
            return;
        }

        await using var second = await ListenAsync(
            fixture,
            broker,
            address,
            (request, _) => ValueTask.FromResult($"second:{request}")
        );
        Assert.Matches(
            "^(first|second):orders:1$",
            await RequestAsync(fixture, broker, address, "orders:1")
        );

        await first.DisposeAsync().AsTask().WaitAsync(fixture.Bound, fixture.CancellationToken);

        // The address outlives the listener that left: the one still attached answers.
        Assert.Equal("second:orders:2", await RequestAsync(fixture, broker, address, "orders:2"));
    }

    private static ReadOnlyMemory<byte> Frame(string text) => Encoding.UTF8.GetBytes(text);

    private static string Text(ReadOnlyMemory<byte> frame) => Encoding.UTF8.GetString(frame.Span);

    /// <summary>The broker's publish/subscribe side, after checking the profile declares it truthfully.</summary>
    private static IEventBroker? EventsOf(
        TransportConformanceFixture fixture,
        IRequestBroker broker
    )
    {
        Assert.Equal(fixture.Profile.PublishSubscribe, broker is IEventBroker);
        return broker as IEventBroker;
    }

    private static ValueTask<IAsyncDisposable> ListenAsync(
        TransportConformanceFixture fixture,
        IRequestBroker broker,
        RequestAddress address,
        Func<string, CancellationToken, ValueTask<string>> handler
    ) =>
        broker.ListenAsync(
            address,
            async (frame, cancellationToken) =>
                Frame(await handler(Text(frame), cancellationToken).ConfigureAwait(false)),
            fixture.CancellationToken
        );

    private static async Task<string> RequestAsync(
        TransportConformanceFixture fixture,
        IRequestBroker broker,
        RequestAddress address,
        string request
    )
    {
        var reply = await Bounded(
                fixture,
                broker
                    .RequestAsync(
                        address,
                        Frame(request),
                        Guid.NewGuid(),
                        fixture.RequestTimeout,
                        fixture.CancellationToken
                    )
                    .AsTask()
            )
            .ConfigureAwait(false);
        return Text(reply);
    }

    private static Task Bounded(TransportConformanceFixture fixture, Task operation) =>
        operation.WaitAsync(fixture.Bound, fixture.CancellationToken);

    private static Task<T> Bounded<T>(TransportConformanceFixture fixture, Task<T> operation) =>
        operation.WaitAsync(fixture.Bound, fixture.CancellationToken);

    /// <summary>
    /// Waits for <paramref name="operation"/> to finish, however it finishes, and fails the
    /// scenario if it does not within the bound. Its outcome is the scenario's to assert on.
    /// </summary>
    private static async Task SettleAsync(
        TransportConformanceFixture fixture,
        Task operation,
        string what
    )
    {
        try
        {
            await operation
                .WaitAsync(fixture.Bound, fixture.CancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception) when (operation.IsCompleted)
        {
            // The operation's own outcome.
        }
        catch (TimeoutException)
        {
            Assert.Fail($"{what} did not end within {fixture.Bound}.");
        }
    }

    /// <summary>
    /// Lets a request's budget pass. On a manual clock this stops a moment short first and checks
    /// the request is still waiting; on the system clock that check runs right after sending,
    /// where a request failing fast instead of waiting would show.
    /// </summary>
    private static async Task ElapseBudgetAsync(
        TransportConformanceFixture fixture,
        Task pending,
        TimeSpan budget
    )
    {
        await fixture.Clock.AdvanceAsync(budget - Tick, fixture.CancellationToken);
        Assert.False(pending.IsCompleted, $"The request ended before its {budget} budget.");
        await fixture.Clock.AdvanceAsync(Tick, fixture.CancellationToken);
    }

    private static void AssertTimedOutWithinBudget(
        TransportConformanceFixture fixture,
        long started,
        TimeSpan budget,
        RequestTimeoutException timeout,
        RequestAddress address
    )
    {
        var elapsed = fixture.Clock.Provider.GetElapsedTime(started);
        Assert.InRange(elapsed, budget - fixture.Clock.Resolution, budget + fixture.Slack);
        Assert.Equal(address, timeout.Address);
        Assert.Equal(budget, timeout.Timeout);
    }

    /// <summary>
    /// A request handler that answers <see cref="Ping"/> at once and holds anything else until
    /// released, reporting the token it was handed.
    /// </summary>
    private sealed class Parking
    {
        public const string Ping = "inventory:ping";

        public TaskCompletionSource<CancellationToken> Entered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Returned { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async ValueTask<string> HandleAsync(
            string request,
            CancellationToken cancellationToken
        )
        {
            if (string.Equals(request, Ping, StringComparison.Ordinal))
            {
                return "pong";
            }

            Entered.TrySetResult(cancellationToken);
            await Release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            Returned.TrySetResult();
            return $"released:{request}";
        }
    }
}
