using System.Text;
using System.Text.Json;
using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Controlled faults around the endpoint host and the request client: a transport that refuses a
/// listen part way through startup, a subscription that fails to release, a caller that cancels
/// while a handler is running, a handler that never answers, and replies that belong to another
/// request. The hypothesis under every fault is the same: nothing is left bound after a failed
/// start, stopping and disposal stay idempotent, a bounded operation never hangs, caller
/// cancellation stays cancellation, and a reply the client cannot correlate is refused rather
/// than returned.
/// </summary>
public sealed class MessagingChaosTests
{
    private static readonly TimeSpan Bounded = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task A_listen_failure_part_way_through_startup_releases_the_endpoints_already_bound()
    {
        using var host = Build();
        var broker = Broker(host);
        broker.FailListenOn = 2;

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await host.StartAsync(TestContext.Current.CancellationToken)
        );

        Assert.Contains("listen #2", failure.Message, StringComparison.Ordinal);
        // The first endpoint was bound before the fault; the unwind gave it back, so the
        // transport holds nothing for a host that never reached a listening state.
        Assert.Equal(1, broker.DisposeAttempts);
        Assert.Equal(0, broker.LiveSubscriptions);
        Assert.Empty(broker.BoundAddresses);
    }

    [Fact]
    public async Task A_rollback_failure_during_startup_does_not_mask_the_listen_failure()
    {
        using var host = Build();
        var broker = Broker(host);
        broker.FailListenOn = 2;
        broker.FailDisposeOf.Add("orders");

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await host.StartAsync(TestContext.Current.CancellationToken)
        );

        // Both faults happen; only the actionable one reaches the operator.
        Assert.Contains("listen #2", failure.Message, StringComparison.Ordinal);
        Assert.Equal(1, broker.DisposeAttempts);
        Assert.Equal(1, broker.LiveSubscriptions);
    }

    [Fact]
    public async Task A_subscription_that_fails_to_release_still_releases_the_others()
    {
        using var host = Build();
        var broker = Broker(host);
        broker.FailDisposeOf.Add("inventory");
        await host.StartAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, broker.LiveSubscriptions);

        var failure = await Assert.ThrowsAsync<AggregateException>(async () =>
            await host.StopAsync(TestContext.Current.CancellationToken)
        );

        // Every subscription is attempted in reverse order, the one that refused is reported,
        // and the two that could be released are gone.
        Assert.Contains(
            failure.Flatten().InnerExceptions,
            inner => inner.Message.Contains("inventory", StringComparison.Ordinal)
        );
        Assert.Equal(3, broker.DisposeAttempts);
        Assert.Equal(1, broker.LiveSubscriptions);
        Assert.Equal(["inventory"], broker.BoundAddresses);
    }

    [Fact]
    public async Task Stopping_after_a_failed_stop_releases_nothing_twice_and_does_not_throw()
    {
        using var host = Build();
        var broker = Broker(host);
        broker.FailDisposeOf.Add("shipments");
        await host.StartAsync(TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<AggregateException>(async () =>
            await host.StopAsync(TestContext.Current.CancellationToken)
        );
        Assert.Equal(3, broker.DisposeAttempts);

        // The failed stop still emptied the list, so stopping again is a no-op rather than a
        // second attempt at a subscription the transport has already been told about.
        await host.StopAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, broker.DisposeAttempts);
    }

    [Fact]
    public async Task Caller_cancellation_during_a_handler_reaches_the_caller_as_cancellation()
    {
        var gate = new Gate();
        using var host = BuildInMemory(gate);
        await host.StartAsync(TestContext.Current.CancellationToken);
        var client = host.Services.GetRequiredService<IRequestClient<Reserve, Reserved>>();
        using var caller = new CancellationTokenSource();

        var pending = client
            .GetResponseAsync("inventory", new Reserve("R-1"), cancellationToken: caller.Token)
            .AsTask();
        await gate.Entered.Task.WaitAsync(Bounded, TestContext.Current.CancellationToken);
        await caller.CancelAsync();

        // Not a remote fault and not a timeout: the caller walked away, and that is what it sees.
        var cancelled = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        Assert.Equal(caller.Token, cancelled.CancellationToken);
        Assert.Equal(1, gate.Started);
        Assert.Equal(0, gate.Completed);
    }

    [Fact]
    public async Task A_handler_that_never_answers_is_bounded_by_the_request_timeout()
    {
        var gate = new Gate();
        var clock = new TestClock();
        using var host = BuildInMemory(gate, clock);
        await host.StartAsync(TestContext.Current.CancellationToken);
        var client = host.Services.GetRequiredService<IRequestClient<Reserve, Reserved>>();
        var budget = TimeSpan.FromMinutes(5);

        var pending = client
            .GetResponseAsync(
                "inventory",
                new Reserve("R-2"),
                budget,
                TestContext.Current.CancellationToken
            )
            .AsTask();
        await gate.Entered.Task.WaitAsync(Bounded, TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        clock.Advance(budget - TimeSpan.FromSeconds(1));
        Assert.False(pending.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(1));

        var timeout = await Assert.ThrowsAsync<RequestTimeoutException>(() => pending);

        Assert.Equal("inventory", timeout.Address.Value);
        Assert.Equal(budget, timeout.Timeout);
        // Recovery: the handler is released and the host stops without the abandoned call
        // stranding the endpoint.
        gate.Release.SetResult();
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_request_after_the_endpoint_stopped_fails_instead_of_hanging()
    {
        var gate = new Gate();
        var clock = new TestClock();
        using var host = BuildInMemory(gate, clock);
        await host.StartAsync(TestContext.Current.CancellationToken);
        await host.StopAsync(TestContext.Current.CancellationToken);
        var client = host.Services.GetRequiredService<IRequestClient<Reserve, Reserved>>();

        // An unbound endpoint waits out the whole request budget on the clock rather than
        // failing fast, so the caller sees the same timeout it would against a real broker.
        var budget = TimeSpan.FromMinutes(5);
        var pending = client
            .GetResponseAsync(
                "inventory",
                new Reserve("R-3"),
                budget,
                TestContext.Current.CancellationToken
            )
            .AsTask();
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        clock.Advance(budget - TimeSpan.FromSeconds(1));
        Assert.False(pending.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(1));

        var timeout = await Assert.ThrowsAsync<RequestTimeoutException>(() => pending);

        Assert.Equal(budget, timeout.Timeout);
        Assert.Equal(0, gate.Started);
    }

    [Fact]
    public async Task The_in_memory_transport_takes_the_time_provider_registered_before_it()
    {
        var clock = new TestClock();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<TimeProvider>(clock);
        builder.Services.AddHostLoom().UseInMemory().AddRequestClient<Reserve, Reserved>();
        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var client = host.Services.GetRequiredService<IRequestClient<Reserve, Reserved>>();

        // The transport is registered by type, so the container picks its richest satisfiable
        // constructor; the registered clock, not the system one, must be what bounds the wait.
        var pending = client
            .GetResponseAsync(
                "inventory",
                new Reserve("R-6"),
                TimeSpan.FromHours(1),
                TestContext.Current.CancellationToken
            )
            .AsTask();
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        Assert.False(pending.IsCompleted);
        clock.Advance(TimeSpan.FromHours(1));

        await Assert.ThrowsAsync<RequestTimeoutException>(() => pending);
    }

    [Fact]
    public async Task Stopping_the_listener_cancels_receiver_work()
    {
        var gate = new Gate();
        var clock = new TestClock();
        using var host = BuildInMemory(gate, clock);
        await host.StartAsync(TestContext.Current.CancellationToken);
        var client = host.Services.GetRequiredService<IRequestClient<Reserve, Reserved>>();

        var pending = client
            .GetResponseAsync(
                "inventory",
                new Reserve("R-4"),
                Bounded,
                TestContext.Current.CancellationToken
            )
            .AsTask();
        await gate.Entered.Task.WaitAsync(Bounded, TestContext.Current.CancellationToken);
        await host.StopAsync(TestContext.Current.CancellationToken);
        gate.Release.SetResult();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        Assert.Equal(0, gate.Completed);

        // The stop still took the endpoint away for anything that had not been accepted.
        var unbound = client
            .GetResponseAsync(
                "inventory",
                new Reserve("R-5"),
                TimeSpan.FromMinutes(5),
                TestContext.Current.CancellationToken
            )
            .AsTask();
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        clock.Advance(TimeSpan.FromMinutes(5));
        await Assert.ThrowsAsync<RequestTimeoutException>(() => unbound);
    }

    [Fact]
    public async Task A_reply_correlated_to_another_request_is_refused()
    {
        using var host = Build();
        var broker = Broker(host);
        await host.StartAsync(TestContext.Current.CancellationToken);
        broker.CannedReply = Reply(
            Guid.NewGuid(),
            "Response",
            TypeName<Reserved>(),
            Body(new Reserved("someone else's"))
        );
        var client = host.Services.GetRequiredService<IRequestClient<Reserve, Reserved>>();

        var malformed = await Assert.ThrowsAsync<MalformedEnvelopeException>(async () =>
            await client.GetResponseAsync(
                "inventory",
                new Reserve("R-6"),
                Bounded,
                TestContext.Current.CancellationToken
            )
        );

        Assert.Contains("did not match request", malformed.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_reply_of_another_response_type_is_refused()
    {
        using var host = Build();
        var broker = Broker(host);
        await host.StartAsync(TestContext.Current.CancellationToken);
        broker.CorrelateReply = true;
        broker.CannedReply = Reply(
            Guid.Empty,
            "Response",
            TypeName<Shipped>(),
            Body(new Shipped("S-1"))
        );
        var client = host.Services.GetRequiredService<IRequestClient<Reserve, Reserved>>();

        var malformed = await Assert.ThrowsAsync<MalformedEnvelopeException>(async () =>
            await client.GetResponseAsync(
                "inventory",
                new Reserve("R-7"),
                Bounded,
                TestContext.Current.CancellationToken
            )
        );

        // Correlation alone does not make a frame the response this caller asked for.
        Assert.Contains(TypeName<Reserved>(), malformed.Message, StringComparison.Ordinal);
        Assert.Contains(TypeName<Shipped>(), malformed.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_fault_naming_a_type_this_process_does_not_have_reaches_the_caller_as_text()
    {
        using var host = Build();
        var broker = Broker(host);
        await host.StartAsync(TestContext.Current.CancellationToken);
        broker.CorrelateReply = true;
        broker.CannedReply = Fault("Acme.Inventory.NoSuchFault", "the reserve ledger was busy");
        var client = host.Services.GetRequiredService<IRequestClient<Reserve, Reserved>>();

        var remote = await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await client.GetResponseAsync(
                "inventory",
                new Reserve("R-8"),
                Bounded,
                TestContext.Current.CancellationToken
            )
        );

        // The fault type is a wire name, never a type to resolve: a name no assembly here
        // defines still arrives, and arrives as data.
        Assert.Equal("Acme.Inventory.NoSuchFault", remote.ErrorType);
        Assert.Contains("the reserve ledger was busy", remote.Message, StringComparison.Ordinal);
        Assert.Null(Type.GetType("Acme.Inventory.NoSuchFault", throwOnError: false));
    }

    private static IHost Build()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<Gate>();
        builder
            .Services.AddHostLoom()
            .UseTransport<ChaosBroker>()
            .AddHandler<Place, Placed, PlaceHandler>("orders")
            .AddHandler<Reserve, Reserved, ReserveHandler>("inventory")
            .AddHandler<Ship, Shipped, ShipHandler>("shipments");
        return builder.Build();
    }

    private static IHost BuildInMemory(Gate gate, TestClock? clock = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(gate);
        if (clock is not null)
        {
            builder.Services.AddSingleton<TimeProvider>(clock);
        }

        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHandler<Reserve, Reserved, GatedReserveHandler>("inventory");
        return builder.Build();
    }

    private static ChaosBroker Broker(IHost host) =>
        (ChaosBroker)host.Services.GetRequiredService<IRequestBroker>();

    private static string TypeName<T>() =>
        $"{typeof(T).Assembly.GetName().Name}:{typeof(T).FullName}";

    private static string Body<T>(T value) =>
        Convert.ToBase64String(JsonSerializer.SerializeToUtf8Bytes(value));

    private static byte[] Reply(Guid correlationId, string kind, string messageType, string body) =>
        Encoding.UTF8.GetBytes(
            $$"""
            {
              "messageId": "{{Guid.NewGuid()}}",
              "correlationId": "{{correlationId}}",
              "kind": "{{kind}}",
              "messageType": "{{messageType}}",
              "responseType": "{{messageType}}",
              "sentAt": "2026-09-20T10:00:00Z",
              "body": "{{body}}"
            }
            """
        );

    private static byte[] Fault(string errorType, string message) =>
        Encoding.UTF8.GetBytes(
            $$"""
            {
              "messageId": "{{Guid.NewGuid()}}",
              "correlationId": "{{Guid.Empty}}",
              "kind": "Fault",
              "messageType": "{{TypeName<Reserved>()}}",
              "responseType": "{{TypeName<Reserved>()}}",
              "sentAt": "2026-09-20T10:00:00Z",
              "body": "",
              "fault": { "errorType": "{{errorType}}", "message": "{{message}}" }
            }
            """
        );

    public sealed record Place(string Reference) : IRequest<Placed>;

    public sealed record Placed(string Reference);

    public sealed record Reserve(string Reference) : IRequest<Reserved>;

    public sealed record Reserved(string Reference);

    public sealed record Ship(string Reference) : IRequest<Shipped>;

    public sealed record Shipped(string Reference);

    public sealed class PlaceHandler : IRequestHandler<Place, Placed>
    {
        public ValueTask<Placed> HandleAsync(Place request, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new Placed(request.Reference));
    }

    public sealed class ReserveHandler : IRequestHandler<Reserve, Reserved>
    {
        public ValueTask<Reserved> HandleAsync(
            Reserve request,
            CancellationToken cancellationToken
        ) => ValueTask.FromResult(new Reserved(request.Reference));
    }

    public sealed class ShipHandler : IRequestHandler<Ship, Shipped>
    {
        public ValueTask<Shipped> HandleAsync(Ship request, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new Shipped(request.Reference));
    }

    /// <summary>A handler that answers only when the test lets it, so a call can be caught mid-flight.</summary>
    public sealed class GatedReserveHandler(Gate gate) : IRequestHandler<Reserve, Reserved>
    {
        public async ValueTask<Reserved> HandleAsync(
            Reserve request,
            CancellationToken cancellationToken
        )
        {
            gate.Enter();
            await gate.Release.Task.WaitAsync(cancellationToken);
            gate.Complete();
            return new Reserved(request.Reference);
        }
    }

    /// <summary>The handler's side of a request the test holds open.</summary>
    public sealed class Gate
    {
        private int _started;
        private int _completed;

        public TaskCompletionSource Entered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Started => Volatile.Read(ref _started);

        public int Completed => Volatile.Read(ref _completed);

        public void Enter()
        {
            Interlocked.Increment(ref _started);
            Entered.TrySetResult();
        }

        public void Complete() => Interlocked.Increment(ref _completed);
    }

    /// <summary>
    /// A transport whose listens, releases, and replies are under the test's control: it can refuse
    /// the n-th listen, refuse to release a named endpoint, and answer a request with a frame the
    /// caller never asked for.
    /// </summary>
    public sealed class ChaosBroker : IRequestBroker
    {
        private readonly Lock _gate = new();
        private readonly Dictionary<RequestAddress, RequestFrameHandler> _handlers = [];
        private int _listenCalls;
        private int _disposeAttempts;

        /// <summary>The one-based listen call that throws instead of binding, if any.</summary>
        public int? FailListenOn { get; set; }

        /// <summary>Endpoints whose subscription throws when it is released.</summary>
        public HashSet<string> FailDisposeOf { get; } = new(StringComparer.Ordinal);

        /// <summary>The frame to answer every request with, instead of reaching a handler.</summary>
        public ReadOnlyMemory<byte>? CannedReply { get; set; }

        /// <summary>Rewrites <see cref="CannedReply"/> to carry the real request's correlation id.</summary>
        public bool CorrelateReply { get; set; }

        public int DisposeAttempts => Volatile.Read(ref _disposeAttempts);

        public int LiveSubscriptions
        {
            get
            {
                lock (_gate)
                {
                    return _handlers.Count;
                }
            }
        }

        public IReadOnlyList<string> BoundAddresses
        {
            get
            {
                lock (_gate)
                {
                    return [.. _handlers.Keys.Select(address => address.Value)];
                }
            }
        }

        public ValueTask<IAsyncDisposable> ListenAsync(
            RequestAddress address,
            RequestFrameHandler handler,
            CancellationToken cancellationToken
        )
        {
            lock (_gate)
            {
                _listenCalls++;
                if (_listenCalls == FailListenOn)
                {
                    throw new InvalidOperationException(
                        $"The transport refused listen #{_listenCalls} for '{address}'."
                    );
                }

                _handlers[address] = handler;
            }

            // CA2000: ownership of the subscription transfers to the caller, which releases it.
#pragma warning disable CA2000
            return ValueTask.FromResult<IAsyncDisposable>(new Subscription(this, address));
#pragma warning restore CA2000
        }

        public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(
            RequestAddress address,
            ReadOnlyMemory<byte> request,
            Guid requestId,
            TimeSpan timeout,
            CancellationToken cancellationToken
        )
        {
            if (CannedReply is { } canned)
            {
                return CorrelateReply ? Correlate(canned, requestId) : canned;
            }

            RequestFrameHandler handler;
            lock (_gate)
            {
                if (!_handlers.TryGetValue(address, out var bound))
                {
                    throw new RequestTimeoutException(address, timeout);
                }

                handler = bound;
            }

            return await handler(request, cancellationToken);
        }

        public ValueTask DisposeAsync()
        {
            lock (_gate)
            {
                _handlers.Clear();
            }

            return ValueTask.CompletedTask;
        }

        /// <summary>Replaces the placeholder correlation id with the one the client is waiting on.</summary>
        private static ReadOnlyMemory<byte> Correlate(ReadOnlyMemory<byte> frame, Guid requestId) =>
            Encoding.UTF8.GetBytes(
                Encoding
                    .UTF8.GetString(frame.Span)
                    .Replace(Guid.Empty.ToString(), requestId.ToString(), StringComparison.Ordinal)
            );

        private void Release(RequestAddress address)
        {
            Interlocked.Increment(ref _disposeAttempts);
            if (FailDisposeOf.Contains(address.Value))
            {
                throw new InvalidOperationException(
                    $"The transport could not release the subscription for '{address}'."
                );
            }

            lock (_gate)
            {
                _handlers.Remove(address);
            }
        }

        private sealed class Subscription(ChaosBroker broker, RequestAddress address)
            : IAsyncDisposable
        {
            public ValueTask DisposeAsync()
            {
                broker.Release(address);
                return ValueTask.CompletedTask;
            }
        }
    }
}
