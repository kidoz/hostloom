using System.Collections.Concurrent;

namespace HostLoom.IntegrationTests;

public sealed record Greet(string Name) : IRequest<Greeting>;

public sealed record Greeting(string Text);

public sealed record Fail(string Reason) : IRequest<Never>;

public sealed record Never;

public sealed record OrderPlaced(string Reference) : IEvent;

public sealed record Hold(int Index) : IRequest<Held>;

public sealed record Held(int Index);

/// <summary>
/// Parks handlers in flight until released, so dispatch concurrency is asserted on how many were
/// observed inside the handler at once rather than on elapsed time.
/// </summary>
public sealed class Gate(int expected)
{
    private readonly TaskCompletionSource _arrived = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private readonly TaskCompletionSource _released = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private int _inFlight;
    private int _peak;

    /// <summary>Completes once <paramref name="expected"/> handlers are parked at the same time.</summary>
    public Task AllArrived => _arrived.Task;

    /// <summary>The largest number of handlers ever parked at once.</summary>
    public int Peak => Volatile.Read(ref _peak);

    public async Task EnterAsync(CancellationToken cancellationToken)
    {
        var now = Interlocked.Increment(ref _inFlight);
        int peak;
        do
        {
            peak = Volatile.Read(ref _peak);
        } while (now > peak && Interlocked.CompareExchange(ref _peak, now, peak) != peak);
        if (now >= expected)
        {
            _arrived.TrySetResult();
        }

        try
        {
            await _released.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
        }
    }

    public void Release() => _released.TrySetResult();
}

public sealed class HoldingHandler(Gate gate) : IRequestHandler<Hold, Held>
{
    public async ValueTask<Held> HandleAsync(Hold request, CancellationToken cancellationToken)
    {
        await gate.EnterAsync(cancellationToken).ConfigureAwait(false);
        return new Held(request.Index);
    }
}

public sealed class GreetHandler : IRequestHandler<Greet, Greeting>
{
    public ValueTask<Greeting> HandleAsync(Greet request, CancellationToken cancellationToken) =>
        ValueTask.FromResult(new Greeting($"Hello, {request.Name}!"));
}

/// <summary>
/// Fails with a <see cref="RemoteFaultException"/>, the one exception whose message is forwarded
/// to the caller by default; any other type reaches the caller as an anonymous handler fault.
/// </summary>
public sealed class FailingHandler : IRequestHandler<Fail, Never>
{
    public ValueTask<Never> HandleAsync(Fail request, CancellationToken cancellationToken) =>
        throw new RemoteFaultException(request.Reason);
}

/// <summary>Collects deliveries across subscriptions so fan-out can be asserted on.</summary>
public sealed class Received
{
    private readonly ConcurrentQueue<string> _entries = new();
    private readonly TaskCompletionSource _idle = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private int _expected = int.MaxValue;

    public void Expect(int count)
    {
        Volatile.Write(ref _expected, count);
        Complete();
    }

    public void Record(string entry)
    {
        _entries.Enqueue(entry);
        Complete();
    }

    /// <summary>
    /// Waits for the expected number of deliveries rather than sleeping: a broker that delivers
    /// late fails on the bound instead of passing because the sleep happened to be long enough.
    /// </summary>
    public async Task<IReadOnlyList<string>> WaitAsync(TimeSpan timeout)
    {
        await _idle.Task.WaitAsync(timeout).ConfigureAwait(false);
        return [.. _entries.OrderBy(entry => entry, StringComparer.Ordinal)];
    }

    /// <summary>The same deliveries in arrival order, for asserting on ordering.</summary>
    public async Task<IReadOnlyList<string>> WaitInOrderAsync(TimeSpan timeout)
    {
        await _idle.Task.WaitAsync(timeout).ConfigureAwait(false);
        return [.. _entries];
    }

    private void Complete()
    {
        if (_entries.Count >= Volatile.Read(ref _expected))
        {
            _idle.TrySetResult();
        }
    }
}

public sealed class AuditHandler(Received received) : IEventHandler<OrderPlaced>
{
    public ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken)
    {
        received.Record($"audit:{@event.Reference}");
        return ValueTask.CompletedTask;
    }
}

public sealed class SequenceHandler(Received received) : IEventHandler<OrderPlaced>
{
    public ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken)
    {
        received.Record($"sequence:{@event.Reference}");
        return ValueTask.CompletedTask;
    }
}

public sealed class ShippingHandler(Received received) : IEventHandler<OrderPlaced>
{
    public ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken)
    {
        received.Record($"shipping:{@event.Reference}");
        return ValueTask.CompletedTask;
    }
}
