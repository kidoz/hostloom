using System.Collections.Concurrent;

namespace HostLoom.IntegrationTests;

public sealed record Greet(string Name) : IRequest<Greeting>;

public sealed record Greeting(string Text);

public sealed record Fail(string Reason) : IRequest<Never>;

public sealed record Never;

public sealed record OrderPlaced(string Reference) : IEvent;

public sealed class GreetHandler : IRequestHandler<Greet, Greeting>
{
    public ValueTask<Greeting> HandleAsync(Greet request, CancellationToken cancellationToken) =>
        ValueTask.FromResult(new Greeting($"Hello, {request.Name}!"));
}

public sealed class FailingHandler : IRequestHandler<Fail, Never>
{
    public ValueTask<Never> HandleAsync(Fail request, CancellationToken cancellationToken) =>
        throw new InvalidOperationException(request.Reason);
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

public sealed class ShippingHandler(Received received) : IEventHandler<OrderPlaced>
{
    public ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken)
    {
        received.Record($"shipping:{@event.Reference}");
        return ValueTask.CompletedTask;
    }
}
