using System.Collections.Concurrent;
using System.Text;

namespace HostLoom.Conformance;

/// <summary>Asks for stock to be set aside; the conformance handler always refuses.</summary>
public sealed record ReserveStock(string Sku) : IRequest<StockReserved>;

/// <summary>The reply a successful reservation would carry.</summary>
public sealed record StockReserved(string Sku);

/// <summary>
/// Faults every reservation: with a <see cref="RemoteFaultException"/> for a SKU that is not in
/// the catalog, whose message is written for the caller, and with an internal failure for anything
/// else, which must reach the caller without its type or message.
/// </summary>
public sealed class ReserveStockHandler : IRequestHandler<ReserveStock, StockReserved>
{
    /// <summary>The SKU refused with a message meant for the caller.</summary>
    public const string UnknownSku = "unknown-sku";

    /// <summary>The internal detail that must not cross the transport.</summary>
    public const string InternalDetail = "inventory ledger row is locked";

    public ValueTask<StockReserved> HandleAsync(
        ReserveStock request,
        CancellationToken cancellationToken
    ) =>
        string.Equals(request.Sku, UnknownSku, StringComparison.Ordinal)
            ? throw new RemoteFaultException($"{request.Sku} is not in the catalog.")
            : throw new InvalidOperationException(InternalDetail);
}

/// <summary>An invoice was issued.</summary>
public sealed record InvoiceIssued(string Number) : IEvent;

/// <summary>
/// Registered scoped, so the handlers that ran in one delivery see one instance and the handlers
/// of another delivery see another.
/// </summary>
public sealed class DeliveryScope
{
    public Guid Id { get; } = Guid.NewGuid();
}

/// <summary>One handler run: which handler, for which invoice, in which delivery scope.</summary>
public sealed record JournalEntry(string Handler, string Number, Guid Scope);

/// <summary>Collects handler runs and lets a scenario wait for a count instead of sleeping.</summary>
public sealed class InvoiceJournal
{
    private readonly ConcurrentQueue<JournalEntry> _entries = new();
    private readonly Lock _gate = new();
    private readonly List<(int Count, TaskCompletionSource Reached)> _waiters = [];

    public void Record(JournalEntry entry)
    {
        _entries.Enqueue(entry);
        Signal();
    }

    /// <summary>Waits until at least <paramref name="count"/> runs were recorded, then returns them all.</summary>
    public async Task<IReadOnlyList<JournalEntry>> WaitForAsync(
        int count,
        TimeSpan bound,
        CancellationToken cancellationToken
    )
    {
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_gate)
        {
            _waiters.Add((count, reached));
        }

        Signal();
        await reached.Task.WaitAsync(bound, cancellationToken).ConfigureAwait(false);
        return [.. _entries];
    }

    private void Signal()
    {
        lock (_gate)
        {
            foreach (var (count, reached) in _waiters)
            {
                if (_entries.Count >= count)
                {
                    reached.TrySetResult();
                }
            }
        }
    }
}

public sealed class LedgerHandler(InvoiceJournal journal, DeliveryScope scope)
    : IEventHandler<InvoiceIssued>
{
    public ValueTask HandleAsync(InvoiceIssued @event, CancellationToken cancellationToken)
    {
        journal.Record(new JournalEntry("ledger", @event.Number, scope.Id));
        return ValueTask.CompletedTask;
    }
}

public sealed class ReminderHandler(InvoiceJournal journal, DeliveryScope scope)
    : IEventHandler<InvoiceIssued>
{
    public ValueTask HandleAsync(InvoiceIssued @event, CancellationToken cancellationToken)
    {
        journal.Record(new JournalEntry("reminder", @event.Number, scope.Id));
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// Records the frames one broker-level subscription receives, in arrival order, and lets a
/// scenario wait for a count instead of sleeping.
/// </summary>
public sealed class FrameRecorder
{
    private readonly ConcurrentQueue<string> _frames = new();
    private readonly Lock _gate = new();
    private readonly List<(int Count, TaskCompletionSource Reached)> _waiters = [];

    /// <summary>The handler to subscribe with.</summary>
    public EventFrameHandler Handler =>
        (frame, _) =>
        {
            _frames.Enqueue(Encoding.UTF8.GetString(frame.Span));
            Signal();
            return ValueTask.CompletedTask;
        };

    /// <summary>Waits until at least <paramref name="count"/> frames arrived, then returns them in arrival order.</summary>
    public async Task<IReadOnlyList<string>> WaitForAsync(
        int count,
        TimeSpan bound,
        CancellationToken cancellationToken
    )
    {
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_gate)
        {
            _waiters.Add((count, reached));
        }

        Signal();
        await reached.Task.WaitAsync(bound, cancellationToken).ConfigureAwait(false);
        return [.. _frames];
    }

    private void Signal()
    {
        lock (_gate)
        {
            foreach (var (count, reached) in _waiters)
            {
                if (_frames.Count >= count)
                {
                    reached.TrySetResult();
                }
            }
        }
    }
}
