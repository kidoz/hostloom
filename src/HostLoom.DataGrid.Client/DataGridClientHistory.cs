using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("HostLoom.Tests")]

namespace HostLoom.DataGrid.Client;

public sealed record DataGridClientAttempt(Guid AttemptId, Guid MemberId, bool RequestSubmitted)
{
    /// <summary>Validated wire reply, or null when no reply was observed. Local synthetic failures are excluded.</summary>
    public DataGridClientReply? Reply { get; init; }

    /// <summary>Shared recorder event order; null when no history recorder was supplied.</summary>
    public long? InvocationOrder { get; init; }
    public long? CompletionOrder { get; init; }
}

public sealed record DataGridClientHistoryOperation(
    Guid OperationId,
    Guid PrincipalId,
    long InvocationOrder,
    long? CompletionOrder,
    DataGridClientRequest Request,
    DataGridClientOutcome? Outcome
);

/// <summary>Optional bounded payload-bearing experiment history. It is not automatic telemetry.</summary>
public sealed class DataGridClientHistory
{
    private readonly object _sync = new();
    private readonly Dictionary<Guid, DataGridClientHistoryOperation> _operations = [];
    private long _order,
        _bytes;

    public IReadOnlyList<DataGridClientHistoryOperation> GetOperations()
    {
        lock (_sync)
            return _operations.Values.OrderBy(operation => operation.InvocationOrder).ToArray();
    }

    internal long AttemptEvent()
    {
        lock (_sync)
            return ++_order;
    }

    internal Guid Begin(Guid incarnation, Guid principal, DataGridClientRequest request)
    {
        var bytes = DataGridClientCodec
            .EncodeRequest(request, incarnation, principal, Guid.NewGuid())
            .Encode()
            .Length;
        lock (_sync)
        {
            if (_operations.Count >= 8192 || _bytes + bytes > 64L * 1024 * 1024)
                throw new InvalidOperationException(
                    "History capacity exceeded; experiment is inconclusive."
                );
            _bytes += bytes;
            var id = Guid.NewGuid();
            _operations.Add(id, new(id, principal, ++_order, null, request, null));
            return id;
        }
    }

    internal void Complete(Guid id, DataGridClientOutcome outcome)
    {
        lock (_sync)
        {
            var operation = _operations[id];
            var size = outcome.Reply.Read is { } read
                ? Enumerable
                    .Range(0, read.Count)
                    .Sum(i => (long)(read.GetItem(i)?.GetValue().Length ?? 0) + 42)
                : 4096;
            if (_bytes + size > 64L * 1024 * 1024)
                throw new InvalidOperationException(
                    "History response capacity exceeded; experiment is inconclusive."
                );
            size += outcome.Attempts.Count * 128L;
            if (_bytes + size > 64L * 1024 * 1024)
                throw new InvalidOperationException(
                    "History attempt capacity exceeded; experiment is inconclusive."
                );
            _bytes += size;
            _operations[id] = operation with { CompletionOrder = ++_order, Outcome = outcome };
        }
    }
}

public sealed class DataGridClientOutcome
{
    internal DataGridClientOutcome(
        DataGridClientReply reply,
        bool mayHaveBeenAdmitted,
        IEnumerable<DataGridClientAttempt> attempts
    )
    {
        Reply = reply;
        MayHaveBeenAdmitted = mayHaveBeenAdmitted;
        Attempts = Array.AsReadOnly(attempts.ToArray());
    }

    public DataGridClientReply Reply { get; }

    /// <summary>Cumulative across attempts. A later pre-admission failure does not roll back earlier attempts.</summary>
    public bool MayHaveBeenAdmitted { get; }
    public IReadOnlyList<DataGridClientAttempt> Attempts { get; }
}
