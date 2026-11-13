namespace HostLoom.DataGrid.Client;

public enum DataGridHistoryStatus
{
    Valid,
    Invalid,
    Inconclusive,
}

public sealed record DataGridHistoryCheck(
    DataGridHistoryStatus Status,
    int ExploredStates,
    string Reason,
    IReadOnlyList<Guid> Witness
);

/// <summary>
/// Bounded independent map, expiry and session history search. Grid time and relative deadlines
/// use symbolic difference constraints. Empty-session mode checks the complete observed session
/// lifecycle; default mode assumes pre-opened live sessions. Quotas/policies remain unsupported.
/// </summary>
public static partial class DataGridClientHistoryChecker
{
    public static DataGridHistoryCheck Check(
        IReadOnlyList<DataGridClientHistoryOperation> history,
        int maxStates = 10000,
        bool startsWithEmptySessions = false
    )
    {
        ArgumentNullException.ThrowIfNull(history);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxStates, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maxStates, 100000);
        if (history.Count > 32)
            return Inconclusive("More than 32 operations.");
        var events = new HashSet<long>();
        var ids = new HashSet<Guid>();
        long bytes = 0;
        var expandAttempts = false;
        foreach (var operation in history)
        {
            if (
                operation.OperationId == Guid.Empty
                || operation.PrincipalId == Guid.Empty
                || !ids.Add(operation.OperationId)
                || operation.InvocationOrder <= 0
                || !events.Add(operation.InvocationOrder)
                || (operation.CompletionOrder is null) != (operation.Outcome is null)
            )
                return Inconclusive("Malformed history identity/order.");
            if (
                operation.CompletionOrder is { } end
                && (end <= operation.InvocationOrder || !events.Add(end))
            )
                return Inconclusive("Malformed completion order.");
            var request = operation.Request;
            var outcome = operation.Outcome;
            if (request.Mutation is { } payload)
                for (var i = 0; i < payload.ItemCount; i++)
                    bytes += payload.GetItem(i).ValueByteCount + payload.GetItem(i).Key.ByteCount;
            if (outcome?.Reply.Failure is { } failure)
            {
                if (!outcome.MayHaveBeenAdmitted && !failure.MayHaveBeenAdmitted)
                    continue;
                if (Semantic(failure.Code))
                {
                    if (!startsWithEmptySessions)
                        return Inconclusive("Applied session failures require empty-session mode.");
                    expandAttempts |= outcome.Attempts.Count > 1;
                }
                else if (
                    failure.Code
                    is not (
                        DataGridClientFailureCode.OutcomeUnknown
                        or DataGridClientFailureCode.QuorumUnavailable
                        or DataGridClientFailureCode.ProtocolMismatch
                        or DataGridClientFailureCode.Unauthorized
                    )
                )
                    return Inconclusive("Applied capacity/map/unsupported failure.");
            }
            if (request.Kind is DataGridMessageKind.OpenSession or DataGridMessageKind.CloseSession)
            {
                if (!startsWithEmptySessions)
                    return Inconclusive("Session lifecycle requires empty-session mode.");
                if (
                    outcome is not null
                    && outcome.Reply.Failure is null
                    && request.Kind == DataGridMessageKind.OpenSession
                    && (
                        outcome.Reply.Result
                        is not { SessionNumber: > 0, SessionExpiresAt: not null }
                    )
                )
                    return Inconclusive("Unsupported session response.");
            }
            else if (request.Mutation is { } command)
            {
                var result = outcome?.Reply.Result;
                if (
                    outcome is not null
                    && outcome.Reply.Failure is null
                    && (result is null || result.ItemCount != command.ItemCount)
                )
                    return Inconclusive("Unsupported mutation result.");
                for (var i = 0; i < command.ItemCount; i++)
                    if (
                        result?.GetItem(i).Status
                        is DataGridMutationStatus.CapacityExceeded
                            or DataGridMutationStatus.InvalidRequest
                    )
                        return Inconclusive(
                            "Quota/policy/invalid-item outcomes remain unsupported."
                        );
            }
            else if (request.Kind == DataGridMessageKind.Read)
            {
                if (outcome is not null && outcome.Reply.Failure is null)
                {
                    if (outcome.Reply.Read is not { } read || read.Count != request.Keys.Count)
                        return Inconclusive("Unsupported read response.");
                    for (var i = 0; i < read.Count; i++)
                        bytes += read.GetItem(i)?.GetValue().Length ?? 0;
                }
            }
            else
                return Inconclusive("Unsupported request.");
        }
        if (expandAttempts)
        {
            var expanded = ExpandAttempts(history, events, ids);
            return expanded is null
                ? Inconclusive("Incomplete or inconsistent physical-attempt evidence.")
                : Check(expanded, maxStates, startsWithEmptySessions);
        }
        if (bytes > 2 * 1024 * 1024)
            return Inconclusive("History exceeds two MiB checker budget.");
        var budget = new Budget(maxStates);
        var path = new List<Guid>();
        var done = new bool[history.Count];
        bool Search(State state)
        {
            if (!budget.Take())
                return false;
            if (path.Count == history.Count)
                return true;
            for (var i = 0; i < history.Count && !budget.Exhausted; i++)
            {
                // An uncertain response does not finish the logical operation's server work.
                if (
                    done[i]
                    || Enumerable
                        .Range(0, history.Count)
                        .Any(j =>
                            !done[j]
                            && !Uncertain(history[j])
                            && history[j].CompletionOrder < history[i].InvocationOrder
                        )
                )
                    continue;
                done[i] = true;
                path.Add(history[i].OperationId);
                foreach (var next in Apply(state, history[i], budget))
                    if (Search(next))
                        return true;
                path.RemoveAt(path.Count - 1);
                done[i] = false;
            }
            return false;
        }
        var valid = Search(new(startsWithEmptySessions));
        return new(
            valid ? DataGridHistoryStatus.Valid
                : budget.Exhausted ? DataGridHistoryStatus.Inconclusive
                : DataGridHistoryStatus.Invalid,
            budget.Used,
            valid ? "Found a consistent symbolic history (uncertain operations may be omitted)."
                : budget.Exhausted ? "Search budget exhausted."
                : "No permitted sequential history.",
            path.AsReadOnly()
        );
    }

    private static bool Semantic(DataGridClientFailureCode code) =>
        code
            is DataGridClientFailureCode.SessionExpired
                or DataGridClientFailureCode.RequestRetired
                or DataGridClientFailureCode.RequestIdentityMismatch
                or DataGridClientFailureCode.InvalidRequest;

    private static bool Uncertain(DataGridClientHistoryOperation operation) =>
        operation.Outcome is null
        || operation.Outcome.Reply.Failure is { } failure
            && !Semantic(failure.Code)
            && (operation.Outcome.MayHaveBeenAdmitted || failure.MayHaveBeenAdmitted);

    private static IEnumerable<State> Apply(
        State state,
        DataGridClientHistoryOperation operation,
        Budget budget
    )
    {
        var uncertain = Uncertain(operation);
        if (uncertain)
            yield return state;
        else if (
            operation.Outcome?.Reply.Failure is { } failure
            && !operation.Outcome.MayHaveBeenAdmitted
            && !failure.MayHaveBeenAdmitted
        )
        {
            yield return state;
            yield break;
        }
        if (!budget.Take())
            yield break;
        var next = state.Copy();
        next.Time = next.Clock.Advance(next.Time);
        foreach (var result in ApplyAt(next, operation, budget))
            yield return result;
        // Elections may commit TimeAdvance without a client read. Only session retirement
        // adds an effect beyond an operation's own monotonic time advance. Keep its time
        // separate: a hidden barrier can retire an earlier cutoff but retain a later one.
        if (state.Strict && state.Sessions.Count > 0)
        {
            var barrier = state.Copy();
            barrier.Time = barrier.Clock.Advance(barrier.Time);
            foreach (var retired in Retire(barrier, budget))
            {
                if (retired.Sessions.Count == state.Sessions.Count)
                    continue;
                retired.Time = retired.Clock.Advance(retired.Time);
                foreach (var result in ApplyAt(retired, operation, budget))
                    yield return result;
            }
        }
    }

    private static IEnumerable<State> ApplyAt(
        State next,
        DataGridClientHistoryOperation operation,
        Budget budget
    )
    {
        var uncertain = Uncertain(operation);
        switch (operation.Request.Kind)
        {
            case DataGridMessageKind.OpenSession:
                foreach (var result in Open(next, operation))
                    yield return result;
                break;
            case DataGridMessageKind.CloseSession:
                foreach (var result in Close(next, operation, budget))
                    yield return result;
                break;
            case DataGridMessageKind.Read:
                if (uncertain)
                {
                    // A lost read reply may still hide a committed session-retirement barrier.
                    foreach (var retired in Retire(next, budget))
                        yield return retired;
                    yield break;
                }
                if (operation.Outcome!.Reply.Failure is not null)
                    yield break;
                var read = operation.Outcome.Reply.Read!;
                if (!next.Clock.Equal(next.Now, Moment.At(read.CapturedAt.UtcTicks)))
                    yield break;
                foreach (var retired in Retire(next, budget))
                foreach (var result in ReadItems(retired, operation, 0, budget))
                    yield return result;
                break;
            default:
                foreach (var result in Mutate(next, operation, budget))
                    yield return result;
                break;
        }
    }

    private static IEnumerable<State> ReadItems(
        State state,
        DataGridClientHistoryOperation operation,
        int ordinal,
        Budget budget
    )
    {
        if (!budget.Take())
            yield break;
        var request = operation.Request;
        var read = operation.Outcome!.Reply.Read!;
        if (ordinal == request.Keys.Count)
        {
            yield return state;
            yield break;
        }
        foreach (var (next, entry) in Live(state, (request.MapId, request.Keys[ordinal])))
        {
            var actual = read.GetItem(ordinal);
            if ((entry is null) != (actual is null))
                continue;
            if (entry is not null)
            {
                if (
                    !actual!.GetValue().AsSpan().SequenceEqual(entry.Value)
                    || !next.Bind(entry.Stamp, entry.Ordinal, actual.Version)
                )
                    continue;
                if (entry.Deadline is { } deadline)
                {
                    if (
                        actual.RemainingTimeToLive is not { } remaining
                        || remaining.Ticks <= 0
                        || !next.Clock.Equal(deadline, new(next.Time, remaining.Ticks))
                    )
                        continue;
                }
                else if (actual.RemainingTimeToLive is not null)
                    continue;
            }
            foreach (var result in ReadItems(next, operation, ordinal + 1, budget))
                yield return result;
        }
    }

    private static IEnumerable<(State State, Entry? Entry)> Live(
        State state,
        (Guid Map, DataGridKey Key) key
    )
    {
        if (!state.Entries.TryGetValue(key, out var entry))
        {
            yield return (state, null);
            yield break;
        }
        if (entry.Deadline is not { } deadline)
        {
            yield return (state, entry);
            yield break;
        }
        var live = state.Copy();
        if (live.Clock.Before(live.Now, deadline, strict: true))
            yield return (live, entry);
        var expired = state.Copy();
        if (expired.Clock.Before(deadline, expired.Now))
            yield return (expired, null);
    }

    private static IEnumerable<State> Mutate(
        State state,
        DataGridClientHistoryOperation operation,
        Budget budget
    )
    {
        var command = operation.Request.Mutation!;
        foreach (var (active, session) in ActiveSession(state, operation))
        {
            if (session is null)
            {
                if (Failure(operation, DataGridClientFailureCode.SessionExpired))
                    yield return active;
                continue;
            }
            var digest = command.ComputeDigest();
            if (command.RequestNumber == session.Number)
            {
                if (!digest.AsSpan().SequenceEqual(session.Digest))
                {
                    if (Failure(operation, DataGridClientFailureCode.RequestIdentityMismatch))
                        yield return active;
                }
                else if (Matches(active, session.Items, operation))
                    yield return active;
                continue;
            }
            if (command.RequestNumber < session.Number)
            {
                if (Failure(operation, DataGridClientFailureCode.RequestRetired))
                    yield return active;
                continue;
            }
            if (session.Number == ulong.MaxValue || command.RequestNumber != session.Number + 1)
            {
                if (Failure(operation, DataGridClientFailureCode.InvalidRequest))
                    yield return active;
                continue;
            }
            if (!Uncertain(operation) && operation.Outcome!.Reply.Failure is not null)
                continue;
            Moment? deadline = command.ExpiryKind switch
            {
                DataGridExpiryKind.Absolute => Moment.At(command.ExpiryTicks),
                DataGridExpiryKind.Relative => new(active.Time, command.ExpiryTicks),
                _ => null,
            };
            if (command.ExpiryKind == DataGridExpiryKind.Relative)
            {
                var overflow = active.Copy();
                if (
                    Uncertain(operation)
                    && overflow.Clock.Before(
                        Moment.At(DateTimeOffset.MaxValue.Ticks),
                        deadline!.Value,
                        strict: true
                    )
                )
                {
                    overflow.Sessions[command.SessionNumber] = session with
                    {
                        Number = command.RequestNumber,
                        Digest = digest,
                        Items = Enumerable
                            .Range(0, command.ItemCount)
                            .Select(i => new Item(DataGridMutationStatus.InvalidRequest, -1, i))
                            .ToArray(),
                    };
                    yield return overflow;
                }
                if (!active.Clock.Before(deadline!.Value, Moment.At(DateTimeOffset.MaxValue.Ticks)))
                    continue;
            }
            var observed = Uncertain(operation) ? null : operation.Outcome!.Reply.Result;
            var items = new Item[command.ItemCount];
            IEnumerable<State> Items(State current, int ordinal, int stamp)
            {
                if (!budget.Take())
                    yield break;
                if (ordinal == command.ItemCount)
                {
                    current.Sessions[command.SessionNumber] = session with
                    {
                        Number = command.RequestNumber,
                        Digest = digest,
                        Items = (Item[])items.Clone(),
                    };
                    yield return current;
                    yield break;
                }
                var item = command.GetItem(ordinal);
                var key = (command.MapId, item.Key);
                foreach (var (live, old) in Live(current, key))
                foreach (
                    var equal in command.Kind
                        is DataGridMutationKind.CompareExchange
                            or DataGridMutationKind.CompareRemove
                    && old is not null
                        ? new[] { true, false }
                        : new[] { true }
                )
                {
                    if (budget.Exhausted)
                        yield break;
                    var next = live.Copy();
                    var conditional =
                        command.Kind
                        is DataGridMutationKind.CompareExchange
                            or DataGridMutationKind.CompareRemove;
                    if (
                        conditional
                        && old is not null
                        && !(
                            equal
                                ? next.Bind(old.Stamp, old.Ordinal, item.ExpectedVersion)
                                : next.Exclude(old.Stamp, old.Ordinal, item.ExpectedVersion)
                        )
                    )
                        continue;
                    var status =
                        conditional && old is null ? DataGridMutationStatus.Missing
                        : conditional && !equal ? DataGridMutationStatus.VersionMismatch
                        : command.Kind == DataGridMutationKind.PutIfAbsent && old is not null
                            ? DataGridMutationStatus.AlreadyExists
                        : command.Kind
                            is DataGridMutationKind.Remove
                                or DataGridMutationKind.CompareRemove
                            ? old is null ? DataGridMutationStatus.Missing
                                : DataGridMutationStatus.Removed
                        : DataGridMutationStatus.Stored;
                    if (observed is not null && observed.GetItem(ordinal).Status != status)
                        continue;
                    var position = stamp;
                    if (status == DataGridMutationStatus.Stored)
                    {
                        if (position < 0)
                        {
                            position = next.Stamps.Count;
                            next.Stamps.Add(new(null, []));
                            if (!next.Feasible())
                                continue;
                        }
                        if (
                            observed is not null
                            && !next.Bind(position, ordinal, observed.GetItem(ordinal).Version)
                        )
                            continue;
                        next.Entries[key] = new(item.GetValue(), position, ordinal, deadline);
                    }
                    else if (status == DataGridMutationStatus.Removed)
                        next.Entries.Remove(key);
                    items[ordinal] = new(
                        status,
                        status == DataGridMutationStatus.Stored ? position : -1,
                        ordinal
                    );
                    foreach (var result in Items(next, ordinal + 1, position))
                        yield return result;
                }
            }
            foreach (var result in Items(active, 0, -1))
                yield return result;
        }
    }

    private static bool Matches(State state, Item[] items, DataGridClientHistoryOperation operation)
    {
        if (Uncertain(operation))
            return true;
        if (
            operation.Outcome!.Reply.Failure is not null
            || operation.Outcome.Reply.Result is not { } observed
            || observed.ItemCount != items.Length
        )
            return false;
        for (var i = 0; i < items.Length; i++)
            if (
                observed.GetItem(i).Status != items[i].Status
                || items[i].Status == DataGridMutationStatus.Stored
                    && !state.Bind(items[i].Stamp, items[i].Ordinal, observed.GetItem(i).Version)
            )
                return false;
        return true;
    }

    private static bool Failure(
        DataGridClientHistoryOperation operation,
        DataGridClientFailureCode code
    ) => Uncertain(operation) || operation.Outcome?.Reply.Failure?.Code == code;

    private static DataGridHistoryCheck Inconclusive(string reason) =>
        new(DataGridHistoryStatus.Inconclusive, 0, reason, Array.Empty<Guid>());

    private sealed record Entry(byte[] Value, int Stamp, int Ordinal, Moment? Deadline);

    private sealed record Item(DataGridMutationStatus Status, int Stamp, int Ordinal);

    private sealed record Session(
        Guid Principal,
        Guid Nonce,
        long NotAfter,
        Moment? Expires,
        ulong Number,
        byte[] Digest,
        Item[] Items,
        bool Closed = false
    );

    private sealed record Stamp(long? Index, HashSet<long> Excluded);

    private sealed class Budget(int maximum)
    {
        internal int Used { get; private set; }
        internal bool Exhausted { get; private set; }

        internal bool Take()
        {
            if (Used == maximum)
            {
                Exhausted = true;
                return false;
            }
            Used++;
            return true;
        }
    }

    private sealed class State(bool strict = false)
    {
        internal bool Strict { get; } = strict;
        internal Dictionary<(Guid Map, DataGridKey Key), Entry> Entries { get; private init; } = [];
        internal Dictionary<long, Session> Sessions { get; private init; } = [];
        internal List<Stamp> Stamps { get; private init; } = [];
        internal GridClock Clock { get; private init; } = new();
        internal Guid Incarnation { get; private set; }
        internal int Time { get; set; }
        internal Moment Now => new(Time);
        internal long HighWater { get; set; }

        internal State Copy() =>
            new(Strict)
            {
                Entries = new(Entries),
                Sessions = new(Sessions),
                Stamps = new(Stamps),
                Clock = Clock.Copy(),
                Incarnation = Incarnation,
                Time = Time,
                HighWater = HighWater,
            };

        internal bool Bind(int stamp, int ordinal, DataGridEntryVersion actual)
        {
            if (
                actual.IsEmpty
                || actual.ItemIndex != ordinal
                || Incarnation != Guid.Empty && actual.IncarnationId != Incarnation
            )
                return false;
            Incarnation = actual.IncarnationId;
            var old = Stamps[stamp];
            if (
                old.Index is { } index && index != actual.LogIndex
                || old.Excluded.Contains(actual.LogIndex)
            )
                return false;
            Stamps[stamp] = old with { Index = actual.LogIndex };
            return Feasible();
        }

        internal bool Exclude(int stamp, int ordinal, DataGridEntryVersion actual)
        {
            if (Incarnation == Guid.Empty)
                Incarnation = actual.IncarnationId;
            if (actual.ItemIndex != ordinal || actual.IncarnationId != Incarnation)
                return true;
            var old = Stamps[stamp];
            if (old.Index == actual.LogIndex)
                return false;
            Stamps[stamp] = old with { Excluded = new(old.Excluded) { actual.LogIndex } };
            return Feasible();
        }

        internal bool Feasible()
        {
            long previous = 0;
            foreach (var stamp in Stamps)
            {
                if (previous == long.MaxValue)
                    return false;
                var candidate = stamp.Index ?? previous + 1;
                if (candidate <= previous)
                    return false;
                while (stamp.Excluded.Contains(candidate))
                {
                    if (stamp.Index is not null || candidate == long.MaxValue)
                        return false;
                    candidate++;
                }
                previous = candidate;
            }
            return true;
        }
    }
}
