namespace HostLoom.DataGrid.Client;

public static partial class DataGridClientHistoryChecker
{
    private static IEnumerable<(State State, Session? Session)> ActiveSession(
        State state,
        DataGridClientHistoryOperation operation
    )
    {
        var number = operation.Request.SessionNumber;
        if (!state.Sessions.TryGetValue(number, out var session) && !state.Strict)
        {
            session = new(operation.PrincipalId, Guid.Empty, 0, null, 0, [], []);
            state.Sessions.Add(number, session);
        }
        if (session is null || session.Principal != operation.PrincipalId || session.Closed)
        {
            yield return (state, null);
            yield break;
        }
        if (session.Expires is not { } expires)
        {
            yield return (state, session);
            yield break;
        }
        var live = state.Copy();
        if (live.Clock.Before(live.Now, expires, strict: true))
            yield return (live, session);
        var expired = state.Copy();
        if (expired.Clock.Before(expires, expired.Now))
            yield return (expired, null);
    }

    private static IEnumerable<State> Close(
        State state,
        DataGridClientHistoryOperation operation,
        Budget budget
    )
    {
        foreach (var (active, session) in ActiveSession(state, operation))
        {
            if (!budget.Take())
                yield break;
            if (session is null)
            {
                if (Failure(operation, DataGridClientFailureCode.SessionExpired))
                    yield return active;
                continue;
            }
            if (
                session.Number == ulong.MaxValue
                || operation.Request.RequestNumber != session.Number + 1
            )
            {
                if (Failure(operation, DataGridClientFailureCode.InvalidRequest))
                    yield return active;
                continue;
            }
            if (
                !Uncertain(operation)
                && operation.Outcome!.Reply.Kind != DataGridMessageKind.CloseSessionResult
            )
                continue;
            active.Sessions[operation.Request.SessionNumber] = session with
            {
                Number = operation.Request.RequestNumber,
                Digest = [],
                Items = [],
                Closed = true,
            };
            yield return active;
        }
    }

    private static IEnumerable<State> Open(State state, DataGridClientHistoryOperation operation)
    {
        var request = operation.Request;
        var existing = state.Sessions.FirstOrDefault(pair =>
            pair.Value.Principal == operation.PrincipalId && pair.Value.Nonce == request.OpenNonce
        );
        if (existing.Value is { } old && old.NotAfter != request.OpenNotAfter.UtcTicks)
        {
            if (Failure(operation, DataGridClientFailureCode.RequestIdentityMismatch))
                yield return state;
            yield break;
        }
        var late = state.Copy();
        if (
            Failure(operation, DataGridClientFailureCode.SessionExpired)
            && late.Clock.Before(Moment.At(request.OpenNotAfter.UtcTicks), late.Now)
        )
            yield return late;
        var early = state.Copy();
        if (
            Failure(operation, DataGridClientFailureCode.InvalidRequest)
            && early.Clock.Before(
                new(early.Time, TimeSpan.TicksPerSecond * 5),
                Moment.At(request.OpenNotAfter.UtcTicks),
                strict: true
            )
        )
            yield return early;
        if (
            !state.Clock.Before(state.Now, Moment.At(request.OpenNotAfter.UtcTicks), strict: true)
            || !state.Clock.Before(
                Moment.At(request.OpenNotAfter.UtcTicks),
                new(state.Time, TimeSpan.TicksPerSecond * 5)
            )
        )
            yield break;
        if (existing.Value is { } session)
        {
            if (session.Closed)
            {
                if (Failure(operation, DataGridClientFailureCode.SessionExpired))
                    yield return state;
                yield break;
            }
            var expired = state.Copy();
            if (
                Failure(operation, DataGridClientFailureCode.SessionExpired)
                && expired.Clock.Before(session.Expires!.Value, expired.Now)
            )
                yield return expired;
            if (
                state.Clock.Before(state.Now, session.Expires!.Value, strict: true)
                && OpenMatches(state, operation, existing.Key, session.Expires.Value)
            )
                yield return state;
            yield break;
        }
        var expires = new Moment(state.Time, TimeSpan.TicksPerMinute * 10);
        var overflow = state.Copy();
        if (
            Failure(operation, DataGridClientFailureCode.InvalidRequest)
            && overflow.Clock.Before(
                Moment.At(DateTimeOffset.MaxValue.Ticks),
                expires,
                strict: true
            )
        )
            yield return overflow;
        if (
            state.HighWater == long.MaxValue
            || !state.Clock.Before(expires, Moment.At(DateTimeOffset.MaxValue.Ticks))
        )
            yield break;
        var number = ++state.HighWater;
        if (!OpenMatches(state, operation, number, expires))
            yield break;
        state.Sessions.Add(
            number,
            new(
                operation.PrincipalId,
                request.OpenNonce,
                request.OpenNotAfter.UtcTicks,
                expires,
                0,
                [],
                []
            )
        );
        yield return state;
    }

    private static bool OpenMatches(
        State state,
        DataGridClientHistoryOperation operation,
        long number,
        Moment expires
    )
    {
        if (Uncertain(operation))
            return true;
        return operation.Outcome!.Reply.Kind == DataGridMessageKind.OpenSessionResult
            && operation.Outcome.Reply.Result is { } result
            && result.SessionNumber == number
            && state.Clock.Equal(expires, Moment.At(result.SessionExpiresAt!.Value.UtcTicks));
    }

    private static IEnumerable<State> Retire(State state, Budget budget)
    {
        var sessions = state.Sessions.OrderBy(pair => pair.Key).ToArray();
        IEnumerable<State> Next(State current, int ordinal)
        {
            if (!budget.Take())
                yield break;
            if (ordinal == sessions.Length)
            {
                yield return current;
                yield break;
            }
            var (number, session) = sessions[ordinal];
            var cutoff = session.Closed ? Moment.At(session.NotAfter) : session.Expires;
            if (cutoff is null)
            {
                foreach (var result in Next(current, ordinal + 1))
                    yield return result;
                yield break;
            }
            var expired = current.Copy();
            if (expired.Clock.Before(cutoff.Value, expired.Now))
            {
                expired.Sessions.Remove(number);
                foreach (var result in Next(expired, ordinal + 1))
                    yield return result;
            }
            if (current.Clock.Before(current.Now, cutoff.Value, strict: true))
                foreach (var result in Next(current, ordinal + 1))
                    yield return result;
        }
        return Next(state, 0);
    }
}
