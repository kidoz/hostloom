namespace HostLoom.DataGrid.Client;

public static partial class DataGridClientHistoryChecker
{
    private static List<DataGridClientHistoryOperation>? ExpandAttempts(
        IReadOnlyList<DataGridClientHistoryOperation> history,
        HashSet<long> events,
        HashSet<Guid> ids
    )
    {
        var expanded = new List<DataGridClientHistoryOperation>();
        foreach (var operation in history)
        {
            if (
                operation.Outcome is not { } outcome
                || outcome.Reply.Failure is not { } final
                || !Semantic(final.Code)
                || outcome.Attempts.Count <= 1
            )
            {
                expanded.Add(operation);
                continue;
            }
            if (
                outcome.Attempts.Count > 3
                || !ReferenceEquals(outcome.Attempts[^1].Reply, outcome.Reply)
            )
                return null;
            var previous = operation.InvocationOrder;
            for (var i = 0; i < outcome.Attempts.Count; i++)
            {
                var attempt = outcome.Attempts[i];
                if (
                    attempt.AttemptId == Guid.Empty
                    || attempt.MemberId == Guid.Empty
                    || !ids.Add(attempt.AttemptId)
                    || attempt.InvocationOrder is not { } start
                    || attempt.CompletionOrder is not { } end
                    || start <= previous
                    || end <= start
                    || end >= operation.CompletionOrder
                    || !events.Add(start)
                    || !events.Add(end)
                    || attempt.Reply is not null && !attempt.RequestSubmitted
                )
                    return null;
                if (
                    i < outcome.Attempts.Count - 1
                    && attempt.Reply is { } observed
                    && (
                        observed.Failure is not { } failure
                        || !(
                            failure.Code
                                is DataGridClientFailureCode.Redirect
                                    or DataGridClientFailureCode.QuorumUnavailable
                                    or DataGridClientFailureCode.OutcomeUnknown
                            || failure.Code == DataGridClientFailureCode.CapacityExceeded
                                && !failure.MayHaveBeenAdmitted
                        )
                    )
                )
                    return null;
                var admitted =
                    attempt.RequestSubmitted
                    && (
                        attempt.Reply is null
                        || attempt.Reply.Failure is null
                        || attempt.Reply.Failure.MayHaveBeenAdmitted
                    );
                var reply =
                    attempt.Reply
                    ?? DataGridClientReply.Failed(
                        new(
                            admitted
                                ? DataGridClientFailureCode.OutcomeUnknown
                                : DataGridClientFailureCode.QuorumUnavailable,
                            admitted
                        )
                    );
                expanded.Add(
                    new(
                        attempt.AttemptId,
                        operation.PrincipalId,
                        start,
                        end,
                        operation.Request,
                        new(reply, admitted, [attempt])
                    )
                );
                previous = end;
            }
        }
        return expanded;
    }
}
