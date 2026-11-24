using HostLoom.Actors;

namespace HostLoom.Tests;

// Spawn one probe per actor. Its first turn stays inside the real mailbox handler until released.
internal sealed class ActorTestProbe<TMessage, TReply>(
    int entryLimit,
    TimeSpan waitBound,
    Func<TMessage, TReply> reply
) : IActor<TMessage, TReply>
{
    private readonly Lock _gate = new();
    private readonly List<TMessage> _entries = [];
    private readonly TaskCompletionSource _entered = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private readonly TaskCompletionSource _release = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    internal TMessage[] Entries
    {
        get
        {
            lock (_gate)
                return [.. _entries];
        }
    }

    internal Task WaitForFirstEntryAsync(CancellationToken cancellationToken) =>
        _entered.Task.WaitAsync(waitBound, cancellationToken);

    internal void Release() => _release.TrySetResult();

    public async ValueTask<TReply> ReceiveAsync(TMessage message, CancellationToken stopping)
    {
        bool first;
        lock (_gate)
        {
            if (_entries.Count >= entryLimit)
                throw new InvalidOperationException(
                    "The actor probe exceeded its observation limit."
                );
            _entries.Add(message);
            first = _entries.Count == 1;
        }
        if (first)
        {
            _entered.SetResult();
            await _release.Task.WaitAsync(waitBound, stopping);
        }
        return reply(message);
    }
}
