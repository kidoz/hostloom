using HostLoom.Actors;

namespace HostLoom.Benchmarks;

/// <summary>Benchmark-only alternative for the process-local inbox. The actor owns the entire claim table.</summary>
internal sealed class ActorInboxStore : IInboxStore, IAsyncDisposable
{
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(5);
    private readonly ActorSystem _system;
    private readonly IActorRef<Command, bool> _reference;

    public ActorInboxStore(TimeProvider clock, int capacity)
    {
        _system = new(1, capacity, Budget);
        _reference = _system.Spawn<Command, bool>(() => new Claims(clock)).Ref;
    }

    private readonly record struct Command(string Key, Guid Token, TimeSpan Window, bool Release);

    private sealed class Claims(TimeProvider clock) : IActor<Command, bool>
    {
        private readonly Dictionary<string, (Guid Token, DateTimeOffset Expiry)> _claims = new(
            StringComparer.Ordinal
        );

        public ValueTask<bool> ReceiveAsync(Command command, CancellationToken stopping)
        {
            var exists = _claims.TryGetValue(command.Key, out var current);
            if (command.Release)
            {
                if (exists && current.Token == command.Token)
                    _claims.Remove(command.Key);
                return ValueTask.FromResult(true);
            }
            var now = clock.GetUtcNow();
            if (exists && current.Expiry > now)
                return ValueTask.FromResult(false);
            // Use the reference store's reclamation cadence for the comparison.
            if (_claims.Count >= 1024 && _claims.Count % 1024 == 0)
                foreach (
                    var key in _claims
                        .Where(item => item.Value.Expiry <= now)
                        .Select(item => item.Key)
                        .ToArray()
                )
                    _claims.Remove(key);
            _claims[command.Key] = (command.Token, now + command.Window);
            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<bool> TryRecordAsync(
        string key,
        Guid claimToken,
        TimeSpan window,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(window, TimeSpan.Zero);
        return _reference.AskAsync(new(key, claimToken, window, false), Budget, cancellationToken);
    }

    public async ValueTask ReleaseAsync(
        string key,
        Guid claimToken,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        await _reference
            .AskAsync(new(key, claimToken, TimeSpan.Zero, true), Budget, cancellationToken)
            .ConfigureAwait(false);
    }

    public ValueTask DisposeAsync() => _system.DisposeAsync();
}
