using HostLoom.Locking;
using Microsoft.Extensions.Logging;

namespace HostLoom.Tests;

/// <summary>Which provider call a <see cref="FaultingLockProvider"/> fails.</summary>
internal enum LockOperation
{
    Acquire,
    Release,
    Extend,
}

/// <summary>
/// Decorates a provider so the next <c>count</c> calls of one operation throw, which is how the
/// failure matrix is exercised without a real backend.
/// </summary>
internal sealed class FaultingLockProvider(
    ILockProvider inner,
    LockOperation operation,
    LockFailureKind kind,
    int count,
    Exception? custom = null
) : ILockProvider
{
    private int _remaining = count;

    public int Faulted { get; private set; }

    public ValueTask<bool> TryAcquireAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        Maybe(LockOperation.Acquire);
        return inner.TryAcquireAsync(key, owner, lease, cancellationToken);
    }

    public ValueTask<bool> ReleaseAsync(
        string key,
        string owner,
        CancellationToken cancellationToken = default
    )
    {
        Maybe(LockOperation.Release);
        return inner.ReleaseAsync(key, owner, cancellationToken);
    }

    public ValueTask<bool> ExtendAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        Maybe(LockOperation.Extend);
        return inner.ExtendAsync(key, owner, lease, cancellationToken);
    }

    private void Maybe(LockOperation current)
    {
        if (current != operation || _remaining <= 0)
        {
            return;
        }

        _remaining--;
        Faulted++;
        throw custom ?? new LockProviderException(kind, $"Injected {kind} on {operation}.");
    }
}

/// <summary>
/// A provider whose acquire never answers, so a test can show what bounds the wait. Release and
/// extend answer normally: nothing is ever held.
/// </summary>
internal sealed class HangingLockProvider : ILockProvider
{
    public async ValueTask<bool> TryAcquireAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        return false;
    }

    public ValueTask<bool> ReleaseAsync(
        string key,
        string owner,
        CancellationToken cancellationToken = default
    ) => ValueTask.FromResult(false);

    public ValueTask<bool> ExtendAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    ) => ValueTask.FromResult(false);
}

/// <summary>
/// Answers acquire and extend <c>latency</c> after the inner provider decided them, which is what
/// a backend round trip does: the lease starts when the request is accepted, not when the answer
/// arrives. Release is left alone so disposal does not move the clock.
/// </summary>
internal sealed class SlowLockProvider(ILockProvider inner, TestClock clock, TimeSpan latency)
    : ILockProvider
{
    public async ValueTask<bool> TryAcquireAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        var acquired = await inner
            .TryAcquireAsync(key, owner, lease, cancellationToken)
            .ConfigureAwait(false);
        clock.Advance(latency);
        return acquired;
    }

    public ValueTask<bool> ReleaseAsync(
        string key,
        string owner,
        CancellationToken cancellationToken = default
    ) => inner.ReleaseAsync(key, owner, cancellationToken);

    public async ValueTask<bool> ExtendAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        var extended = await inner
            .ExtendAsync(key, owner, lease, cancellationToken)
            .ConfigureAwait(false);
        clock.Advance(latency);
        return extended;
    }
}

/// <summary>An in-memory provider that also reports health, for the readiness tests.</summary>
internal sealed class ProbingLockProvider(TimeProvider clock)
    : ILockProvider,
        ILockProviderHealthProbe
{
    private readonly InMemoryLockProvider _inner = new(clock);

    public LockProviderHealth Health { get; set; } = LockProviderHealth.Healthy();

    public ValueTask<LockProviderHealth> CheckHealthAsync(CancellationToken cancellationToken) =>
        ValueTask.FromResult(Health);

    public ValueTask<bool> TryAcquireAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    ) => _inner.TryAcquireAsync(key, owner, lease, cancellationToken);

    public ValueTask<bool> ReleaseAsync(
        string key,
        string owner,
        CancellationToken cancellationToken = default
    ) => _inner.ReleaseAsync(key, owner, cancellationToken);

    public ValueTask<bool> ExtendAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    ) => _inner.ExtendAsync(key, owner, lease, cancellationToken);
}

/// <summary>Captures every log line so a test asserts on events rather than on output.</summary>
internal sealed class RecordingLogger<T> : ILogger<T>
{
    private readonly Lock _gate = new();
    private readonly List<(
        LogLevel Level,
        EventId Event,
        string Message,
        Exception? Exception
    )> _entries = [];

    public IReadOnlyList<(
        LogLevel Level,
        EventId Event,
        string Message,
        Exception? Exception
    )> Entries
    {
        get
        {
            lock (_gate)
            {
                return [.. _entries];
            }
        }
    }

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter
    )
    {
        lock (_gate)
        {
            _entries.Add((logLevel, eventId, formatter(state, exception), exception));
        }
    }

    public bool Has(EventId eventId) => Entries.Any(entry => entry.Event.Id == eventId.Id);
}
