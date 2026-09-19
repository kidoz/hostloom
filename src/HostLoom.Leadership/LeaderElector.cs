using System.Diagnostics;
using HostLoom.Locking;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HostLoom.Leadership;

/// <summary>
/// Elects one leader for a role by holding the lock <c>leader:{role}</c>: a candidate tries a
/// skip-if-busy acquisition on a jittered cadence, a leader renews the lease itself every
/// <see cref="LeadershipOptions.RenewInterval"/>, and a renewal that fails or a lease that
/// expires steps the instance back to candidate at once, where it waits one retry interval
/// before its next attempt. Composes without a container:
/// <c>new LeaderElector("scheduler", locks, options)</c>, then <see cref="StartAsync"/>.
/// </summary>
/// <remarks>
/// The elector owns renewal instead of the lock's automatic extension, which stops at
/// <c>Locking:MaxHold</c>; a leader must renew for as long as it lives. A stopped elector releases
/// the lease so a graceful restart hands over at once; a crashed process hands over when the
/// lease expires.
/// </remarks>
public sealed class LeaderElector : ILeadership, IAsyncDisposable
{
    private static readonly CancellationToken NotLeading = new(canceled: true);

    private readonly IDistributedLock _locks;
    private readonly string _key;
    private readonly TimeProvider _clock;
    private readonly ILogger<LeaderElector> _logger;
    private readonly Lock _gate = new();
    private readonly HashSet<Action<LeadershipChange>> _listeners = [];
    private readonly CancellationTokenSource _stopping = new();
    private readonly KeyValuePair<string, object?> _roleTag;
    private TaskCompletionSource _becameLeader = NewSignal();
    private CancellationTokenSource? _leaderToken;
    private CancellationTokenSource? _resign;
    private TaskCompletionSource? _resigned;
    private Task? _loop;
    private long _term;
    private bool _providerDown;
    private bool _disposed;

    /// <summary>Composes the elector.</summary>
    /// <param name="role">The role; see <see cref="LeadershipRole.Validate"/>.</param>
    /// <param name="locks">The lock the role is elected over; its namespace prefixes the key.</param>
    /// <param name="options">Lease, renewal, and retry cadence.</param>
    /// <param name="timeProvider">The clock every wait uses; the system clock by default.</param>
    /// <param name="logger">Receives the events in <see cref="LeadershipEvents"/>.</param>
    /// <exception cref="ArgumentException">The role or the options are not acceptable.</exception>
    public LeaderElector(
        string role,
        IDistributedLock locks,
        LeadershipOptions options,
        TimeProvider? timeProvider = null,
        ILogger<LeaderElector>? logger = null
    )
    {
        ArgumentNullException.ThrowIfNull(locks);
        ArgumentNullException.ThrowIfNull(options);
        _key = LeadershipRole.KeyFor(role);
        var problems = options.Validate();
        if (problems.Count > 0)
        {
            throw new ArgumentException(
                "LeadershipOptions are not usable: " + string.Join(" ", problems),
                nameof(options)
            );
        }

        Role = role;
        Options = options;
        _locks = locks;
        _clock = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger<LeaderElector>.Instance;
        _roleTag = new KeyValuePair<string, object?>(LeadershipDiagnostics.RoleTag, role);
        LeadershipDiagnostics.Register(this);
    }

    /// <inheritdoc />
    public string Role { get; }

    /// <summary>The cadence this elector was composed with.</summary>
    public LeadershipOptions Options { get; }

    /// <summary>Where the elector stands.</summary>
    public LeadershipStatus Status
    {
        get
        {
            lock (_gate)
            {
                return field;
            }
        }
        private set
        {
            lock (_gate)
            {
                field = value;
            }
        }
    }

    /// <inheritdoc />
    public bool IsLeader => Status == LeadershipStatus.Leader;

    /// <inheritdoc />
    public long Term => Interlocked.Read(ref _term);

    /// <inheritdoc />
    public CancellationToken LeadershipToken
    {
        get
        {
            lock (_gate)
            {
                return _leaderToken?.Token ?? NotLeading;
            }
        }
    }

    /// <inheritdoc />
    public ValueTask WaitForLeadershipAsync(CancellationToken cancellationToken = default)
    {
        Task signal;
        lock (_gate)
        {
            if (Status == LeadershipStatus.Leader)
            {
                return ValueTask.CompletedTask;
            }

            signal = _becameLeader.Task;
        }

        return new ValueTask(signal.WaitAsync(cancellationToken));
    }

    /// <inheritdoc />
    public IDisposable OnChange(Action<LeadershipChange> listener)
    {
        ArgumentNullException.ThrowIfNull(listener);
        lock (_gate)
        {
            _listeners.Add(listener);
        }

        return new Subscription(this, listener);
    }

    /// <summary>Starts as a candidate on the thread pool and returns at once. Repeated calls do nothing.</summary>
    /// <exception cref="ObjectDisposedException">The elector was disposed.</exception>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_loop is not null)
            {
                return Task.CompletedTask;
            }

            Status = LeadershipStatus.Candidate;
            var token = _stopping.Token;
            _loop = Task.Run(() => RunAsync(token), CancellationToken.None);
        }

        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation(
                LeadershipEvents.Started,
                "Elector for role '{Role}' started as a candidate: lease {Lease}, renewal every {RenewInterval}, retry every {RetryInterval}.",
                Role,
                Options.Lease,
                Options.RenewInterval,
                Options.RetryInterval
            );
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Stops the elector, releasing the lease it holds so another instance can lead at once, and
    /// waits for the loop bounded by <paramref name="cancellationToken"/>.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        Task? loop;
        lock (_gate)
        {
            loop = _loop;
        }

        await _stopping.CancelAsync().ConfigureAwait(false);
        if (loop is null)
        {
            Status = LeadershipStatus.Stopped;
            return;
        }

        try
        {
            await loop.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The host gave up waiting; the loop releases the lease on its own.
        }
    }

    /// <summary>
    /// Steps down when leading: releases the lease, raises <see cref="LeadershipChangeReason.Resigned"/>,
    /// and stays a candidate that waits one retry interval before its next attempt so another
    /// instance has the first chance. Does nothing when not leading.
    /// </summary>
    public async ValueTask ResignAsync(CancellationToken cancellationToken = default)
    {
        Task resigned;
        CancellationTokenSource resign;
        lock (_gate)
        {
            if (Status != LeadershipStatus.Leader || _resign is null)
            {
                return;
            }

            _resigned ??= NewSignal();
            resigned = _resigned.Task;
            resign = _resign;
        }

        await resign.CancelAsync().ConfigureAwait(false);
        await resigned.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Stops the elector and waits for its loop without a bound.</summary>
    public async ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
        }

        await StopAsync(CancellationToken.None).ConfigureAwait(false);
        LeadershipDiagnostics.Unregister(this);
        lock (_gate)
        {
            _leaderToken?.Dispose();
            _leaderToken = null;
        }

        _stopping.Dispose();
    }

    private static TaskCompletionSource NewSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private async Task RunAsync(CancellationToken stopping)
    {
        try
        {
            while (!stopping.IsCancellationRequested)
            {
                var handle = await TryAcquireAsync(stopping).ConfigureAwait(false);
                if (handle is null)
                {
                    await DelayAsync(RetryDelay(jitter: true), stopping).ConfigureAwait(false);
                    continue;
                }

                var reason = await LeadAsync(handle, stopping).ConfigureAwait(false);
                if (reason is LeadershipChangeReason.Resigned or LeadershipChangeReason.Lost)
                {
                    // One retry interval before running again, so another instance gets the
                    // first chance and a flapping backend does not turn renewals into a hot loop.
                    await DelayAsync(
                            RetryDelay(jitter: reason == LeadershipChangeReason.Lost),
                            stopping
                        )
                        .ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested)
        {
            // Stopping while waiting.
        }
        finally
        {
            Status = LeadershipStatus.Stopped;
        }
    }

    private async Task<ILockHandle?> TryAcquireAsync(CancellationToken stopping)
    {
        using var activity = LeadershipDiagnostics.ActivitySource.StartActivity("leader.acquire");
        activity?.SetTag(LeadershipDiagnostics.RoleTag, Role);
        try
        {
            var handle = await _locks
                .TryAcquireAsync(
                    _key,
                    new LockOptions
                    {
                        Lease = Options.Lease,
                        MaxWait = TimeSpan.Zero,
                        AutoExtend = false,
                        OnLost = LostLeaseBehavior.Observe,
                    },
                    stopping
                )
                .ConfigureAwait(false);
            if (_providerDown)
            {
                _providerDown = false;
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation(
                        LeadershipEvents.ProviderRecovered,
                        "The lock provider answers again for role '{Role}'.",
                        Role
                    );
                }
            }

            activity?.SetTag("hostloom.leader.acquired", handle is not null);
            return handle;
        }
        catch (LockProviderUnavailableException exception)
        {
            activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
            if (!_providerDown)
            {
                _providerDown = true;
                _logger.LogWarning(
                    LeadershipEvents.ProviderUnavailable,
                    exception,
                    "The lock provider is unreachable for role '{Role}' ({Kind}); nobody can become leader until it answers.",
                    Role,
                    exception.Kind
                );
            }

            return null;
        }
    }

    private async Task<LeadershipChangeReason> LeadAsync(
        ILockHandle handle,
        CancellationToken stopping
    )
    {
        using var resign = new CancellationTokenSource();
        BecomeLeader(handle, resign);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            stopping,
            handle.LostToken,
            resign.Token
        );

        LeadershipChangeReason reason;
        try
        {
            while (true)
            {
                await Task.Delay(Options.RenewInterval, _clock, linked.Token).ConfigureAwait(false);
                if (!await RenewAsync(handle, linked.Token).ConfigureAwait(false))
                {
                    reason = LeadershipChangeReason.Lost;
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested)
        {
            reason = LeadershipChangeReason.Stopped;
        }
        catch (OperationCanceledException) when (resign.IsCancellationRequested)
        {
            reason = LeadershipChangeReason.Resigned;
        }
        catch (OperationCanceledException)
        {
            reason = LeadershipChangeReason.Lost;
        }

        await StepDownAsync(handle, reason).ConfigureAwait(false);
        return reason;
    }

    private async Task<bool> RenewAsync(ILockHandle handle, CancellationToken cancellationToken)
    {
        var start = _clock.GetTimestamp();
        string outcome;
        bool renewed;
        try
        {
            // A provider failure is logged by the lock and reported as false, never thrown.
            renewed = await handle
                .ExtendAsync(Options.Lease, cancellationToken)
                .ConfigureAwait(false);
            outcome = renewed ? "renewed" : "refused";
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            renewed = false;
            outcome = "failed";
        }

        LeadershipDiagnostics.RenewDuration.Record(
            _clock.GetElapsedTime(start).TotalSeconds,
            _roleTag,
            new KeyValuePair<string, object?>(LeadershipDiagnostics.OutcomeTag, outcome)
        );
        return renewed;
    }

    private void BecomeLeader(ILockHandle handle, CancellationTokenSource resign)
    {
        _ = handle;
        long term;
        TaskCompletionSource signal;
        lock (_gate)
        {
            term = Interlocked.Increment(ref _term);
            _leaderToken?.Dispose();
            _leaderToken = new CancellationTokenSource();
            _resign = resign;
            _resigned = null;
            Status = LeadershipStatus.Leader;
            signal = _becameLeader;
            _becameLeader = NewSignal();
        }

        signal.TrySetResult();
        Record(LeadershipChangeReason.Acquired, term);
        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation(
                LeadershipEvents.Acquired,
                "This instance leads role '{Role}' for term {Term}.",
                Role,
                term
            );
        }
    }

    private async Task StepDownAsync(ILockHandle handle, LeadershipChangeReason reason)
    {
        CancellationTokenSource? leaderToken;
        TaskCompletionSource? resigned;
        lock (_gate)
        {
            leaderToken = _leaderToken;
            _leaderToken = null;
            _resign = null;
            resigned = _resigned;
            _resigned = null;
            Status =
                reason == LeadershipChangeReason.Stopped
                    ? LeadershipStatus.Stopped
                    : LeadershipStatus.Candidate;
        }

        if (leaderToken is not null)
        {
            try
            {
                await leaderToken.CancelAsync().ConfigureAwait(false);
            }
            catch (AggregateException exception)
            {
                _logger.LogWarning(
                    LeadershipEvents.CancellationCallbackFailed,
                    exception,
                    "A leadership cancellation callback for role '{Role}' threw; the lease will still be released and the elector continues.",
                    Role
                );
            }
            finally
            {
                leaderToken.Dispose();
            }
        }

        // A refused release is reported by the lock as a lost lease, never thrown.
        await handle.DisposeAsync().ConfigureAwait(false);
        Record(reason, Term);
        resigned?.TrySetResult();
        var term = Term;
        if (reason == LeadershipChangeReason.Lost)
        {
            _logger.LogWarning(
                LeadershipEvents.Lost,
                "This instance lost leadership of role '{Role}' in term {Term}: a renewal was refused or the lease expired. It is a candidate again.",
                Role,
                term
            );
        }
        else if (_logger.IsEnabled(LogLevel.Information))
        {
            if (reason == LeadershipChangeReason.Resigned)
            {
                _logger.LogInformation(
                    LeadershipEvents.Resigned,
                    "This instance resigned leadership of role '{Role}' in term {Term}.",
                    Role,
                    term
                );
            }
            else
            {
                _logger.LogInformation(
                    LeadershipEvents.Stopped,
                    "The elector for role '{Role}' stopped and released the lease it held in term {Term}.",
                    Role,
                    term
                );
            }
        }
    }

    private void Record(LeadershipChangeReason reason, long term)
    {
        LeadershipDiagnostics.Changes.Add(
            1,
            _roleTag,
            new KeyValuePair<string, object?>(
                LeadershipDiagnostics.ReasonTag,
                LeadershipDiagnostics.ReasonName(reason)
            )
        );

        Action<LeadershipChange>[] listeners;
        lock (_gate)
        {
            listeners = [.. _listeners];
        }

        var change = new LeadershipChange(
            Role,
            reason == LeadershipChangeReason.Acquired,
            term,
            reason
        );
        foreach (var listener in listeners)
        {
            try
            {
                listener(change);
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    LeadershipEvents.ListenerFailed,
                    exception,
                    "A leadership listener for role '{Role}' threw; the elector continues.",
                    Role
                );
            }
        }
    }

    private TimeSpan RetryDelay(bool jitter)
    {
        if (!jitter || Options.RetryJitter == TimeSpan.Zero)
        {
            return Options.RetryInterval;
        }

        // CA5394: jitter decorrelates candidates; it does not resist an attacker.
#pragma warning disable CA5394
        return Options.RetryInterval + (Options.RetryJitter * Random.Shared.NextDouble());
#pragma warning restore CA5394
    }

    private Task DelayAsync(TimeSpan delay, CancellationToken stopping) =>
        Task.Delay(delay, _clock, stopping);

    private sealed class Subscription(LeaderElector owner, Action<LeadershipChange> listener)
        : IDisposable
    {
        public void Dispose()
        {
            lock (owner._gate)
            {
                owner._listeners.Remove(listener);
            }
        }
    }
}
