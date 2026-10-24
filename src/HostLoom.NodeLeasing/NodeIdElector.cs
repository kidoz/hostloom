using System.Diagnostics;
using System.Globalization;
using System.Runtime.ExceptionServices;
using HostLoom.Locking;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HostLoom.NodeLeasing;

/// <summary>
/// Leases one node ID out of a pool by holding the lock <c>nodeid:{pool}:{id}</c>: a candidate
/// scans the pool on a jittered cadence — the ID it last held first, for stickiness, then
/// ascending from a random offset — and takes the first ID a skip-if-busy acquisition grants; a
/// holder renews the lease itself every <see cref="NodeLeasingOptions.RenewInterval"/>, and a
/// renewal the backend refuses or a lease that runs out steps the instance back to candidate at
/// once, where it waits one retry interval before its next scan. A renewal that fails at the
/// provider while the lease still runs does not end the assignment: the holder retries, at the
/// renewal cadence or at half the remaining lease when that is sooner, until an extension
/// succeeds or the lease ends. Composes without a container:
/// <c>new NodeIdElector("orders-workers", locks, options)</c>, then <see cref="StartAsync"/>.
/// </summary>
/// <remarks>
/// The elector owns renewal instead of the lock's automatic extension, which stops at
/// <c>Locking:MaxHold</c>; a holder must renew for as long as it lives. A stopped elector
/// releases the lease so a graceful restart hands the ID over at once; a crashed process hands
/// over when the lease expires. Over a lock that does not coordinate
/// (<c>Locking:Enabled = false</c>) a lease proves nothing, so the elector follows
/// <see cref="NodeLeasingOptions.WhenUncoordinated"/>, warns once per pool, and reports
/// <see cref="IsCoordinated"/> as <see langword="false"/>. The loop never stops on an unexpected
/// exception: it logs, backs off one retry interval, and continues.
/// </remarks>
public sealed class NodeIdElector : INodeLeasing, IAsyncDisposable
{
    private static readonly CancellationToken Unassigned = new(canceled: true);

    private readonly IDistributedLock _locks;
    private readonly string _keyPrefix;
    private readonly TimeProvider _clock;
    private readonly ILogger<NodeIdElector> _logger;
    private readonly Lock _gate = new();
    private readonly HashSet<Action<NodeLeaseChange>> _listeners = [];
    private readonly CancellationTokenSource _stopping = new();
    private readonly KeyValuePair<string, object?> _poolTag;
    private TaskCompletionSource<int> _becameAssigned = NewSignal();
    private CancellationTokenSource? _assignmentToken;
    private CancellationTokenSource? _release;
    private TaskCompletionSource? _released;
    private Task? _loop;
    private int? _nodeId;
    private int? _lastHeldId;
    private long _term;
    private bool _providerDown;
    private bool _disposed;
    private volatile bool _uncoordinated;
    private volatile bool _uncoordinatedWarned;

    /// <summary>Composes the elector.</summary>
    /// <param name="pool">The pool; see <see cref="NodeLeasingPool.Validate"/>.</param>
    /// <param name="locks">The lock the IDs are leased over; its namespace prefixes the keys.</param>
    /// <param name="options">Pool size, lease, renewal, and retry cadence.</param>
    /// <param name="timeProvider">The clock every wait uses; the system clock by default.</param>
    /// <param name="logger">Receives the events in <see cref="NodeLeasingEvents"/>.</param>
    /// <exception cref="ArgumentException">The pool or the options are not acceptable.</exception>
    public NodeIdElector(
        string pool,
        IDistributedLock locks,
        NodeLeasingOptions options,
        TimeProvider? timeProvider = null,
        ILogger<NodeIdElector>? logger = null
    )
    {
        NodeLeasingPool.Validate(pool);
        ArgumentNullException.ThrowIfNull(locks);
        ArgumentNullException.ThrowIfNull(options);
        var problems = options.Validate();
        if (problems.Count > 0)
        {
            throw new ArgumentException(
                "NodeLeasingOptions are not usable: " + string.Join(" ", problems),
                nameof(options)
            );
        }

        Pool = pool;
        Options = options;
        _keyPrefix = NodeLeasingPool.KeyPrefix + pool + ":";
        _locks = locks;
        _clock = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger<NodeIdElector>.Instance;
        _poolTag = new KeyValuePair<string, object?>(NodeLeasingDiagnostics.PoolTag, pool);
        NodeLeasingDiagnostics.Register(this);
    }

    /// <inheritdoc />
    public string Pool { get; }

    /// <summary>The cadence this elector was composed with.</summary>
    public NodeLeasingOptions Options { get; }

    /// <summary>Where the elector stands.</summary>
    public NodeLeasingStatus Status
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
    public int? NodeId
    {
        get
        {
            lock (_gate)
            {
                return _nodeId;
            }
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// <see langword="false"/> when the lock reports that it does not coordinate, or once it has
    /// handed this elector a placeholder lease; the elector then follows
    /// <see cref="NodeLeasingOptions.WhenUncoordinated"/>.
    /// </remarks>
    public bool IsCoordinated => !_uncoordinated && _locks.IsCoordinated;

    /// <inheritdoc />
    public long Term => Interlocked.Read(ref _term);

    /// <inheritdoc />
    public CancellationToken AssignmentToken
    {
        get
        {
            lock (_gate)
            {
                return _assignmentToken?.Token ?? Unassigned;
            }
        }
    }

    /// <inheritdoc />
    public ValueTask<int> WaitForAssignmentAsync(CancellationToken cancellationToken = default)
    {
        Task<int> signal;
        lock (_gate)
        {
            if (_nodeId is { } id)
            {
                return new ValueTask<int>(id);
            }

            signal = _becameAssigned.Task;
        }

        return new ValueTask<int>(signal.WaitAsync(cancellationToken));
    }

    /// <inheritdoc />
    public IDisposable OnChange(Action<NodeLeaseChange> listener)
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

            Status = NodeLeasingStatus.Candidate;
            var token = _stopping.Token;
            _loop = Task.Run(() => RunAsync(token), CancellationToken.None);
        }

        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation(
                NodeLeasingEvents.Started,
                "Elector for pool '{Pool}' started as a candidate: {PoolSize} IDs, lease {Lease}, renewal every {RenewInterval}, retry every {RetryInterval}.",
                Pool,
                Options.PoolSize,
                Options.Lease,
                Options.RenewInterval,
                Options.RetryInterval
            );
        }

        if (!_locks.IsCoordinated)
        {
            WarnUncoordinated();
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Stops the elector, releasing the lease it holds so another instance can take the ID at
    /// once, and waits for the loop bounded by <paramref name="cancellationToken"/>.
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
            Status = NodeLeasingStatus.Stopped;
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
    /// Steps down when assigned: releases the lease, raises <see cref="NodeLeaseChangeReason.Released"/>,
    /// and stays a candidate that waits one retry interval before its next scan so another
    /// instance has the first chance. Does nothing when not assigned.
    /// </summary>
    public async ValueTask ReleaseAsync(CancellationToken cancellationToken = default)
    {
        Task released;
        CancellationTokenSource release;
        lock (_gate)
        {
            if (Status != NodeLeasingStatus.Assigned || _release is null)
            {
                return;
            }

            _released ??= NewReleaseSignal();
            released = _released.Task;
            release = _release;
        }

        try
        {
            await release.CancelAsync().ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
            // The term ended on its own between the snapshot and this call, and HoldAsync
            // disposed its release source. The assignment ending is what the release asked for.
        }

        await released.WaitAsync(cancellationToken).ConfigureAwait(false);
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
        NodeLeasingDiagnostics.Unregister(this);
        lock (_gate)
        {
            _assignmentToken?.Dispose();
            _assignmentToken = null;
        }

        _stopping.Dispose();
    }

    private static TaskCompletionSource<int> NewSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static TaskCompletionSource NewReleaseSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private async Task RunAsync(CancellationToken stopping)
    {
        try
        {
            while (!stopping.IsCancellationRequested)
            {
                try
                {
                    var claim = await TryAcquireAsync(stopping).ConfigureAwait(false);
                    if (claim is null)
                    {
                        await DelayAsync(RetryDelay(jitter: true), stopping).ConfigureAwait(false);
                        continue;
                    }

                    var reason = await HoldAsync(claim.Value.Handle, claim.Value.Id, stopping)
                        .ConfigureAwait(false);
                    if (reason is NodeLeaseChangeReason.Released or NodeLeaseChangeReason.Lost)
                    {
                        // One retry interval before scanning again, so another instance gets the
                        // first chance and a flapping backend does not turn renewals into a hot loop.
                        await DelayAsync(
                                RetryDelay(jitter: reason == NodeLeaseChangeReason.Lost),
                                stopping
                            )
                            .ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (stopping.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    // Anything else is a defect or a misbehaving lock. Leasing must not end
                    // silently on it: log, back off one retry interval, and try again.
                    var backoff = RetryDelay(jitter: true);
                    NodeLeasingDiagnostics.LoopFaults.Add(1, _poolTag);
                    _logger.LogError(
                        NodeLeasingEvents.LoopFaulted,
                        exception,
                        "The elector loop for pool '{Pool}' threw; it backs off {Backoff} and continues as a candidate.",
                        Pool,
                        backoff
                    );
                    await DelayAsync(backoff, stopping).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested)
        {
            // Stopping while waiting.
        }
        finally
        {
            Status = NodeLeasingStatus.Stopped;
        }
    }

    private async Task<(ILockHandle Handle, int Id)?> TryAcquireAsync(CancellationToken stopping)
    {
        using var activity = NodeLeasingDiagnostics.ActivitySource.StartActivity("nodeid.acquire");
        activity?.SetTag(NodeLeasingDiagnostics.PoolTag, Pool);
        try
        {
            (ILockHandle Handle, int Id)? claim = null;
            foreach (var id in Candidates())
            {
                var handle = await _locks
                    .TryAcquireAsync(
                        _keyPrefix + id.ToString(CultureInfo.InvariantCulture),
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
                if (handle is not null)
                {
                    claim = (handle, id);
                    break;
                }
            }

            if (_providerDown)
            {
                _providerDown = false;
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation(
                        NodeLeasingEvents.ProviderRecovered,
                        "The lock provider answers again for pool '{Pool}'.",
                        Pool
                    );
                }
            }

            if (claim.HasValue && !claim.Value.Handle.IsCoordinated)
            {
                // A placeholder lease: granted to every instance, so it proves nothing.
                _uncoordinated = true;
                WarnUncoordinated();
                if (Options.WhenUncoordinated == UncoordinatedNodeAssignment.None)
                {
                    await claim.Value.Handle.DisposeAsync().ConfigureAwait(false);
                    claim = null;
                }
            }

            activity?.SetTag("hostloom.node.acquired", claim.HasValue);
            if (claim.HasValue)
            {
                activity?.SetTag("hostloom.node.id", claim.Value.Id);
            }

            return claim;
        }
        catch (LockProviderUnavailableException exception)
        {
            activity?.SetStatus(ActivityStatusCode.Error);
            if (!_providerDown)
            {
                _providerDown = true;
                _logger.LogWarning(
                    NodeLeasingEvents.ProviderUnavailable,
                    exception,
                    "The lock provider is unreachable for pool '{Pool}' ({Kind}); no ID can be assigned until it answers.",
                    Pool,
                    exception.Kind
                );
            }

            return null;
        }
    }

    /// <summary>
    /// The scan order for one attempt: the ID this instance last held first, so a restart or a
    /// release-then-reacquire keeps its ID, then every other ID ascending from a random offset,
    /// so contenders after a mass restart do not all probe the same low IDs first. Over a lock
    /// that does not coordinate, only ID zero is probed: the placeholder grant is certain, and
    /// <see cref="UncoordinatedNodeAssignment.AssignZero"/> is deterministic.
    /// </summary>
    private IEnumerable<int> Candidates()
    {
        if (!_locks.IsCoordinated || _uncoordinated)
        {
            yield return 0;
            yield break;
        }

        var size = Options.PoolSize;
        var sticky = _lastHeldId;
        if (sticky is >= 0 && sticky < size)
        {
            yield return sticky.Value;
        }

        // CA5394: the offset decorrelates contenders; it does not resist an attacker.
#pragma warning disable CA5394
        var offset = Random.Shared.Next(size);
#pragma warning restore CA5394
        for (var i = 0; i < size; i++)
        {
            var id = (offset + i) % size;
            if (id != sticky)
            {
                yield return id;
            }
        }
    }

    private void WarnUncoordinated()
    {
        if (_uncoordinatedWarned)
        {
            return;
        }

        _uncoordinatedWarned = true;
        if (Options.WhenUncoordinated == UncoordinatedNodeAssignment.AssignZero)
        {
            _logger.LogWarning(
                NodeLeasingEvents.Uncoordinated,
                "The lock for pool '{Pool}' does not coordinate across instances (Locking:Enabled = false). NodeLeasing:WhenUncoordinated = AssignZero: this instance holds node 0 without coordination, and so does every other instance configured this way.",
                Pool
            );
        }
        else
        {
            _logger.LogWarning(
                NodeLeasingEvents.Uncoordinated,
                "The lock for pool '{Pool}' does not coordinate across instances (Locking:Enabled = false). NodeLeasing:WhenUncoordinated = None: this instance is never assigned; set AssignZero for a single-instance deployment.",
                Pool
            );
        }
    }

    private async Task<NodeLeaseChangeReason> HoldAsync(
        ILockHandle handle,
        int id,
        CancellationToken stopping
    )
    {
        using var release = new CancellationTokenSource();
        BecomeAssigned(handle, id, release);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            stopping,
            handle.LostToken,
            release.Token
        );

        NodeLeaseChangeReason reason;
        ExceptionDispatchInfo? fault = null;
        try
        {
            var delay = Options.RenewInterval;
            while (true)
            {
                await Task.Delay(delay, _clock, linked.Token).ConfigureAwait(false);
                if (await RenewAsync(handle, linked.Token).ConfigureAwait(false))
                {
                    delay = Options.RenewInterval;
                    continue;
                }

                // A refusal marks the handle lost. A renewal that failed at the provider leaves
                // the lease intact until its end, as the lock's own heartbeat assumes, so the
                // holder keeps its ID and retries until an extension lands or the lease is over.
                if (!handle.IsHeld || RenewalRetryDelay(handle) is not { } retry)
                {
                    reason = NodeLeaseChangeReason.Lost;
                    break;
                }

                delay = retry;
            }
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested)
        {
            reason = NodeLeaseChangeReason.Stopped;
        }
        catch (OperationCanceledException) when (release.IsCancellationRequested)
        {
            reason = NodeLeaseChangeReason.Released;
        }
        catch (OperationCanceledException)
        {
            reason = NodeLeaseChangeReason.Lost;
        }
        catch (Exception exception)
        {
            // Step down first so no work keeps running under a term the loop is about to report
            // as faulted; the loop logs the exception and backs off.
            reason = NodeLeaseChangeReason.Lost;
            fault = ExceptionDispatchInfo.Capture(exception);
        }

        await StepDownAsync(handle, reason).ConfigureAwait(false);
        fault?.Throw();
        return reason;
    }

    private async Task<bool> RenewAsync(ILockHandle handle, CancellationToken cancellationToken)
    {
        var start = _clock.GetTimestamp();
        string outcome;
        bool renewed;
        try
        {
            // A provider failure is logged by the lock and reported as false, never thrown, and
            // leaves the handle held; a refusal or a spent lease marks it lost.
            renewed = await handle
                .ExtendAsync(Options.Lease, cancellationToken)
                .ConfigureAwait(false);
            outcome =
                renewed ? "renewed"
                : handle.IsHeld ? "failed"
                : "refused";
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

        NodeLeasingDiagnostics.RenewDuration.Record(
            _clock.GetElapsedTime(start).TotalSeconds,
            _poolTag,
            new KeyValuePair<string, object?>(NodeLeasingDiagnostics.OutcomeTag, outcome)
        );
        return renewed;
    }

    /// <summary>
    /// When to retry a renewal that failed while the lease still runs: after
    /// <see cref="NodeLeasingOptions.RenewInterval"/>, or half of what is left of the lease when
    /// that is sooner, but never sooner than a twentieth of the lease so the last moments do not
    /// become a hot loop. <see langword="null"/> once the lease is over.
    /// </summary>
    private TimeSpan? RenewalRetryDelay(ILockHandle handle)
    {
        var remaining = handle.LeaseEnd - _clock.GetUtcNow();
        if (remaining <= TimeSpan.Zero)
        {
            return null;
        }

        var due = remaining / 2;
        if (due > Options.RenewInterval)
        {
            due = Options.RenewInterval;
        }

        var floor = Options.Lease / 20;
        if (due < floor)
        {
            due = floor < remaining ? floor : remaining;
        }

        return due;
    }

    private void BecomeAssigned(ILockHandle handle, int id, CancellationTokenSource release)
    {
        _ = handle;
        long term;
        TaskCompletionSource<int> signal;
        lock (_gate)
        {
            term = Interlocked.Increment(ref _term);
            _assignmentToken?.Dispose();
            _assignmentToken = new CancellationTokenSource();
            _release = release;
            _released = null;
            _nodeId = id;
            _lastHeldId = id;
            Status = NodeLeasingStatus.Assigned;
            signal = _becameAssigned;
            _becameAssigned = NewSignal();
        }

        signal.TrySetResult(id);
        Record(NodeLeaseChangeReason.Acquired, id, term);
        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation(
                NodeLeasingEvents.Acquired,
                "This instance holds node {NodeId} in pool '{Pool}' for term {Term}.",
                id,
                Pool,
                term
            );
        }
    }

    private async Task StepDownAsync(ILockHandle handle, NodeLeaseChangeReason reason)
    {
        CancellationTokenSource? assignmentToken;
        TaskCompletionSource? released;
        lock (_gate)
        {
            assignmentToken = _assignmentToken;
            _assignmentToken = null;
            _release = null;
            released = _released;
            _released = null;
            _nodeId = null;
            Status =
                reason == NodeLeaseChangeReason.Stopped
                    ? NodeLeasingStatus.Stopped
                    : NodeLeasingStatus.Candidate;
        }

        if (assignmentToken is not null)
        {
            try
            {
                await assignmentToken.CancelAsync().ConfigureAwait(false);
            }
            catch (AggregateException exception)
            {
                _logger.LogWarning(
                    NodeLeasingEvents.CancellationCallbackFailed,
                    exception,
                    "An assignment cancellation callback for pool '{Pool}' threw; the lease will still be released and the elector continues.",
                    Pool
                );
            }
            finally
            {
                assignmentToken.Dispose();
            }
        }

        // A refused release is reported by the lock as a lost lease, never thrown. Should a lock
        // throw anyway, the transition is still recorded and a release call still returns; the
        // exception then reaches the loop, which logs it and backs off.
        try
        {
            await handle.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            Record(reason, null, Term);
            released?.TrySetResult();
        }

        var term = Term;
        if (reason == NodeLeaseChangeReason.Lost)
        {
            _logger.LogWarning(
                NodeLeasingEvents.Lost,
                "This instance lost its node ID in pool '{Pool}' in term {Term}: a renewal was refused or the lease expired. It is a candidate again.",
                Pool,
                term
            );
        }
        else if (_logger.IsEnabled(LogLevel.Information))
        {
            if (reason == NodeLeaseChangeReason.Released)
            {
                _logger.LogInformation(
                    NodeLeasingEvents.Released,
                    "This instance released its node ID in pool '{Pool}' in term {Term}.",
                    Pool,
                    term
                );
            }
            else
            {
                _logger.LogInformation(
                    NodeLeasingEvents.Stopped,
                    "The elector for pool '{Pool}' stopped and released the lease it held in term {Term}.",
                    Pool,
                    term
                );
            }
        }
    }

    private void Record(NodeLeaseChangeReason reason, int? id, long term)
    {
        NodeLeasingDiagnostics.Changes.Add(
            1,
            _poolTag,
            new KeyValuePair<string, object?>(
                NodeLeasingDiagnostics.ReasonTag,
                NodeLeasingDiagnostics.ReasonName(reason)
            )
        );

        Action<NodeLeaseChange>[] listeners;
        lock (_gate)
        {
            listeners = [.. _listeners];
        }

        var change = new NodeLeaseChange(Pool, id, term, reason);
        foreach (var listener in listeners)
        {
            try
            {
                listener(change);
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    NodeLeasingEvents.ListenerFailed,
                    exception,
                    "A node-leasing listener for pool '{Pool}' threw; the elector continues.",
                    Pool
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

    private sealed class Subscription(NodeIdElector owner, Action<NodeLeaseChange> listener)
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
