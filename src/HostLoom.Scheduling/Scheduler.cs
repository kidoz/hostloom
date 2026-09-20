using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HostLoom.Scheduling;

/// <summary>
/// Runs a set of <see cref="ScheduleDefinition"/>s: one sequential loop per schedule that waits
/// for the trigger's next due time on a <see cref="TimeProvider"/>, claims the schedule through
/// the <see cref="IScheduleGuard"/> when it is exclusive, runs the job with the configured
/// timeout, and records the outcome. Composes without a container:
/// <c>new Scheduler(options, definitions)</c>, then <see cref="StartAsync"/>.
/// </summary>
public sealed class Scheduler : IAsyncDisposable
{
    private readonly ScheduleRunner[] _runners;
    private readonly CancellationTokenSource _stopping = new();
    private readonly Lock _gate = new();
    private Task? _loops;
    private bool _started;
    private bool _disposed;

    /// <summary>Composes the scheduler.</summary>
    /// <param name="options">Scheduler-wide defaults.</param>
    /// <param name="schedules">The schedules to run. Names must be unique.</param>
    /// <param name="guard">Decides which instance runs an exclusive schedule; required when any schedule is exclusive.</param>
    /// <param name="timeProvider">The clock every wait and timestamp uses; the system clock by default.</param>
    /// <param name="logger">Receives the events in <see cref="SchedulingEvents"/>.</param>
    /// <exception cref="ArgumentException">The options are not usable, a name repeats, or an exclusive schedule has no guard.</exception>
    public Scheduler(
        SchedulingOptions options,
        IEnumerable<ScheduleDefinition> schedules,
        IScheduleGuard? guard = null,
        TimeProvider? timeProvider = null,
        ILogger<Scheduler>? logger = null
    )
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(schedules);
        List<string> problems = [.. options.Validate()];
        var definitions = schedules.ToArray();
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var definition in definitions)
        {
            ArgumentNullException.ThrowIfNull(definition, nameof(schedules));
            if (!names.Add(definition.Name))
            {
                problems.Add($"Schedule '{definition.Name}' is defined more than once.");
            }

            if (definition.Options.Exclusive && guard is null)
            {
                problems.Add(
                    $"Schedule '{definition.Name}' is exclusive but no guard was supplied; "
                        + "exclusivity across instances needs an IScheduleGuard."
                );
            }
        }

        if (problems.Count > 0)
        {
            throw new ArgumentException(
                "The scheduler composition is not usable: " + string.Join(" ", problems),
                nameof(schedules)
            );
        }

        Options = options;
        Schedules = definitions;
        Guard = guard;
        Clock = timeProvider ?? TimeProvider.System;
        Logger = logger ?? NullLogger<Scheduler>.Instance;
        _runners = new ScheduleRunner[definitions.Length];
        for (var index = 0; index < definitions.Length; index++)
        {
            _runners[index] = new ScheduleRunner(this, definitions[index]);
        }

        if (!options.Enabled)
        {
            Logger.LogWarning(
                SchedulingEvents.Disabled,
                "Scheduling:Enabled is false: none of the {Count} schedule(s) runs in this process.",
                definitions.Length
            );
        }
    }

    /// <summary>The scheduler-wide options.</summary>
    public SchedulingOptions Options { get; }

    /// <summary>The schedules, in registration order.</summary>
    public IReadOnlyList<ScheduleDefinition> Schedules { get; }

    /// <summary>The guard exclusive schedules claim through, or <see langword="null"/>.</summary>
    public IScheduleGuard? Guard { get; }

    /// <summary>Whether schedules run at all.</summary>
    public bool Enabled => Options.Enabled;

    internal TimeProvider Clock { get; }

    internal ILogger<Scheduler> Logger { get; }

    /// <summary>
    /// Starts every schedule's loop on the thread pool and returns at once. Repeated calls do
    /// nothing. With <see cref="SchedulingOptions.Enabled"/> false, nothing starts.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The scheduler was disposed.</exception>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_started)
            {
                return Task.CompletedTask;
            }

            _started = true;
            if (!Enabled)
            {
                return Task.CompletedTask;
            }

            var token = _stopping.Token;
            var loops = new Task[_runners.Length];
            for (var index = 0; index < _runners.Length; index++)
            {
                var runner = _runners[index];
                loops[index] = Task.Run(() => runner.RunAsync(token), CancellationToken.None);
            }

            _loops = Task.WhenAll(loops);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Cancels every waiting and running schedule and waits for the loops to end, bounded by
    /// <paramref name="cancellationToken"/>. A job that ignores its token keeps running past the
    /// bound; the scheduler stops waiting for it.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        Task? loops;
        lock (_gate)
        {
            loops = _loops;
        }

        await _stopping.CancelAsync().ConfigureAwait(false);
        if (loops is null)
        {
            return;
        }

        try
        {
            await loops.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The host gave up waiting; the loops observe the stop token on their own.
        }
    }

    /// <summary>The current state of the schedule named <paramref name="name"/>.</summary>
    /// <exception cref="ArgumentException">No schedule has that name.</exception>
    public ScheduleState GetState(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        foreach (var runner in _runners)
        {
            if (string.Equals(runner.Definition.Name, name, StringComparison.Ordinal))
            {
                return runner.Snapshot();
            }
        }

        throw new ArgumentException($"No schedule is named '{name}'.", nameof(name));
    }

    /// <summary>The current state of every schedule, in registration order.</summary>
    public IReadOnlyList<ScheduleState> GetStates()
    {
        var states = new ScheduleState[_runners.Length];
        for (var index = 0; index < _runners.Length; index++)
        {
            states[index] = _runners[index].Snapshot();
        }

        return states;
    }

    /// <summary>Stops the scheduler and waits for its loops without a bound.</summary>
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
        _stopping.Dispose();
    }

    /// <summary>One schedule's loop and state. State is read under its gate by the probe.</summary>
    private sealed class ScheduleRunner(Scheduler scheduler, ScheduleDefinition definition)
    {
        private static readonly TimeSpan MaxWaitChunk = TimeSpan.FromDays(1);

        private readonly Lock _gate = new();
        private DateTimeOffset? _nextDue;
        private bool _running;
        private long _runs;
        private ScheduleRunOutcome? _lastOutcome;
        private DateTimeOffset? _lastStartedAt;
        private DateTimeOffset? _lastCompletedAt;
        private IScheduleClaim? _heldClaim;
        private DateTimeOffset _heldUntil;

        public ScheduleDefinition Definition { get; } = definition;

        public ScheduleState Snapshot()
        {
            lock (_gate)
            {
                return new ScheduleState(
                    Definition.Name,
                    _nextDue,
                    _running,
                    _runs,
                    _lastOutcome,
                    _lastStartedAt,
                    _lastCompletedAt,
                    _heldClaim is null ? null : _heldUntil
                );
            }
        }

        public async Task RunAsync(CancellationToken stopping)
        {
            var clock = scheduler.Clock;
            var logger = scheduler.Logger;
            var history = default(ScheduleHistory);
            var first = true;
            try
            {
                while (true)
                {
                    var now = clock.GetUtcNow();
                    var due = Definition.Trigger.GetNextDue(now, history);
                    if (due is null)
                    {
                        await ReleaseHeldClaimAsync().ConfigureAwait(false);
                        SetNextDue(null);
                        if (logger.IsEnabled(LogLevel.Information))
                        {
                            logger.LogInformation(
                                SchedulingEvents.ScheduleEnded,
                                "Schedule '{Schedule}' ({Trigger}) has no further occurrence; its loop has ended.",
                                Definition.Name,
                                Definition.Trigger.Description
                            );
                        }

                        return;
                    }

                    SetNextDue(due);
                    if (first)
                    {
                        first = false;
                        if (logger.IsEnabled(LogLevel.Information))
                        {
                            logger.LogInformation(
                                SchedulingEvents.ScheduleStarted,
                                "Schedule '{Schedule}' runs {Trigger}; first due at {DueAt:O}.",
                                Definition.Name,
                                Definition.Trigger.Description,
                                due.Value
                            );
                        }
                    }

                    var remaining = due.Value - now;
                    while (remaining > TimeSpan.Zero)
                    {
                        var wait = remaining > MaxWaitChunk ? MaxWaitChunk : remaining;
                        if (_heldClaim is not null)
                        {
                            // The claim is kept past the run so a slower instance that reaches
                            // the same occurrence is refused; it goes at the lease end.
                            var untilRelease = _heldUntil - clock.GetUtcNow();
                            if (untilRelease <= TimeSpan.Zero)
                            {
                                await ReleaseHeldClaimAsync().ConfigureAwait(false);
                            }
                            else if (untilRelease < wait)
                            {
                                wait = untilRelease;
                            }
                        }

                        await Task.Delay(wait, clock, stopping).ConfigureAwait(false);
                        remaining = due.Value - clock.GetUtcNow();
                    }

                    // Never across an occurrence: a claim this instance still held would refuse
                    // its own next run as surely as another instance's.
                    await ReleaseHeldClaimAsync().ConfigureAwait(false);
                    var run = new ScheduledRun(
                        Definition.Name,
                        due.Value,
                        clock.GetUtcNow(),
                        BeginRun()
                    );
                    var outcome = await ExecuteAsync(run, stopping).ConfigureAwait(false);
                    var completedAt = clock.GetUtcNow();
                    EndRun(outcome, run.StartedAt, completedAt);
                    history = new ScheduleHistory(due, run.StartedAt, completedAt);
                    if (outcome == ScheduleRunOutcome.Canceled)
                    {
                        return;
                    }
                }
            }
            catch (OperationCanceledException) when (stopping.IsCancellationRequested)
            {
                // Stopping while waiting for the next due time.
            }
            catch (Exception exception)
            {
                logger.LogError(
                    SchedulingEvents.LoopFaulted,
                    exception,
                    "The loop of schedule '{Schedule}' faulted and stopped; this is a defect in the scheduler or its trigger.",
                    Definition.Name
                );
            }
            finally
            {
                await ReleaseHeldClaimAsync().ConfigureAwait(false);
                SetNextDue(null);
            }
        }

        private async ValueTask ReleaseHeldClaimAsync()
        {
            IScheduleClaim? claim;
            lock (_gate)
            {
                claim = _heldClaim;
                _heldClaim = null;
            }

            if (claim is not null)
            {
                await claim.DisposeAsync().ConfigureAwait(false);
            }
        }

        private async ValueTask<ScheduleRunOutcome> ExecuteAsync(
            ScheduledRun run,
            CancellationToken stopping
        )
        {
            var clock = scheduler.Clock;
            var logger = scheduler.Logger;
            var options = Definition.Options;
            var nameTag = new KeyValuePair<string, object?>(
                SchedulingDiagnostics.NameTag,
                Definition.Name
            );

            IScheduleClaim? claim = null;
            if (options.Exclusive)
            {
                var lease = options.Lease ?? scheduler.Options.DefaultLease;
                try
                {
                    claim = await scheduler
                        .Guard!.TryClaimAsync(Definition.Name, lease, stopping)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stopping.IsCancellationRequested)
                {
                    return ScheduleRunOutcome.Canceled;
                }
                catch (Exception exception)
                {
                    logger.LogWarning(
                        SchedulingEvents.GuardFailed,
                        exception,
                        "The guard failed while claiming schedule '{Schedule}'; the run due at {DueAt:O} was skipped.",
                        Definition.Name,
                        run.DueAt
                    );
                    SchedulingDiagnostics.Skipped.Add(
                        1,
                        nameTag,
                        new KeyValuePair<string, object?>(
                            SchedulingDiagnostics.ReasonTag,
                            "guard_failed"
                        )
                    );
                    return ScheduleRunOutcome.GuardFailed;
                }

                if (claim is null)
                {
                    if (logger.IsEnabled(LogLevel.Debug))
                    {
                        logger.LogDebug(
                            SchedulingEvents.RunSkipped,
                            "Schedule '{Schedule}' due at {DueAt:O} was skipped: another instance holds the claim.",
                            Definition.Name,
                            run.DueAt
                        );
                    }

                    SchedulingDiagnostics.Skipped.Add(
                        1,
                        nameTag,
                        new KeyValuePair<string, object?>(
                            SchedulingDiagnostics.ReasonTag,
                            "claimed_elsewhere"
                        )
                    );
                    return ScheduleRunOutcome.Skipped;
                }
            }

            var claimedAt = clock.GetUtcNow();
            ScheduleRunOutcome outcome;
            try
            {
                outcome = await RunClaimedAsync(run, claim, nameTag, stopping)
                    .ConfigureAwait(false);
            }
            catch
            {
                if (claim is not null)
                {
                    await claim.DisposeAsync().ConfigureAwait(false);
                }

                throw;
            }

            if (claim is null)
            {
                return outcome;
            }

            // The claim outlives the run until its lease ends, so an instance that reaches this
            // occurrence later is refused rather than running it again. A claim that is no
            // longer held, or a run cut short by stopping or loss, has nothing to keep.
            var holdUntil = claimedAt + (options.Lease ?? scheduler.Options.DefaultLease);
            if (
                claim.IsHeld
                && outcome is not (ScheduleRunOutcome.Canceled or ScheduleRunOutcome.ClaimLost)
                && holdUntil > clock.GetUtcNow()
            )
            {
                lock (_gate)
                {
                    _heldClaim = claim;
                    _heldUntil = holdUntil;
                }
            }
            else
            {
                await claim.DisposeAsync().ConfigureAwait(false);
            }

            return outcome;
        }

        private async ValueTask<ScheduleRunOutcome> RunClaimedAsync(
            ScheduledRun run,
            IScheduleClaim? claim,
            KeyValuePair<string, object?> nameTag,
            CancellationToken stopping
        )
        {
            var clock = scheduler.Clock;
            var logger = scheduler.Logger;
            var options = Definition.Options;
            using var timeout = new CancellationTokenSource();
            using var timeoutTimer = StartTimeout(clock, options.Timeout, timeout);
            {
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                    stopping,
                    timeout.Token,
                    claim?.LostToken ?? CancellationToken.None
                );
                using var activity = SchedulingDiagnostics.ActivitySource.StartActivity(
                    "schedule.run"
                );
                activity?.SetTag(SchedulingDiagnostics.NameTag, Definition.Name);
                activity?.SetTag("hostloom.schedule.sequence", run.Sequence);

                var startedAt = clock.GetTimestamp();
                ScheduleRunOutcome outcome;
                try
                {
                    await Definition.Run(run, linked.Token).ConfigureAwait(false);
                    outcome = CancellationOutcome();
                }
                catch (OperationCanceledException) when (linked.IsCancellationRequested)
                {
                    outcome = CancellationOutcome();
                }
                catch (Exception exception)
                {
                    outcome = ScheduleRunOutcome.Failed;
                    var elapsed = clock.GetElapsedTime(startedAt);
                    logger.LogWarning(
                        SchedulingEvents.RunFailed,
                        exception,
                        "Run {Sequence} of schedule '{Schedule}' failed after {Elapsed} ms; the schedule continues.",
                        run.Sequence,
                        Definition.Name,
                        (long)elapsed.TotalMilliseconds
                    );
                }

                if (outcome == ScheduleRunOutcome.TimedOut)
                    logger.LogWarning(
                        SchedulingEvents.RunTimedOut,
                        "Run {Sequence} of schedule '{Schedule}' exceeded its timeout of {Timeout} and was cancelled.",
                        run.Sequence,
                        Definition.Name,
                        options.Timeout
                    );
                else if (outcome == ScheduleRunOutcome.ClaimLost)
                    logger.LogWarning(
                        SchedulingEvents.ClaimLost,
                        "The exclusive claim for schedule '{Schedule}' was lost during run {Sequence}; the run was cancelled.",
                        Definition.Name,
                        run.Sequence
                    );

                ScheduleRunOutcome CancellationOutcome() =>
                    stopping.IsCancellationRequested ? ScheduleRunOutcome.Canceled
                    : timeout.IsCancellationRequested ? ScheduleRunOutcome.TimedOut
                    : claim?.LostToken.IsCancellationRequested == true
                        ? ScheduleRunOutcome.ClaimLost
                    : ScheduleRunOutcome.Succeeded;

                var outcomeName = SchedulingDiagnostics.OutcomeName(outcome);
                activity?.SetTag(SchedulingDiagnostics.OutcomeTag, outcomeName);
                SchedulingDiagnostics.RunDuration.Record(
                    clock.GetElapsedTime(startedAt).TotalSeconds,
                    nameTag,
                    new KeyValuePair<string, object?>(SchedulingDiagnostics.OutcomeTag, outcomeName)
                );
                SchedulingDiagnostics.Lag.Record(run.Lag.TotalSeconds, nameTag);
                return outcome;
            }
        }

        /// <summary>Cancels <paramref name="source"/> after <paramref name="limit"/> on the scheduler's clock.</summary>
        private static ITimer? StartTimeout(
            TimeProvider clock,
            TimeSpan? limit,
            CancellationTokenSource source
        ) =>
            limit is { } timeout
                ? clock.CreateTimer(
                    static state => ((CancellationTokenSource)state!).Cancel(),
                    source,
                    timeout,
                    Timeout.InfiniteTimeSpan
                )
                : null;

        private long BeginRun()
        {
            lock (_gate)
            {
                _running = true;
                _nextDue = null;
                return ++_runs;
            }
        }

        private void EndRun(
            ScheduleRunOutcome outcome,
            DateTimeOffset startedAt,
            DateTimeOffset completedAt
        )
        {
            lock (_gate)
            {
                _running = false;
                _lastOutcome = outcome;
                _lastStartedAt = startedAt;
                _lastCompletedAt = completedAt;
            }
        }

        private void SetNextDue(DateTimeOffset? due)
        {
            lock (_gate)
            {
                _nextDue = due;
            }
        }
    }
}
