using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HostLoom.Locking;

/// <summary>
/// The composed lock over an <see cref="ILockProvider"/>: namespace prefixing, retries bounded by
/// a <see cref="LockRetryPolicy"/> and <see cref="LockOptions.MaxWait"/>, owner tokens, lease
/// timers, lost-lease detection, re-entrancy detection, metrics, and logging. Composes without a
/// container: <c>new DistributedLock(options, new InMemoryLockProvider())</c>.
/// </summary>
public sealed class DistributedLock : IDistributedLock, IAsyncDisposable
{
    private static readonly AsyncLocal<HeldKeys?> Held = new();
    private static readonly LockOptions ExecuteDefaults = new();
    private static readonly LockOptions SkipIfBusy = new() { MaxWait = TimeSpan.Zero };

    private readonly ILockProvider? _provider;
    private readonly string _prefix;
    private readonly KeyValuePair<string, object?> _namespaceTag;
    private readonly CancellationTokenSource _disposing = new();
    private readonly CancellationToken _disposed;
    private int _disposeCalled;

    /// <summary>
    /// Composes the lock. <paramref name="provider"/> may be <see langword="null"/> only when
    /// <see cref="LockingOptions.Enabled"/> is <see langword="false"/>.
    /// </summary>
    /// <exception cref="ArgumentException"><see cref="LockingOptions.Validate"/> reported a violation.</exception>
    public DistributedLock(
        LockingOptions options,
        ILockProvider? provider,
        TimeProvider? timeProvider = null,
        ILogger<DistributedLock>? logger = null
    )
    {
        ArgumentNullException.ThrowIfNull(options);
        var problems = options.Validate();
        if (problems.Count > 0)
        {
            throw new ArgumentException(
                "LockingOptions are not usable: " + string.Join(" ", problems),
                nameof(options)
            );
        }

        if (options.Enabled && provider is null)
        {
            throw new ArgumentNullException(
                nameof(provider),
                "A lock provider is required unless Locking:Enabled is false."
            );
        }

        Options = options;
        _provider = provider;
        // Read once: the source is disposed with the lock, and the token stays readable after.
        _disposed = _disposing.Token;
        Clock = timeProvider ?? TimeProvider.System;
        Logger = logger ?? NullLogger<DistributedLock>.Instance;
        _prefix = options.Namespace + ":lock:";
        _namespaceTag = new KeyValuePair<string, object?>(
            LockingDiagnostics.NamespaceTag,
            options.Namespace
        );
        LockingDiagnostics.Register(this);

        if (!options.Enabled)
        {
            Logger.LogWarning(
                LockingEvents.Disabled,
                "Locking:Enabled is false for namespace '{Namespace}': single-instance mode, every action runs without coordination.",
                options.Namespace
            );
        }
        else if (Logger.IsEnabled(LogLevel.Information))
        {
            Logger.LogInformation(
                LockingEvents.DerivedMaxWait,
                "Locks in namespace '{Namespace}' retry with {Retry}; the derived maximum wait is {MaxWait} ms.",
                options.Namespace,
                options.Retry.Description,
                options.Retry.MaxTotalDelay.TotalMilliseconds
            );
        }
    }

    /// <summary>The namespace every provider key is prefixed with.</summary>
    public string Namespace => Options.Namespace;

    /// <summary>Whether the lock coordinates across instances or runs actions immediately.</summary>
    public bool Enabled => Options.Enabled;

    /// <inheritdoc />
    public bool IsCoordinated => Options.Enabled;

    internal LockingOptions Options { get; }

    internal TimeProvider Clock { get; }

    internal ILogger<DistributedLock> Logger { get; }

    internal KeyValuePair<string, object?> NamespaceTag => _namespaceTag;

    internal ILockProvider Provider =>
        _provider ?? throw new InvalidOperationException("Locking is disabled.");

    /// <inheritdoc />
    public async ValueTask<T> ExecuteWithLockAsync<T>(
        string key,
        Func<CancellationToken, ValueTask<T>> action,
        LockOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(action);
        LockKey.Validate(key, Options.MaxKeyLength);
        ObjectDisposedException.ThrowIf(_disposed.IsCancellationRequested, this);
        options ??= ExecuteDefaults;

        using var activity = LockingDiagnostics.ActivitySource.StartActivity("lock.execute");
        activity?.SetTag("hostloom.lock.key", key);

        if (!Enabled)
        {
            return await action(cancellationToken).ConfigureAwait(false);
        }

        var handle = await AcquireAsync(key, options, throwWhenBusy: true, cancellationToken)
            .ConfigureAwait(false);
        var previous = Held.Value;
        Held.Value = new HeldKeys(handle!.PrefixedKey, previous);
        using var linked =
            options.OnLost == LostLeaseBehavior.Cancel
                ? CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken,
                    handle.LostToken
                )
                : null;
        try
        {
            return await action(linked?.Token ?? cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Held.Value = previous;
            await handle.DisposeAsync().ConfigureAwait(false);
            activity?.SetTag("hostloom.lock.hold_ms", handle.HoldDuration.TotalMilliseconds);
        }
    }

    /// <inheritdoc />
    public async ValueTask ExecuteWithLockAsync(
        string key,
        Func<CancellationToken, ValueTask> action,
        LockOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(action);
        await ExecuteWithLockAsync(
                key,
                async token =>
                {
                    await action(token).ConfigureAwait(false);
                    return true;
                },
                options,
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<ILockHandle?> TryAcquireAsync(
        string key,
        LockOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        LockKey.Validate(key, Options.MaxKeyLength);
        ObjectDisposedException.ThrowIf(_disposed.IsCancellationRequested, this);
        if (!Enabled)
        {
            return new DisabledLockHandle(key);
        }

        return await AcquireAsync(
                key,
                options ?? SkipIfBusy,
                throwWhenBusy: false,
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <summary>Execution-free description of this composition; see <see cref="LockingProbe"/>.</summary>
    public LockDescription Describe()
    {
        var provider = Enabled ? Provider.GetType().Name : "(disabled)";
        var retry = Options.Retry;
        string[] lines =
        [
            $"Namespace = {Options.Namespace} (Locking:Namespace)",
            Enabled
                ? $"Provider = {provider} (Locking:Enabled = true)"
                : "Provider = (disabled) (Locking:Enabled = false): actions run without coordination",
            $"DefaultLease = {Options.DefaultLease} (Locking:DefaultLease), MaxLease = {Options.MaxLease} (Locking:MaxLease), MaxHold = {Options.MaxHold} (Locking:MaxHold)",
            $"Retry = {retry.Description} (Locking:Retry); derived maximum wait {retry.MaxTotalDelay}",
            $"AutoExtend = {Options.AutoExtend} (Locking:AutoExtend)",
            $"DetectReentrancy = {Options.DetectReentrancy} (Locking:DetectReentrancy)",
        ];
        return new LockDescription(
            Options.Namespace,
            provider,
            Enabled,
            Options.DefaultLease,
            Options.MaxLease,
            retry.Description,
            retry.MaxTotalDelay,
            lines
        );
    }

    /// <summary>
    /// Removes the instance from the metrics registry and stops the provider calls of
    /// acquisitions still in flight; their callers get <see cref="ObjectDisposedException"/>.
    /// Held handles stay valid and still release. Later acquisitions throw
    /// <see cref="ObjectDisposedException"/>.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeCalled, 1) != 0)
        {
            return;
        }

        LockingDiagnostics.Unregister(this);
        // The callbacks are the in-flight attempts' own, which never throw.
        await _disposing.CancelAsync().ConfigureAwait(false);
        _disposing.Dispose();
    }

    private async ValueTask<LockHandle?> AcquireAsync(
        string key,
        LockOptions options,
        bool throwWhenBusy,
        CancellationToken cancellationToken
    )
    {
        var prefixed = _prefix + key;
        if (Options.DetectReentrancy && HeldKeys.Contains(Held.Value, prefixed))
        {
            throw new LockReentrancyException(key);
        }

        var lease = options.Lease ?? Options.DefaultLease;
        if (lease <= TimeSpan.Zero)
        {
            throw new ArgumentException("LockOptions.Lease must be positive.", nameof(options));
        }

        if (lease > Options.MaxLease)
        {
            lease = Options.MaxLease;
        }

        var retry = options.Retry ?? Options.Retry;
        var maxWait = options.MaxWait;
        var autoExtend = options.AutoExtend ?? Options.AutoExtend;
        var owner = Guid.NewGuid().ToString("N");

        using var activity = LockingDiagnostics.ActivitySource.StartActivity("lock.acquire");
        activity?.SetTag("hostloom.lock.key", key);

        var start = Clock.GetTimestamp();
        var attempts = 0;
        while (true)
        {
            bool acquired;
            AcquisitionAttempt attempt;

            // The caller's token and MaxWait bound the lock's wait for a reply, never the provider
            // call: an abandoned call runs on under its own lease-bound token, so a grant it still
            // delivers is seen and released rather than hidden by a provider that was cancelled.
            using (var bounded = Budget.ForAttempt(maxWait, start, Clock, cancellationToken))
            {
                var wait = bounded?.Token ?? cancellationToken;
                cancellationToken.ThrowIfCancellationRequested();
                if (bounded is { Expired: true })
                {
                    // Nothing was sent: the bound ran out before the attempt could start.
                    break;
                }

                ObjectDisposedException.ThrowIf(_disposed.IsCancellationRequested, this);
                attempts++;
                attempt = new AcquisitionAttempt(this, key, prefixed, owner, lease, _disposed);
                try
                {
                    acquired = await attempt.Reply.WaitAsync(wait).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (wait.IsCancellationRequested)
                {
                    attempt.Abandon();
                    if (!cancellationToken.IsCancellationRequested)
                    {
                        // MaxWait, not the caller, ended the wait. A backend that stops answering
                        // is reported the same way as contention.
                        break;
                    }

                    throw;
                }
                catch (Exception exception)
                {
                    // The caller gets the provider's failure at once. A Timeout or Other failure
                    // may still have taken the key, and the attempt releases it on its own.
                    attempt.Abandon();
                    throw Failed(exception);
                }
            }

            var waited = Clock.GetElapsedTime(start);
            if (acquired)
            {
                // A successful but late reply is not a usable grant. Do not create a handle
                // or start an action while its expiry callback is merely queued. Whatever the
                // backend still holds for this owner is given back best-effort, as after a
                // cancelled acquisition.
                if (attempt.Deadline.IsSpent(Clock))
                {
                    attempt.Abandon();
                    throw Unavailable(
                        LockFailureKind.Timeout,
                        new LockProviderException(
                            LockFailureKind.Timeout,
                            "The acquisition reply arrived after the usable lease expired."
                        )
                    );
                }

                LockingDiagnostics.AcquireDuration.Record(
                    waited.TotalSeconds,
                    _namespaceTag,
                    new KeyValuePair<string, object?>(LockingDiagnostics.OutcomeTag, "acquired")
                );
                LockingDiagnostics.Active.Add(1, _namespaceTag);
                activity?.SetTag("hostloom.lock.acquired", true);
                activity?.SetTag("hostloom.lock.wait_ms", waited.TotalMilliseconds);
                return new LockHandle(this, key, prefixed, owner, attempt.Deadline, autoExtend);
            }

            if (attempts > retry.RetryLimit)
            {
                break;
            }

            var delay = retry.GetDelay(attempts);
            if (maxWait is { } bound && delay >= bound - waited)
            {
                // The next attempt would start on or after the bound, so waiting for it would buy
                // nothing: stop now rather than sleeping out a budget nothing can use.
                break;
            }

            if (delay > TimeSpan.Zero)
            {
                await Task.Delay(delay, Clock, cancellationToken).ConfigureAwait(false);
            }
        }

        var total = Clock.GetElapsedTime(start);
        LockingDiagnostics.AcquireDuration.Record(
            total.TotalSeconds,
            _namespaceTag,
            new KeyValuePair<string, object?>(LockingDiagnostics.OutcomeTag, "not_acquired")
        );
        activity?.SetTag("hostloom.lock.acquired", false);
        activity?.SetTag("hostloom.lock.wait_ms", total.TotalMilliseconds);
        return throwWhenBusy ? throw new LockNotAcquiredException(key, total, attempts) : null;

        Exception Failed(Exception exception) =>
            exception switch
            {
                LockProviderException provider => Unavailable(provider.Kind, provider),
                OperationCanceledException when _disposed.IsCancellationRequested =>
                    new ObjectDisposedException(
                        nameof(DistributedLock),
                        "The lock was disposed while the acquisition was in flight."
                    ),
                OperationCanceledException cancelled => Unavailable(
                    LockFailureKind.Timeout,
                    new LockProviderException(
                        LockFailureKind.Timeout,
                        "The provider did not answer within the usable lease.",
                        cancelled
                    )
                ),
                _ => Unavailable(LockFailureKind.Other, exception),
            };

        LockProviderUnavailableException Unavailable(LockFailureKind kind, Exception cause)
        {
            var waited = Clock.GetElapsedTime(start);
            LockingDiagnostics.AcquireDuration.Record(
                waited.TotalSeconds,
                _namespaceTag,
                new KeyValuePair<string, object?>(LockingDiagnostics.OutcomeTag, "unavailable")
            );
            activity?.SetTag("hostloom.lock.acquired", false);
            return new LockProviderUnavailableException(key, waited, attempts, kind, cause);
        }
    }

    /// <summary>
    /// What is left of <see cref="LockOptions.MaxWait"/> for one attempt, as the token the lock's
    /// wait for the reply runs under; the provider call never sees it. Null when the caller set no
    /// bound, or set a zero one: skip-if-busy makes exactly one attempt and bounds the wait by the
    /// caller's token alone.
    /// </summary>
    private sealed class Budget : IDisposable
    {
        private readonly CancellationTokenSource _expiry;
        private readonly CancellationTokenSource _linked;

        private Budget(TimeSpan remaining, TimeProvider clock, CancellationToken cancellationToken)
        {
            _expiry = new CancellationTokenSource(remaining, clock);
            _linked = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken,
                _expiry.Token
            );
        }

        public CancellationToken Token => _linked.Token;

        /// <summary>Whether the bound, rather than the caller, ended the attempt.</summary>
        public bool Expired => _expiry.IsCancellationRequested;

        public static Budget? ForAttempt(
            TimeSpan? maxWait,
            long start,
            TimeProvider clock,
            CancellationToken cancellationToken
        )
        {
            if (maxWait is not { } bound || bound <= TimeSpan.Zero)
            {
                return null;
            }

            var remaining = bound - clock.GetElapsedTime(start);
            return new Budget(
                remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero,
                clock,
                cancellationToken
            );
        }

        public void Dispose()
        {
            _linked.Dispose();
            _expiry.Dispose();
        }
    }

    /// <summary>
    /// Immutable list of the prefixed keys held by the current asynchronous flow. Immutable so a
    /// continuation that outlives the release still sees the set as it was when it forked.
    /// </summary>
    private sealed class HeldKeys(string key, HeldKeys? next)
    {
        public static bool Contains(HeldKeys? head, string key)
        {
            for (var current = head; current is not null; current = current.Next)
            {
                if (string.Equals(current.Key, key, StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }

        private string Key { get; } = key;

        private HeldKeys? Next { get; } = next;
    }

    /// <summary>The handle single-instance mode hands out: held for ever, never lost, nothing to release.</summary>
    private sealed class DisabledLockHandle(string key) : ILockHandle
    {
        public string Key { get; } = key;

        public bool IsHeld => true;

        public DateTimeOffset LeaseEnd => DateTimeOffset.MaxValue;

        public bool IsCoordinated => false;

        public CancellationToken LostToken => CancellationToken.None;

        public ValueTask<bool> ExtendAsync(TimeSpan lease, CancellationToken cancellationToken) =>
            ValueTask.FromResult(true);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
