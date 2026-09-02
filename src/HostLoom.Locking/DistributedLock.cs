using System.Diagnostics;
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

    /// <summary>Removes the instance from the metrics registry. Held handles stay valid.</summary>
    public ValueTask DisposeAsync()
    {
        LockingDiagnostics.Unregister(this);
        return ValueTask.CompletedTask;
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
            attempts++;
            bool acquired;
            try
            {
                acquired = await Provider
                    .TryAcquireAsync(prefixed, owner, lease, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (LockProviderException exception)
            {
                throw Unavailable(exception.Kind, exception);
            }
            catch (Exception exception)
            {
                throw Unavailable(LockFailureKind.Other, exception);
            }

            var waited = Clock.GetElapsedTime(start);
            if (acquired)
            {
                LockingDiagnostics.AcquireDuration.Record(
                    waited.TotalSeconds,
                    _namespaceTag,
                    new KeyValuePair<string, object?>(LockingDiagnostics.OutcomeTag, "acquired")
                );
                LockingDiagnostics.Active.Add(1, _namespaceTag);
                activity?.SetTag("hostloom.lock.acquired", true);
                activity?.SetTag("hostloom.lock.wait_ms", waited.TotalMilliseconds);
                return new LockHandle(this, key, prefixed, owner, lease, autoExtend);
            }

            if (attempts > retry.RetryLimit)
            {
                break;
            }

            var delay = retry.GetDelay(attempts);
            if (maxWait is { } bound)
            {
                var remaining = bound - waited;
                if (remaining <= TimeSpan.Zero)
                {
                    break;
                }

                if (delay > remaining)
                {
                    delay = remaining;
                }
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

        public CancellationToken LostToken => CancellationToken.None;

        public ValueTask<bool> ExtendAsync(TimeSpan lease, CancellationToken cancellationToken) =>
            ValueTask.FromResult(true);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
