namespace HostLoom.Locking.Testing;

/// <summary>
/// An in-process provider a test can script: hold a key as if another instance owned it, release
/// it, and see who owns what. Everything else behaves like <see cref="InMemoryLockProvider"/>,
/// including lease expiry on the supplied clock.
/// </summary>
public sealed class ManualLockProvider(TimeProvider? timeProvider = null) : ILockProvider
{
    /// <summary>The owner token used for keys held by the test.</summary>
    public const string TestOwner = "held-by-test";

    private readonly InMemoryLockProvider _inner = new(timeProvider);

    /// <summary>
    /// Holds <paramref name="providerKey"/> as if another instance owned it, for
    /// <paramref name="lease"/> or one hour. The key is what the provider sees:
    /// <c>{namespace}:lock:{key}</c>, which <see cref="TestLock.ProviderKey"/> builds.
    /// </summary>
    /// <returns><see langword="false"/> when the key is already held.</returns>
    public bool Hold(string providerKey, TimeSpan? lease = null) =>
        _inner
            .TryAcquireAsync(providerKey, TestOwner, lease ?? TimeSpan.FromHours(1))
            .AsTask()
            .GetAwaiter()
            .GetResult();

    /// <summary>Releases a key held through <see cref="Hold"/>.</summary>
    public bool Release(string providerKey) =>
        _inner.ReleaseAsync(providerKey, TestOwner).AsTask().GetAwaiter().GetResult();

    /// <summary>Keys currently held, by anyone.</summary>
    public int Count => _inner.Count;

    /// <inheritdoc />
    public ValueTask<bool> TryAcquireAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    ) => _inner.TryAcquireAsync(key, owner, lease, cancellationToken);

    /// <inheritdoc />
    public ValueTask<bool> ReleaseAsync(
        string key,
        string owner,
        CancellationToken cancellationToken = default
    ) => _inner.ReleaseAsync(key, owner, cancellationToken);

    /// <inheritdoc />
    public ValueTask<bool> ExtendAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    ) => _inner.ExtendAsync(key, owner, lease, cancellationToken);
}
