using HostLoom.Locking;

namespace HostLoom.Conformance;

/// <summary>
/// Maps HostLoom's canonical provider keys to the legacy <c>lock:{consumerKey}</c> identity while
/// delegating ownership, lease, failure, cancellation, and cleanup behavior to the inner provider.
/// </summary>
public sealed class LegacyLockIdentityProvider(ILockProvider inner) : ILockProvider
{
    public ValueTask<bool> TryAcquireAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    ) => inner.TryAcquireAsync(LegacyKey(key), owner, lease, cancellationToken);

    public ValueTask<bool> ReleaseAsync(
        string key,
        string owner,
        CancellationToken cancellationToken = default
    ) => inner.ReleaseAsync(LegacyKey(key), owner, cancellationToken);

    public ValueTask<bool> ExtendAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    ) => inner.ExtendAsync(LegacyKey(key), owner, lease, cancellationToken);

    public static string LegacyKey(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        var marker = key.IndexOf(":lock:", StringComparison.Ordinal);
        if (marker <= 0 || marker + 6 == key.Length)
        {
            throw new ArgumentException(
                "Expected a canonical HostLoom key in the form {namespace}:lock:{key}.",
                nameof(key)
            );
        }

        return string.Concat("lock:", key.AsSpan(marker + 6));
    }
}
