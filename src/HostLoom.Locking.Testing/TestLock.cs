namespace HostLoom.Locking.Testing;

/// <summary>
/// Composes a <see cref="DistributedLock"/> for a test without a container, over the in-process
/// provider or one the test supplies, on whatever clock the test controls.
/// </summary>
public static class TestLock
{
    /// <summary>The namespace every test lock uses unless the options say otherwise.</summary>
    public const string Namespace = "test";

    /// <summary>A lock over <paramref name="provider"/>, or a fresh <see cref="InMemoryLockProvider"/>.</summary>
    public static DistributedLock Create(
        Action<LockingOptions>? configure = null,
        ILockProvider? provider = null,
        TimeProvider? timeProvider = null
    ) => new(Options(configure), provider ?? new InMemoryLockProvider(timeProvider), timeProvider);

    /// <summary>Options with the test namespace and an immediate, jitter-free retry policy.</summary>
    public static LockingOptions Options(Action<LockingOptions>? configure = null)
    {
        var options = new LockingOptions
        {
            Namespace = Namespace,
            Retry = LockRetryPolicy.Immediate(3),
        };
        configure?.Invoke(options);
        return options;
    }

    /// <summary>The key a provider sees for a consumer key: <c>{namespace}:lock:{key}</c>.</summary>
    public static string ProviderKey(string key, string @namespace = Namespace) =>
        $"{@namespace}:lock:{key}";
}
