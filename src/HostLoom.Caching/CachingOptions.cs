using System.Text.RegularExpressions;

namespace HostLoom.Caching;

/// <summary>
/// Configuration for one cache. Defaults reproduce the platform behaviour a migrating service
/// expects, so a service moves with no tuning. Every duration is a <see cref="TimeSpan"/>.
/// </summary>
/// <remarks>
/// A plain class with a <see cref="Validate"/> method, so a container-free composition fails the
/// same way a hosted one does; the dependency-injection package wraps it in
/// <c>IValidateOptions</c> and validates at startup.
/// </remarks>
public sealed partial class CachingOptions
{
    /// <summary>Required. Prefixes every key; validated against <c>[a-z0-9-]+</c>.</summary>
    public string Namespace { get; set; } = "";

    /// <summary>The in-process tier.</summary>
    public CacheL1Options L1 { get; } = new();

    /// <summary>Best-effort cluster-wide single-flight.</summary>
    public CacheStampedeOptions Stampede { get; } = new();

    /// <summary>Cross-instance invalidation.</summary>
    public CacheInvalidationOptions Invalidation { get; } = new();

    /// <summary>Payload compression in the distributed tier.</summary>
    public CacheCompressionOptions Compression { get; } = new();

    /// <summary>Bulk warmup.</summary>
    public CacheWarmupOptions Warmup { get; } = new();

    /// <summary>Logging rate limits.</summary>
    public CacheDiagnosticsOptions Diagnostics { get; } = new();

    /// <summary>Longest consumer key accepted, excluding the namespace prefix.</summary>
    public int MaxKeyLength { get; set; } = 512;

    /// <summary>
    /// Largest serialized payload written to the distributed tier. An oversize value is logged at
    /// error level and kept in the in-process tier only.
    /// </summary>
    public long MaxPayloadBytes { get; set; } = 10 * 1024 * 1024;

    /// <summary>
    /// Appended to the key segment of every cache entry, so a service bumps its whole payload
    /// schema by changing one value.
    /// </summary>
    public string? PayloadVersion { get; set; }

    /// <summary>
    /// Every violation, each naming the option key it concerns. Empty when the options are valid.
    /// </summary>
    public IReadOnlyList<string> Validate()
    {
        var problems = new List<string>();
        if (!NamespacePattern().IsMatch(Namespace))
        {
            problems.Add(
                "Caching:Namespace is required and must match [a-z0-9-]+; it prefixes every key."
            );
        }

        if (MaxKeyLength <= 0)
        {
            problems.Add("Caching:MaxKeyLength must be positive.");
        }

        if (MaxPayloadBytes <= 0)
        {
            problems.Add("Caching:MaxPayloadBytes must be positive.");
        }

        if (PayloadVersion is { } version && !NamespacePattern().IsMatch(version))
        {
            problems.Add("Caching:PayloadVersion must match [a-z0-9-]+ when set.");
        }

        L1.Validate(problems);
        Stampede.Validate(problems);
        Invalidation.Validate(problems);
        Compression.Validate(problems);
        Warmup.Validate(problems);
        Diagnostics.Validate(problems);
        return problems;
    }

    /// <summary>Throws when <see cref="Validate"/> reports a problem.</summary>
    internal void ThrowIfInvalid(string parameterName)
    {
        var problems = Validate();
        if (problems.Count > 0)
        {
            throw new ArgumentException(
                "CachingOptions are invalid: " + string.Join(" ", problems),
                parameterName
            );
        }
    }

    [GeneratedRegex("^[a-z0-9-]+$")]
    internal static partial Regex NamespacePattern();

    internal static void RequirePositive(List<string> problems, string key, TimeSpan value)
    {
        if (value <= TimeSpan.Zero)
        {
            problems.Add($"{key} must be positive.");
        }
    }

    internal static void RequireNonNegative(List<string> problems, string key, TimeSpan value)
    {
        if (value < TimeSpan.Zero)
        {
            problems.Add($"{key} must not be negative.");
        }
    }
}

/// <summary>Settings for the in-process tier.</summary>
public sealed class CacheL1Options
{
    /// <summary>When <see langword="false"/>, every read goes to the distributed tier.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Entry bound. At capacity a sampled least-recently-accessed fraction is evicted.</summary>
    public int MaxEntries { get; set; } = 10_000;

    /// <summary>
    /// Approximate byte bound, using the serialized size when the value came from the distributed
    /// tier and <see cref="CacheEntryOptions.Size"/> otherwise. Unbounded when null.
    /// </summary>
    public long? MaxBytes { get; set; }

    /// <summary>Time to live applied when no expiration is given for an in-process write.</summary>
    public TimeSpan MaxEntryAge { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>How often expired entries and idle single-flight guards are reclaimed.</summary>
    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>How long an unused single-flight guard survives before it is reclaimed.</summary>
    public TimeSpan GuardIdleTime { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>Fraction of <see cref="MaxEntries"/> evicted when capacity is reached.</summary>
    public double EvictionFraction { get; set; } = 0.25;

    /// <summary>
    /// Random amount subtracted from each in-process expiry so instances do not all miss at the
    /// same instant. Zero disables it.
    /// </summary>
    public TimeSpan ExpirationJitter { get; set; }

    internal void Validate(List<string> problems)
    {
        if (MaxEntries <= 0)
        {
            problems.Add("Caching:L1:MaxEntries must be positive.");
        }

        if (MaxBytes is <= 0)
        {
            problems.Add("Caching:L1:MaxBytes must be positive when set.");
        }

        CachingOptions.RequirePositive(problems, "Caching:L1:MaxEntryAge", MaxEntryAge);
        CachingOptions.RequirePositive(problems, "Caching:L1:CleanupInterval", CleanupInterval);
        CachingOptions.RequirePositive(problems, "Caching:L1:GuardIdleTime", GuardIdleTime);
        CachingOptions.RequireNonNegative(
            problems,
            "Caching:L1:ExpirationJitter",
            ExpirationJitter
        );
        if (EvictionFraction is <= 0 or > 1)
        {
            problems.Add("Caching:L1:EvictionFraction must be greater than 0 and at most 1.");
        }
    }
}

/// <summary>Settings for the best-effort cluster-wide single-flight lease.</summary>
public sealed class CacheStampedeOptions
{
    /// <summary>How long one instance holds the right to run the factory for a key.</summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How many times a caller that missed the lease re-checks the distributed tier.</summary>
    public int Attempts { get; set; } = 2;

    /// <summary>Pause between those re-checks before the caller runs the factory anyway.</summary>
    public TimeSpan WaitBeforeFallback { get; set; } = TimeSpan.FromMilliseconds(50);

    internal void Validate(List<string> problems)
    {
        CachingOptions.RequirePositive(problems, "Caching:Stampede:LeaseDuration", LeaseDuration);
        if (Attempts < 0)
        {
            problems.Add("Caching:Stampede:Attempts must not be negative.");
        }

        CachingOptions.RequireNonNegative(
            problems,
            "Caching:Stampede:WaitBeforeFallback",
            WaitBeforeFallback
        );
    }
}

/// <summary>How the distributed backend learns about invalidations.</summary>
public enum CacheInvalidationMode
{
    /// <summary>Server-assisted tracking when the backend supports it, otherwise broadcast.</summary>
    Auto,

    /// <summary>Server-assisted client tracking.</summary>
    Tracking,

    /// <summary>Keyspace notifications filtered by <see cref="CacheInvalidationOptions.KeyPrefixFilters"/>.</summary>
    Broadcast,
}

/// <summary>Settings for cross-instance invalidation.</summary>
public sealed class CacheInvalidationOptions
{
    /// <summary>Backend invalidation mode. The explicit channel is always subscribed.</summary>
    public CacheInvalidationMode Mode { get; set; } = CacheInvalidationMode.Auto;

    /// <summary>Key prefixes a broadcast subscription listens to.</summary>
    public IList<string> KeyPrefixFilters { get; } = [];

    /// <summary>Time allowed for one publish before it is abandoned and logged.</summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Bound of the queue that applies received invalidations to the in-process tier.</summary>
    public int MaxPending { get; set; } = 1_000;

    internal void Validate(List<string> problems)
    {
        CachingOptions.RequirePositive(problems, "Caching:Invalidation:Timeout", Timeout);
        if (MaxPending <= 0)
        {
            problems.Add("Caching:Invalidation:MaxPending must be positive.");
        }
    }
}

/// <summary>Settings for payload compression.</summary>
public sealed class CacheCompressionOptions
{
    /// <summary>Payloads at or above this size are Brotli-compressed in the distributed tier.</summary>
    public int ThresholdBytes { get; set; } = 1_024;

    internal void Validate(List<string> problems)
    {
        if (ThresholdBytes <= 0)
        {
            problems.Add("Caching:Compression:ThresholdBytes must be positive.");
        }
    }
}

/// <summary>Settings for warmup.</summary>
public sealed class CacheWarmupOptions
{
    /// <summary>Entries written per distributed-store call during warmup.</summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>Whether readiness waits for registered warmups to complete.</summary>
    public bool BlocksReadiness { get; set; }

    internal void Validate(List<string> problems)
    {
        if (BatchSize <= 0)
        {
            problems.Add("Caching:Warmup:BatchSize must be positive.");
        }
    }
}

/// <summary>Settings for cache logging.</summary>
public sealed class CacheDiagnosticsOptions
{
    /// <summary>Minimum interval between two degraded warnings for the same key.</summary>
    public TimeSpan DegradedLogInterval { get; set; } = TimeSpan.FromMinutes(1);

    internal void Validate(List<string> problems)
    {
        CachingOptions.RequirePositive(
            problems,
            "Caching:Diagnostics:DegradedLogInterval",
            DegradedLogInterval
        );
    }
}
