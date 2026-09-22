using ValkeyDotNet;

namespace HostLoom.Valkey;

/// <summary>Standalone connection, deadlines, and bounded invalidation settings.</summary>
public sealed class ValkeyOptions
{
    /// <summary>Immutable SDK connection settings, including TLS, ACL credentials and database.</summary>
    public ValkeyClientOptions Connection { get; set; } = new();

    /// <summary>Deadline for each command, script or pipeline, including connection acquisition.</summary>
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Deadline for the readiness PING.</summary>
    public TimeSpan HealthTimeout { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Whether an unsuccessful startup PING fails host startup.</summary>
    public bool FailFast { get; set; } = true;

    /// <summary>Messages buffered by the dedicated subscriber before incoming messages are dropped.</summary>
    public int InvalidationQueueCapacity { get; set; } = 1000;

    /// <summary>
    /// How often the invalidation channel publishes a probe to itself to prove its subscriber
    /// socket still delivers. A probe that does not arrive within <see cref="CommandTimeout"/>
    /// replaces the subscriber and, with <c>Caching:Invalidation:FlushLocalOnReconnect</c>,
    /// flushes the in-process tier, so a silent socket death is noticed within
    /// <see cref="InvalidationProbeInterval"/> plus <see cref="CommandTimeout"/>; without the
    /// probe a connection that dies without a close, such as one dropped by an idle firewall,
    /// would look subscribed for as long as the OS keeps the socket. <see cref="TimeSpan.Zero"/>
    /// disables the probe.
    /// </summary>
    public TimeSpan InvalidationProbeInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Enables ValkeyDotNet's payload-free connection-owner telemetry.</summary>
    public bool EnableTelemetry { get; set; }

    internal ValkeyOptions Snapshot()
    {
        ArgumentNullException.ThrowIfNull(Connection);
        ValidateTimeout(CommandTimeout, nameof(CommandTimeout));
        ValidateTimeout(HealthTimeout, nameof(HealthTimeout));
        ArgumentOutOfRangeException.ThrowIfLessThan(InvalidationQueueCapacity, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(InvalidationQueueCapacity, 1_048_576);
        if (InvalidationProbeInterval != TimeSpan.Zero)
        {
            ValidateTimeout(InvalidationProbeInterval, nameof(InvalidationProbeInterval));
        }

        return new ValkeyOptions
        {
            Connection = Connection,
            CommandTimeout = CommandTimeout,
            HealthTimeout = HealthTimeout,
            FailFast = FailFast,
            InvalidationQueueCapacity = InvalidationQueueCapacity,
            InvalidationProbeInterval = InvalidationProbeInterval,
            EnableTelemetry = EnableTelemetry,
        };
    }

    private static void ValidateTimeout(TimeSpan timeout, string name)
    {
        if (timeout <= TimeSpan.Zero || timeout.TotalMilliseconds > uint.MaxValue - 1)
        {
            throw new ArgumentOutOfRangeException(name);
        }
    }
}
