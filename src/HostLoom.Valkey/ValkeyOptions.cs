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

    /// <summary>Enables ValkeyDotNet's payload-free connection-owner telemetry.</summary>
    public bool EnableTelemetry { get; set; }

    internal ValkeyOptions Snapshot()
    {
        ArgumentNullException.ThrowIfNull(Connection);
        ValidateTimeout(CommandTimeout, nameof(CommandTimeout));
        ValidateTimeout(HealthTimeout, nameof(HealthTimeout));
        ArgumentOutOfRangeException.ThrowIfLessThan(InvalidationQueueCapacity, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(InvalidationQueueCapacity, 1_048_576);
        return new ValkeyOptions
        {
            Connection = Connection,
            CommandTimeout = CommandTimeout,
            HealthTimeout = HealthTimeout,
            FailFast = FailFast,
            InvalidationQueueCapacity = InvalidationQueueCapacity,
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
