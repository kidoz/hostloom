using System.Diagnostics.Metrics;

namespace HostLoom.Valkey;

/// <summary>Fixed-cardinality signals for the Valkey adapter's best-effort invalidation transport.</summary>
public static class ValkeyDiagnostics
{
    /// <summary>Meter name for subscription failures, discarded messages and handler failures.</summary>
    public const string MeterName = "HostLoom.Valkey";
    private static readonly Meter Meter = new(MeterName);
    internal static readonly Counter<long> Dropped = Meter.CreateCounter<long>(
        "hostloom.valkey.invalidation.dropped"
    );
    internal static readonly Counter<long> Failures = Meter.CreateCounter<long>(
        "hostloom.valkey.invalidation.failures"
    );
    internal static readonly Counter<long> Malformed = Meter.CreateCounter<long>(
        "hostloom.valkey.invalidation.malformed"
    );
    internal static readonly Counter<long> HandlerFailures = Meter.CreateCounter<long>(
        "hostloom.valkey.invalidation.handler_failures"
    );
}
