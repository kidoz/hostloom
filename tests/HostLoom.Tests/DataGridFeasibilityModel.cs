using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text.Json;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using HostLoom.DataGrid.ProcessProbe;

namespace HostLoom.Tests;

internal static class DataGridFeasibilityMemory
{
    internal const long DriverHeapBytes = 192L * 1024 * 1024;
    internal const long MemberHeapBytes = 448L * 1024 * 1024;
    internal const long RemainingContainerBytes = 512L * 1024 * 1024;
    internal static bool Bounded =>
        Environment.GetEnvironmentVariable("HOSTLOOM_GRID_FEASIBILITY_BOUNDED_GC") == "1";
    internal static object Profile =>
        new
        {
            Name = Bounded ? "shared-2g-bounded-gc-v1" : "runtime_defaults",
            DriverHeapLimitBytes = Bounded ? (long?)DriverHeapBytes : null,
            MemberHeapLimitBytes = Bounded ? (long?)MemberHeapBytes : null,
            RemainingContainerBytes = Bounded ? (long?)RemainingContainerBytes : null,
        };
}

internal sealed record DataGridFeasibilityCase(
    int KeyCount,
    int BatchSize,
    int Concurrency,
    int Seed,
    int WarmupSeconds,
    int SampleSeconds
)
{
    internal void Validate()
    {
        if (
            KeyCount is < 256 or > 100000
            || KeyCount % 2 != 0
            || BatchSize is not (1 or 32 or 128)
            || Concurrency is not (1 or 16 or 64)
            || Seed is < 1 or > 1000000
            || WarmupSeconds is < 0 or > 60
            || SampleSeconds is < 1 or > 300
        )
            throw new ArgumentException("Unsupported bounded feasibility case.");
    }

    internal bool IsP0 => KeyCount == 100000 && WarmupSeconds == 60 && SampleSeconds == 300;
    internal static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        WriteIndented = true,
    };

    internal static DataGridKey Key(int index)
    {
        var bytes = new byte[32];
        "catalog:"u8.CopyTo(bytes);
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(8), index);
        return new(bytes);
    }

    internal static byte[] Value(int seed, int worker, ulong sequence, int index)
    {
        var bytes = new byte[1024];
        "HLFB"u8.CopyTo(bytes);
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(4), seed);
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(8), worker);
        BinaryPrimitives.WriteUInt64BigEndian(bytes.AsSpan(12), sequence);
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(20), index);
        bytes.AsSpan(24, 968).Fill((byte)(index % 251));
        SHA256.HashData(bytes.AsSpan(0, 992), bytes.AsSpan(992));
        return bytes;
    }

    internal static void ValidateValue(byte[] bytes)
    {
        if (
            bytes.Length != 1024
            || !bytes.AsSpan(0, 4).SequenceEqual("HLFB"u8)
            || !SHA256.HashData(bytes.AsSpan(0, 992)).AsSpan().SequenceEqual(bytes.AsSpan(992))
        )
            throw new InvalidDataException("Feasibility read value integrity failure.");
    }

    internal static DataGridClientRequest Put(
        long session,
        ulong sequence,
        int[] indices,
        int seed,
        int worker,
        bool ttl
    ) =>
        DataGridClientRequest.Mutate(
            new(
                DataGridMutationKind.Put,
                ProbeConfiguration.Map,
                session,
                sequence,
                ttl ? DataGridExpiryKind.Relative : DataGridExpiryKind.Never,
                ttl ? TimeSpan.FromSeconds(60).Ticks : 0,
                indices
                    .Select(i => new DataGridMutationItem(Key(i), Value(seed, worker, sequence, i)))
                    .ToArray()
            )
        );

    internal static double? Percentile(IEnumerable<double> values, double percentile)
    {
        var sorted = values.Order().ToArray();
        return sorted.Length == 0
            ? null
            : sorted[Math.Max(0, (int)Math.Ceiling(percentile * sorted.Length) - 1)];
    }
}

internal sealed record DataGridFeasibilityCall(
    int Worker,
    string Operation,
    ulong RequestNumber,
    string? Digest,
    bool NewLogical,
    bool LogicalCompletedHere,
    bool OriginatedHere,
    double StartedUs,
    double DurationUs,
    double? LogicalDurationUs,
    bool WithinWindow,
    string Outcome,
    bool MayHaveBeenAdmitted,
    int Attempts,
    int Items,
    int Hits,
    int ItemFailures
);
