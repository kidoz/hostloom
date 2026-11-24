using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using HostLoom.Actors;
using HostLoom.Examples.Actors;

namespace HostLoom.Benchmarks;

internal static class CatalogCapacityReplay
{
    private static readonly TimeSpan OperationGuard = TimeSpan.FromMinutes(5);
    private const double BytesPerMiB = 1024d * 1024;
    private const string Header = "at_ms,region,source_bytes,io_ms,caller_timeout_ms";

    private sealed record Arrival(
        int Version,
        int AtMs,
        string Region,
        int SourceBytes,
        int IoMs,
        int TimeoutMs
    );

    private enum Execution
    {
        Executed,
        Refused,
        Discarded,
    }

    private sealed record Outcome(Execution Execution, bool CallerTimedOut, double CallerMs);

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Globalization",
        "CA1303",
        Justification = "The replay emits an invariant CSV schema."
    )]
    internal static async Task RunAsync(string[] args)
    {
        if (args is not [_, "actor" or "semaphore", _, _, _, _])
            throw new ArgumentException(
                "Usage: --catalog-replay actor|semaphore TRACE.csv HOT_REGION MAILBOX_CAPACITY CYCLES"
            );
        int capacity = Number(args[4], 1);
        int cycles = Number(args[5], 1);
        var bytes = await File.ReadAllBytesAsync(args[2]).ConfigureAwait(false);
        var rows = ParseTrace(System.Text.Encoding.UTF8.GetString(bytes));
        var regions = rows.Select(row => row.Region).Distinct(StringComparer.Ordinal).ToArray();
        var hot = args[3];
        if (!regions.Contains(hot, StringComparer.Ordinal) || regions.Length < 2)
            throw new ArgumentException(
                "The trace must contain the hot region and an independent region."
            );

        var actors = args[1] == "actor";
        Console.WriteLine(
            FormattableString.Invariant(
                $"# trace_sha256={Convert.ToHexString(SHA256.HashData(bytes))} implementation={args[1]} hot={hot} owners={regions.Length} actor_capacity={capacity} scheduled_window_ms={rows[^1].AtMs} hot_requests={rows.Count(row => row.Region == hot)}"
            )
        );
        Console.WriteLine(
            FormattableString.Invariant(
                $"# {RuntimeInformation.OSDescription}, {RuntimeInformation.ProcessArchitecture}, {RuntimeInformation.FrameworkDescription}, logical_cpus={Environment.ProcessorCount}, server_gc={System.Runtime.GCSettings.IsServerGC}"
            )
        );
        Console.WriteLine(
            "cycle,offered,admitted,refused,executed,discarded,caller_timeouts,offered_per_s,max_dispatch_lag_ms,successful_p99_ms,cold_successes,cold_successful_p99_ms,cold_loads_during_hot_io,drain_and_dispose_ms,sampled_peak_rss_mib,post_drain_rss_mib"
        );
        using var process = Process.GetCurrentProcess();
        for (var cycle = 0; cycle < cycles; cycle++)
        {
            var (line, peakBytes) = await RunCycleAsync(
                    rows,
                    regions,
                    hot,
                    actors,
                    capacity,
                    process
                )
                .ConfigureAwait(false);
            process.Refresh();
            long postDrain = process.WorkingSet64;
            Console.WriteLine(
                FormattableString.Invariant(
                    $"{cycle + 1},{line},{Math.Max(peakBytes, postDrain) / BytesPerMiB:F2},{postDrain / BytesPerMiB:F2}"
                )
            );
        }
    }

    private static int Number(string value, int minimum)
    {
        var parsed = int.Parse(value, NumberStyles.None, CultureInfo.InvariantCulture);
        ArgumentOutOfRangeException.ThrowIfLessThan(parsed, minimum);
        return parsed;
    }

    private static Arrival[] ParseTrace(string text)
    {
        using var reader = new StringReader(text);
        if (reader.ReadLine() != Header)
            throw new FormatException($"Expected header {Header}.");
        var rows = new List<Arrival>();
        while (reader.ReadLine() is string line)
        {
            var fields = line.Split(',');
            if (fields.Length != 5 || string.IsNullOrWhiteSpace(fields[1]))
                throw new FormatException($"Invalid trace row {rows.Count + 2}.");
            var row = new Arrival(
                rows.Count + 1,
                Number(fields[0], 0),
                fields[1],
                Number(fields[2], 0),
                Number(fields[3], 0),
                Number(fields[4], 1)
            );
            if (rows.Count == 0 ? row.AtMs != 0 : row.AtMs < rows[^1].AtMs)
                throw new FormatException(
                    "Arrival offsets must start at zero and be nondecreasing."
                );
            rows.Add(row);
        }
        if (rows.Count == 0 || rows[^1].AtMs == 0)
            throw new FormatException("A replay needs a positive arrival window.");
        return [.. rows];
    }

    private static async Task<(string Line, long PeakBytes)> RunCycleAsync(
        Arrival[] rows,
        string[] regions,
        string hot,
        bool actors,
        int capacity,
        Process process
    )
    {
        var loads = new int[rows.Length];
        int hotLoading = 0,
            overlappingColdLoads = 0;
        async ValueTask<ImmutableArray<string>> LoadAsync(
            string region,
            int version,
            CancellationToken stopping
        )
        {
            var row = rows[version - 1];
            if (row.Region != region || Interlocked.Increment(ref loads[version - 1]) != 1)
                throw new InvalidOperationException("Unexpected physical load.");
            if (region == hot)
                Volatile.Write(ref hotLoading, 1);
            await Task.Delay(row.IoMs, stopping).ConfigureAwait(false);
            if (region == hot)
                Volatile.Write(ref hotLoading, 0);
            else if (Volatile.Read(ref hotLoading) != 0)
                Interlocked.Increment(ref overlappingColdLoads);
            return [new string('x', row.SourceBytes)];
        }

        var pending = new List<Task<Outcome>>(rows.Length);
        // Non-generic joins keep completion evidence without retaining snapshot payloads.
        var semaphoreTurns = new List<Task>();
        var maxLag = 0d;
        double offerMs,
            drainMs;
        long peakBytes;
        Outcome[] results;
        long drainStart;
        using var sampling = new CancellationTokenSource();
        var peak = SamplePeakAsync(process, sampling.Token);
        try
        {
            var workers = new CatalogRefreshWorkers(
                regions,
                actors,
                LoadAsync,
                OperationGuard,
                TimeProvider.System,
                capacity,
                turn => semaphoreTurns.Add(Task.WhenAll(turn))
            );
            await using (workers.ConfigureAwait(false))
            {
                var start = Stopwatch.GetTimestamp();
                try
                {
                    foreach (var row in rows)
                    {
                        var delay =
                            TimeSpan.FromMilliseconds(row.AtMs) - Stopwatch.GetElapsedTime(start);
                        if (delay > TimeSpan.Zero)
                            await Task.Delay((int)Math.Ceiling(delay.TotalMilliseconds))
                                .ConfigureAwait(false);
                        var sent = Stopwatch.GetTimestamp();
                        maxLag = Math.Max(
                            maxLag,
                            Stopwatch.GetElapsedTime(start, sent).TotalMilliseconds - row.AtMs
                        );
                        pending.Add(SendAsync(workers[row.Region], row, sent));
                    }
                    drainStart = Stopwatch.GetTimestamp();
                    offerMs = Stopwatch.GetElapsedTime(start, drainStart).TotalMilliseconds;
                }
                finally
                {
                    try
                    {
                        results = await Task.WhenAll(pending).ConfigureAwait(false);
                    }
                    finally
                    {
                        // Even the operation watchdog may expire before a semaphore turn exits.
                        await Task.WhenAll(semaphoreTurns).ConfigureAwait(false);
                    }
                }
            }
            drainMs = Stopwatch.GetElapsedTime(drainStart).TotalMilliseconds;
        }
        finally
        {
            await sampling.CancelAsync().ConfigureAwait(false);
            peakBytes = await peak.ConfigureAwait(false);
        }
        var executed = results.Count(r => r.Execution == Execution.Executed);
        var discarded = results.Count(r => r.Execution == Execution.Discarded);
        var refused = results.Count(r => r.Execution == Execution.Refused);
        if (
            results.Where((r, i) => loads[i] != (r.Execution == Execution.Executed ? 1 : 0)).Any()
            || rows.Length != executed + discarded + refused
        )
            throw new InvalidOperationException("Replay outcomes differ from physical loads.");
        var successful = results
            .Where(r => r.Execution == Execution.Executed && !r.CallerTimedOut)
            .ToArray();
        var cold = results
            .Where(
                (r, i) =>
                    rows[i].Region != hot && r.Execution == Execution.Executed && !r.CallerTimedOut
            )
            .ToArray();
        var timeouts = results.Count(r => r.CallerTimedOut);
        var line = string.Create(
            CultureInfo.InvariantCulture,
            $"{rows.Length},{executed + discarded},{refused},{executed},{discarded},{timeouts},{rows.Length * 1000 / offerMs:F2},{maxLag:F2},{P99(successful)},{cold.Length},{P99(cold)},{overlappingColdLoads},{drainMs:F2}"
        );
        return (line, peakBytes);
    }

    private static async Task<Outcome> SendAsync(
        RegionalCatalogClient client,
        Arrival row,
        long sent
    )
    {
        var operation = client
            .SendAsync(CatalogRequest.Refresh(row.Version), OperationGuard)
            .AsTask();
        bool timedOut = false;
        try
        {
            // Keep the underlying completion observable after the caller's own wait expires.
            try
            {
                var remaining =
                    TimeSpan.FromMilliseconds(row.TimeoutMs) - Stopwatch.GetElapsedTime(sent);
                if (remaining > TimeSpan.Zero)
                    await operation.WaitAsync(remaining).ConfigureAwait(false);
                else
                    timedOut = true;
            }
            catch (TimeoutException)
            {
                timedOut = true;
            }
            var callerMs = Stopwatch.GetElapsedTime(sent).TotalMilliseconds;
            var reply = await operation.ConfigureAwait(false);
            if (reply.Status == CatalogStatus.Overloaded)
                return new(Execution.Refused, timedOut, callerMs);
            if (
                reply.Status != CatalogStatus.Published
                || reply.Snapshot is null
                || reply.Snapshot.Region != row.Region
                || reply.Snapshot.Version != row.Version
                || reply.Snapshot.Items.Length != 1
                || reply.Snapshot.Items[0].Length != row.SourceBytes
            )
                throw new InvalidOperationException("Unexpected catalog contents or version.");
            return new(Execution.Executed, timedOut, callerMs);
        }
        catch (ActorTerminatedException)
        {
            return new(
                Execution.Discarded,
                timedOut,
                Stopwatch.GetElapsedTime(sent).TotalMilliseconds
            );
        }
    }

    private static string P99(Outcome[] successes)
    {
        if (successes.Length == 0)
            return string.Empty;
        var sorted = successes.Select(r => r.CallerMs).Order().ToArray();
        return sorted[(int)Math.Ceiling(sorted.Length * .99) - 1]
            .ToString("F3", CultureInfo.InvariantCulture);
    }

    private static async Task<long> SamplePeakAsync(Process process, CancellationToken stopping)
    {
        long peak = 0;
        try
        {
            while (true)
            {
                process.Refresh();
                peak = Math.Max(peak, process.WorkingSet64);
                await Task.Delay(10, stopping).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested)
        {
            process.Refresh();
            return Math.Max(peak, process.WorkingSet64);
        }
    }
}
