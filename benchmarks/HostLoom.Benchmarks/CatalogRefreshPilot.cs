using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using HostLoom.Examples.Actors;

namespace HostLoom.Benchmarks;

internal static class CatalogRefreshPilot
{
    private static readonly string[] Regions = ["eu", "us", "ap", "ca"];
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(5);
    private const int Versions = 60;

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Globalization",
        "CA1303",
        Justification = "The pilot emits an invariant CSV header."
    )]
    internal static async Task RunAsync()
    {
        var root = Directory.CreateTempSubdirectory("hostloom-catalog-refresh-");
        try
        {
            foreach (var region in Regions)
            {
                Directory.CreateDirectory(Path.Combine(root.FullName, region));
                for (var version = 1; version <= Versions; version++)
                    await File.WriteAllLinesAsync(
                            FilePath(root.FullName, region, version),
                            ExpectedItems(region, version)
                        )
                        .ConfigureAwait(false);
            }
            Console.WriteLine(
                FormattableString.Invariant(
                    $"# {RuntimeInformation.OSDescription}, {RuntimeInformation.ProcessArchitecture}, {RuntimeInformation.FrameworkDescription}, logical_cpus={Environment.ProcessorCount}, server_gc={System.Runtime.GCSettings.IsServerGC}"
                )
            );
            Console.WriteLine(
                "worker,trial,refreshes,current_reads,source_reads,elapsed_ms,refresh_p50_ms,refresh_p99_ms,current_p50_ms,current_p99_ms"
            );
            for (var trial = 0; trial <= 5; trial++)
            {
                await MeasureAsync(root.FullName, trial % 2 == 0, trial).ConfigureAwait(false);
                await MeasureAsync(root.FullName, trial % 2 != 0, trial).ConfigureAwait(false);
            }
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    private static string FilePath(string root, string region, int version) =>
        Path.Combine(root, region, version.ToString(CultureInfo.InvariantCulture) + ".txt");

    private static string[] ExpectedItems(string region, int version) =>
        ["notebook", FormattableString.Invariant($"{region}-{version}")];

    private readonly record struct Sample(CatalogReply Reply, double Milliseconds);

    private static async Task<Sample> TimedAsync(
        RegionalCatalogClient client,
        CatalogRequest request
    )
    {
        var start = Stopwatch.GetTimestamp();
        var reply = await client.SendAsync(request, Guard).ConfigureAwait(false);
        return new(reply, Stopwatch.GetElapsedTime(start).TotalMilliseconds);
    }

    private static async Task MeasureAsync(string root, bool actors, int trial)
    {
        var waves = trial == 0 ? 12 : Versions;
        var sourceReads = new System.Collections.Concurrent.ConcurrentDictionary<
            (string, int),
            int
        >();
        async ValueTask<ImmutableArray<string>> LoadAsync(
            string region,
            int version,
            CancellationToken stopping
        )
        {
            sourceReads.AddOrUpdate((region, version), 1, static (_, count) => count + 1);
            await Task.Delay(TimeSpan.FromMilliseconds(8), stopping).ConfigureAwait(false);
            return
            [
                .. await File.ReadAllLinesAsync(FilePath(root, region, version), stopping)
                    .ConfigureAwait(false),
            ];
        }

        var workers = new CatalogRefreshWorkers(
            Regions,
            actors,
            LoadAsync,
            Guard,
            TimeProvider.System
        );
        await using var owned = workers.ConfigureAwait(false);
        var targets = Regions.SelectMany((r, i) => Enumerable.Repeat(r, i == 0 ? 8 : 2)).ToArray();
        var refreshTimes = new List<double>(waves * targets.Length);
        var readTimes = new List<double>(waves * Regions.Length);
        var start = Stopwatch.GetTimestamp();
        for (var version = 1; version <= waves; version++)
        {
            var request = CatalogRequest.Refresh(version);
            var refreshed = await Task.WhenAll(
                    targets.Select(region => TimedAsync(workers[region], request))
                )
                .ConfigureAwait(false);
            var current = await Task.WhenAll(
                    Regions.Select(region => TimedAsync(workers[region], CatalogRequest.Read))
                )
                .ConfigureAwait(false);
            CheckWave(refreshed, targets, version, refreshTimes);
            CheckWave(current, Regions, version, readTimes);
        }
        var elapsed = Stopwatch.GetElapsedTime(start);
        if (
            sourceReads.Count != Regions.Length * waves
            || sourceReads.Values.Any(count => count != 1)
        )
            throw new InvalidOperationException(
                "Expected exactly one source load per region and version."
            );
        if (trial == 0)
            return;
        refreshTimes.Sort();
        readTimes.Sort();
        Console.WriteLine(
            string.Create(
                CultureInfo.InvariantCulture,
                $"{(actors ? "actor" : "semaphore")},{trial},{refreshTimes.Count},{readTimes.Count},{sourceReads.Count},{elapsed.TotalMilliseconds:F2},{Percentile(refreshTimes, .50):F4},{Percentile(refreshTimes, .99):F4},{Percentile(readTimes, .50):F4},{Percentile(readTimes, .99):F4}"
            )
        );
    }

    private static void CheckWave(
        Sample[] samples,
        string[] regions,
        int version,
        List<double> times
    )
    {
        for (var i = 0; i < samples.Length; i++)
        {
            var (reply, ms) = samples[i];
            var snapshot = reply.Snapshot;
            if (
                reply.Status != CatalogStatus.Published
                || snapshot is null
                || snapshot.Region != regions[i]
                || snapshot.Version != version
                || !snapshot.Items.SequenceEqual(ExpectedItems(regions[i], version))
            )
                throw new InvalidOperationException(
                    "Catalog contents or version differed from the trace."
                );
            times.Add(ms);
        }
    }

    private static double Percentile(List<double> sorted, double p) =>
        sorted[(int)Math.Ceiling(p * sorted.Count) - 1];
}
