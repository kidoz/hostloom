using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using HostLoom.Actors;

namespace HostLoom.Benchmarks;

internal static class ActorInboxPilot
{
    private const int Producers = 8;
    private const int Burst = 16;
    private const int WaveMilliseconds = 10;
    private const int Waves = 100;
    internal static readonly TimeSpan Window = TimeSpan.FromHours(1);

    private readonly record struct Delivery(string Key, Guid Token);

    private enum Outcome
    {
        Claim,
        Duplicate,
        Full,
    }

    private readonly record struct Reply(Outcome Outcome, double Micros, double LaunchLagMicros);

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Globalization",
        "CA1303",
        Justification = "The pilot emits an invariant CSV header."
    )]
    internal static async Task RunAsync()
    {
        Console.WriteLine(
            FormattableString.Invariant(
                $"# {RuntimeInformation.OSDescription}, {RuntimeInformation.ProcessArchitecture}, {RuntimeInformation.FrameworkDescription}, logical_cpus={Environment.ProcessorCount}, server_gc={System.Runtime.GCSettings.IsServerGC}"
            )
        );
        await ActorInboxChecks.RunAsync().ConfigureAwait(false);

        var deliveries = MakeTrace();
        Console.WriteLine(
            "store,trial,offered,claims,duplicates,full,elapsed_ms,offered_per_s,completed_per_s,p50_us,p99_us,p99_launch_lag_us"
        );
        for (var trial = 0; trial <= 5; trial++)
        {
            await RunTrialAsync(trial % 2 != 0, trial, deliveries).ConfigureAwait(false);
            await RunTrialAsync(trial % 2 == 0, trial, deliveries).ConfigureAwait(false);
        }
    }

    private static Delivery[] MakeTrace()
    {
        static Guid Id(int index, short kind) => new(index, kind, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        var trace = new Delivery[Waves * Producers * Burst];
        var hot = InboxFilter.KeyFor("orders", "audit", Id(0, 1));
        for (var i = 0; i < trace.Length; i++)
        {
            var key = i % 4 == 0 ? InboxFilter.KeyFor("orders", "audit", Id(i + 1, 1)) : hot;
            trace[i] = new(key, Id(i + 1, 2));
        }
        return trace;
    }

    private static async Task RunTrialAsync(bool actor, int trial, Delivery[] trace)
    {
        await WithStoreAsync(
                actor,
                TimeProvider.System,
                store => MeasureAsync(store, actor, trial, trace)
            )
            .ConfigureAwait(false);
    }

    internal static async Task WithStoreAsync(
        bool actor,
        TimeProvider clock,
        Func<IInboxStore, Task> exercise
    )
    {
        if (actor)
        {
            var candidate = new ActorInboxStore(clock, 256);
            await using var owner = candidate.ConfigureAwait(false);
            await exercise(candidate).ConfigureAwait(false);
        }
        else
            await exercise(new InMemoryInboxStore(clock)).ConfigureAwait(false);
    }

    private static async Task MeasureAsync(
        IInboxStore store,
        bool actor,
        int trial,
        Delivery[] trace
    )
    {
        var waves = trial == 0 ? 20 : Waves;
        var offered = waves * Producers * Burst;
        var pending = new Task<Reply>[offered];
        // All producers use the same absolute schedule. A slow completion never delays a later offer.
        var start = Stopwatch.GetTimestamp();
        async Task ProduceAsync(int producer)
        {
            for (var wave = 0; wave < waves; wave++)
            {
                var due = TimeSpan.FromMilliseconds((wave + 1) * WaveMilliseconds);
                var delay = due - Stopwatch.GetElapsedTime(start);
                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay).ConfigureAwait(false);
                for (var n = 0; n < Burst; n++)
                {
                    var index = (wave * Producers + producer) * Burst + n;
                    pending[index] = RecordAsync(store, trace[index], start, due);
                }
            }
        }
        await Task.WhenAll(
                Enumerable.Range(0, Producers).Select(i => Task.Run(() => ProduceAsync(i)))
            )
            .ConfigureAwait(false);
        var results = await Task.WhenAll(pending).ConfigureAwait(false);
        var elapsed = Stopwatch.GetElapsedTime(start);
        var full = results.Count(r => r.Outcome == Outcome.Full);
        var winners = new Dictionary<string, Guid>(StringComparer.Ordinal);
        for (var i = 0; i < results.Length; i++)
            if (
                results[i].Outcome == Outcome.Claim
                && !winners.TryAdd(trace[i].Key, trace[i].Token)
            )
                throw new InvalidOperationException("Two deliveries won the same key.");

        // Check the live state, including the actual winning token for every contested key.
        foreach (var group in Enumerable.Range(0, offered).GroupBy(i => trace[i].Key))
        {
            var won = winners.TryGetValue(group.Key, out var token);
            if (!won && group.Any(i => results[i].Outcome != Outcome.Full))
                throw new InvalidOperationException("A duplicate had no successful owner.");
            if (
                await store.TryRecordAsync(group.Key, Guid.Empty, Window).ConfigureAwait(false)
                == won
            )
                throw new InvalidOperationException("The stored key disagrees with the replies.");
            if (won)
            {
                await store.ReleaseAsync(group.Key, token).ConfigureAwait(false);
                if (
                    !await store.TryRecordAsync(group.Key, Guid.Empty, Window).ConfigureAwait(false)
                )
                    throw new InvalidOperationException(
                        "The winning owner could not release its key."
                    );
            }
        }
        if (trial == 0)
            return;
        var completed = offered - full;
        var latency = results
            .Where(r => r.Outcome != Outcome.Full)
            .Select(r => r.Micros)
            .Order()
            .ToArray();
        var lag = results.Select(r => r.LaunchLagMicros).Order().ToArray();
        Console.WriteLine(
            string.Create(
                CultureInfo.InvariantCulture,
                $"{(actor ? "actor" : "current")},{trial},{offered},{winners.Count},{completed - winners.Count},{full},{elapsed.TotalMilliseconds:F2},{offered / elapsed.TotalSeconds:F0},{completed / elapsed.TotalSeconds:F0},{Percentile(latency, .50):F2},{Percentile(latency, .99):F2},{Percentile(lag, .99):F2}"
            )
        );
    }

    private static double Percentile(double[] sorted, double fraction) =>
        sorted[(int)Math.Ceiling(sorted.Length * fraction) - 1];

    private static async Task<Reply> RecordAsync(
        IInboxStore store,
        Delivery delivery,
        long start,
        TimeSpan due
    )
    {
        var sent = Stopwatch.GetTimestamp();
        Outcome outcome;
        try
        {
            outcome = await store
                .TryRecordAsync(delivery.Key, delivery.Token, Window)
                .ConfigureAwait(false)
                ? Outcome.Claim
                : Outcome.Duplicate;
        }
        catch (ActorAdmissionException error) when (error.Reason == ActorAdmission.Full)
        {
            outcome = Outcome.Full;
        }
        return new(
            outcome,
            Stopwatch.GetElapsedTime(sent).TotalMicroseconds,
            Math.Max(0, (Stopwatch.GetElapsedTime(start, sent) - due).TotalMicroseconds)
        );
    }
}
