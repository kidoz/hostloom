using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using HostLoom.Actors;

namespace HostLoom.Benchmarks;

/// <summary>A burst workload with individual queue-delay observations. Each trial drains every admitted command.</summary>
internal static class ActorCatalogComparison
{
    private const int Capacity = 64;
    private const int Producers = 4;
    private const int Burst = 64;
    private const int Waves = 80;
    private static readonly TimeSpan Grace = TimeSpan.FromSeconds(30);

    private readonly record struct Observation(int Index, int Items, long Sent);

    private sealed class Catalog(long[] delays, bool suspend) : IActor<Observation, long>
    {
        private int _ready;
        internal bool Ready => Volatile.Read(ref _ready) != 0;
        internal long Items { get; private set; }

        public async ValueTask<long> ReceiveAsync(Observation message, CancellationToken stopping)
        {
            if (message.Index < 0)
            {
                Volatile.Write(ref _ready, 1);
                return Items;
            }
            delays[message.Index] = Stopwatch.GetTimestamp() - message.Sent + 1;
            if (suspend)
                await Task.Yield();
            Items += message.Items;
            return Items;
        }
    }

    private sealed class Population : IAsyncDisposable
    {
        private readonly ActorSystem? _system;
        private readonly Channel<Observation>[] _channels;
        private readonly Task[] _readers;
        private readonly IActorRef<Observation, long>[] _refs;
        private readonly Catalog[] _catalogs;

        internal Population(bool actors, int count, long[] delays, bool suspend)
        {
            _catalogs = new Catalog[count];
            _refs = new IActorRef<Observation, long>[actors ? count : 0];
            _channels = new Channel<Observation>[actors ? 0 : count];
            _readers = new Task[_channels.Length];
            if (actors)
                _system = new(count, Capacity, Grace);

            for (var i = 0; i < count; i++)
            {
                var catalog = new Catalog(delays, suspend);
                _catalogs[i] = catalog;
                if (_system is not null)
                    _refs[i] = _system.Spawn<Observation, long>(() => catalog).Ref;
                else
                {
                    var channel = Channel.CreateBounded<Observation>(
                        new BoundedChannelOptions(Capacity) { SingleReader = true }
                    );
                    _channels[i] = channel;
                    _readers[i] = ConsumeAsync(channel.Reader, catalog);
                }
            }
        }

        internal async Task WarmOwnersAsync()
        {
            for (var i = 0; i < _catalogs.Length; i++)
                if (!Send(i, new Observation(-1, 0, 0)))
                    throw new InvalidOperationException(
                        "Idle catalog refused its first observation."
                    );
            using var budget = new CancellationTokenSource(Grace);
            while (!_catalogs.All(catalog => catalog.Ready))
                await Task.Delay(1, budget.Token).ConfigureAwait(false);
        }

        private static async Task ConsumeAsync(ChannelReader<Observation> reader, Catalog catalog)
        {
            await foreach (var message in reader.ReadAllAsync().ConfigureAwait(false))
                await catalog.ReceiveAsync(message, CancellationToken.None).ConfigureAwait(false);
        }

        internal bool Send(int owner, Observation message) =>
            _system is null
                ? _channels[owner].Writer.TryWrite(message)
                : _refs[owner].TryTell(message) == ActorAdmission.Accepted;

        internal long Items => _catalogs.Sum(catalog => catalog.Items);

        public async ValueTask DisposeAsync()
        {
            if (_system is not null)
                await _system.DisposeAsync().ConfigureAwait(false);
            else
            {
                foreach (var channel in _channels)
                    channel.Writer.TryComplete();
                await Task.WhenAll(_readers).ConfigureAwait(false);
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Globalization",
        "CA1303",
        Justification = "Benchmark output is an invariant CSV format."
    )]
    internal static async Task RunAsync()
    {
        Console.WriteLine(
            FormattableString.Invariant(
                $"# {RuntimeInformation.OSDescription}, {RuntimeInformation.ProcessArchitecture}, {RuntimeInformation.FrameworkDescription}, CPUs={Environment.ProcessorCount}, server_gc={System.Runtime.GCSettings.IsServerGC}"
            )
        );
        Console.WriteLine(
            "# Four producers, bursts of 64 per producer, 2 ms timer per wave, capacity 64 per owner. One 8-wave warmup per case."
        );
        Console.WriteLine(
            "kind,owners,suspend,trial,offered,completed,elapsed_ms,offered_per_s,completed_per_s,rejected_pct,p50_us,p99_us,bytes_per_completed"
        );
        foreach (var owners in new[] { 1, 16 })
        foreach (var suspend in new[] { false, true })
        {
            for (var trial = 0; trial <= 3; trial++)
                foreach (var actors in new[] { trial % 2 == 0, trial % 2 != 0 })
                    await RunTrialAsync(actors, owners, suspend, trial).ConfigureAwait(false);
        }

        // Amortize process-wide GC noise over a separate large idle population.
        foreach (var actors in new[] { false, true })
            await MeasureIdleAsync(actors).ConfigureAwait(false);
    }

    private static async Task MeasureIdleAsync(bool actors)
    {
        const int owners = 1024;
        var before = GC.GetTotalMemory(forceFullCollection: true);
        var idle = new Population(actors, owners, [], false);
        await idle.WarmOwnersAsync().ConfigureAwait(false);
        var retained = GC.GetTotalMemory(forceFullCollection: true) - before;
        Console.WriteLine(
            FormattableString.Invariant(
                $"# Idle managed bytes/owner ({(actors ? "actor" : "channel")}, {owners} owners): {(double)retained / owners:F0}"
            )
        );
        await idle.DisposeAsync().ConfigureAwait(false);
    }

    private static async Task RunTrialAsync(bool actors, int owners, bool suspend, int trial)
    {
        var waves = trial == 0 ? 8 : Waves;
        var offered = Producers * Burst * waves;
        var delays = new long[offered];
        var population = new Population(actors, owners, delays, suspend);
        await population.WarmOwnersAsync().ConfigureAwait(false);
        var before = GC.GetTotalAllocatedBytes(precise: true);
        var started = Stopwatch.GetTimestamp();
        // Each producer has the same schedule in every trial and proceeds independently of replies.
        async Task<(int Count, long Items)> ProduceAsync(int producer)
        {
            var count = 0;
            long items = 0;
            using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(2));
            for (var wave = 0; wave < waves; wave++)
            {
                await timer.WaitForNextTickAsync().ConfigureAwait(false);
                for (var offset = 0; offset < Burst; offset++)
                {
                    var index = (wave * Producers + producer) * Burst + offset;
                    var observation = new Observation(
                        index,
                        1 + index % 7,
                        Stopwatch.GetTimestamp()
                    );
                    if (population.Send(index % owners, observation))
                    {
                        count++;
                        items += observation.Items;
                    }
                }
            }
            return (count, items);
        }

        (int Count, long Items)[] results;
        try
        {
            results = await Task.WhenAll(Enumerable.Range(0, Producers).Select(ProduceAsync))
                .ConfigureAwait(false);
        }
        finally
        {
            await population.DisposeAsync().ConfigureAwait(false);
        }
        var elapsed = Stopwatch.GetElapsedTime(started);
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - before;
        var accepted = results.Sum(result => result.Count);
        var observed = delays.Where(delay => delay != 0).Order().ToArray();
        if (
            observed.Length != accepted
            || population.Items != results.Sum(result => result.Items)
            || accepted == 0
        )
            throw new InvalidOperationException("Catalog totals differ from admitted work.");
        if (trial == 0)
            return;
        double Micros(double fraction) =>
            (observed[(int)Math.Ceiling(accepted * fraction) - 1] - 1)
            * 1_000_000d
            / Stopwatch.Frequency;
        Console.WriteLine(
            string.Create(
                CultureInfo.InvariantCulture,
                $"{(actors ? "actor" : "channel")},{owners},{suspend},{trial},{offered},{accepted},{elapsed.TotalMilliseconds:F1},{offered / elapsed.TotalSeconds:F0},{accepted / elapsed.TotalSeconds:F0},{100d * (offered - accepted) / offered:F2},{Micros(.50):F2},{Micros(.99):F2},{(double)allocated / accepted:F1}"
            )
        );
    }
}
