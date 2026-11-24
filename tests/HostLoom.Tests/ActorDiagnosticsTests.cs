using System.Diagnostics.Metrics;
using HostLoom.Actors;
using Xunit;

namespace HostLoom.Tests;

// MeterListener sees actor systems across the process. Collect only while this test runs.
[Collection(nameof(ActorDiagnosticsTests))]
[CollectionDefinition(nameof(ActorDiagnosticsTests), DisableParallelization = true)]
public sealed class ActorDiagnosticsTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task MeterReportsMailboxPressureAndExecutionAfterCallerTimeout()
    {
        var token = TestContext.Current.CancellationToken;
        var observations = new List<(Instrument Instrument, long Value, int Tags)>();
        var timings = new List<(Instrument Instrument, double Seconds, int Tags)>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, owner) =>
        {
            if (instrument.Meter.Name == "HostLoom.Actors")
                owner.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>(
            (instrument, value, tags, _) => observations.Add((instrument, value, tags.Length))
        );
        listener.SetMeasurementEventCallback<double>(
            (instrument, value, tags, _) => timings.Add((instrument, value, tags.Length))
        );
        listener.Start();

        void AssertMailboxCounts(long population, long queued, long executing, long timeouts)
        {
            observations.Clear();
            listener.RecordObservableInstruments();
            Assert.All(
                observations,
                item =>
                {
                    var expectedType = item.Instrument.Name
                        is "hostloom.actors.population"
                            or "hostloom.actors.queued"
                            or "hostloom.actors.executing"
                        ? typeof(ObservableGauge<long>)
                        : typeof(ObservableCounter<long>);
                    Assert.IsType(expectedType, item.Instrument);
                    AssertUntagged(item.Instrument, item.Tags);
                }
            );
            var counts = observations.ToDictionary(
                item => item.Instrument.Name,
                item => item.Value,
                StringComparer.Ordinal
            );
            Assert.Equal(population, counts["hostloom.actors.population"]);
            Assert.Equal(queued, counts["hostloom.actors.queued"]);
            Assert.Equal(executing, counts["hostloom.actors.executing"]);
            Assert.Equal(timeouts, counts["hostloom.actors.ask_timeouts"]);
            Assert.Equal(1, counts["hostloom.actors.full"]);
            Assert.Equal(0, counts["hostloom.actors.discarded"]);
            Assert.Equal(0, counts["hostloom.actors.faults"]);
            Assert.Equal(0, counts["hostloom.actors.stopped_sends"]);
        }

        var clock = new TestClock();
        await using var system = new ActorSystem(1, 1, Bound, clock);
        var probe = new ActorTestProbe<int, int>(2, Bound, value => value);
        var actor = system.Spawn<int, int>(() => probe);
        try
        {
            Assert.Equal(ActorAdmission.Accepted, actor.Ref.TryTell(1));
            await probe.WaitForFirstEntryAsync(token);
            clock.Advance(TimeSpan.FromMilliseconds(250));
            var budget = TimeSpan.FromMilliseconds(125);
            var queued = actor.Ref.AskAsync(2, budget, token).AsTask();
            Assert.Equal(ActorAdmission.Full, actor.Ref.TryTell(3));
            AssertMailboxCounts(population: 1, queued: 1, executing: 1, timeouts: 0);

            clock.Advance(budget);
            await Assert.ThrowsAsync<TimeoutException>(() => queued).WaitAsync(Bound, token);
            AssertMailboxCounts(population: 1, queued: 1, executing: 1, timeouts: 1);
            Assert.Equal([1], probe.Entries);
        }
        finally
        {
            probe.Release();
        }

        await actor.StopAsync(token).WaitAsync(Bound, token);
        Assert.Equal([1, 2], probe.Entries);
        AssertMailboxCounts(population: 0, queued: 0, executing: 0, timeouts: 1);
        Assert.All(
            timings,
            sample =>
            {
                Assert.IsType<Histogram<double>>(sample.Instrument);
                Assert.Equal("s", sample.Instrument.Unit);
                AssertUntagged(sample.Instrument, sample.Tags);
            }
        );
        Assert.Equal(
            [0.0, 0.125],
            timings
                .Where(s => s.Instrument.Name == "hostloom.actors.queue_delay")
                .Select(s => s.Seconds)
        );
        Assert.Equal(
            [0.375, 0.0],
            timings
                .Where(s => s.Instrument.Name == "hostloom.actors.turn_duration")
                .Select(s => s.Seconds)
        );
    }

    private static void AssertUntagged(Instrument instrument, int measurementTags)
    {
        Assert.Equal(0, measurementTags);
        Assert.Empty(instrument.Tags ?? []);
        Assert.Empty(instrument.Meter.Tags ?? []);
    }
}
