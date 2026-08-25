using System.Diagnostics.Metrics;
using HostLoom.Pipelines;
using Xunit;

namespace HostLoom.Tests;

public sealed class InstrumentedFilterTests
{
    [Fact]
    public async Task Recorded_duration_is_the_filters_own_time_excluding_downstream()
    {
        const string pipeline = "self-time";
        var clock = new TestClock();
        using var recorder = new PipelineMetricRecorder(pipeline);
        var filter = new AdvancingFilter(
            clock,
            before: TimeSpan.FromMilliseconds(30),
            after: TimeSpan.FromMilliseconds(20)
        );
        var pipe = Pipe.Create<MeteredContext>(builder =>
        {
            builder.Use(
                new InstrumentedFilter<MeteredContext>(
                    filter,
                    pipeline,
                    "enrich",
                    "slow_attribute",
                    clock
                )
            );
            builder.UseExecute(_ =>
            {
                clock.Advance(TimeSpan.FromMilliseconds(500));
                return ValueTask.CompletedTask;
            });
        });

        await pipe.SendAsync(new MeteredContext());

        var measurement = Assert.Single(recorder.Measurements("hostloom.pipeline.filter.duration"));
        // 550 ms elapsed inside the filter, 500 ms of it downstream: the filter owns 50 ms.
        Assert.Equal(0.05, measurement.Value, precision: 9);
        Assert.Equal("slow_attribute", measurement.Tags["hostloom.pipeline.filter"]);
        Assert.Equal("enrich", measurement.Tags["hostloom.pipeline.stage"]);
        Assert.Equal("success", measurement.Tags["hostloom.pipeline.outcome"]);
    }

    [Fact]
    public async Task A_faulting_filter_counts_a_failure_and_tags_the_duration()
    {
        const string pipeline = "faulting";
        using var recorder = new PipelineMetricRecorder(pipeline);
        var pipe = Pipe.Create<MeteredContext>(builder =>
            builder.Use(
                new InstrumentedFilter<MeteredContext>(
                    new ThrowingFilter(),
                    pipeline,
                    null,
                    "broken"
                )
            )
        );

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pipe.SendAsync(new MeteredContext())
        );

        var failure = Assert.Single(recorder.Measurements("hostloom.pipeline.filter.failures"));
        Assert.Equal(1, failure.Value);
        Assert.Equal("broken", failure.Tags["hostloom.pipeline.filter"]);
        var duration = Assert.Single(recorder.Measurements("hostloom.pipeline.filter.duration"));
        Assert.Equal("failure", duration.Tags["hostloom.pipeline.outcome"]);
    }

    [Fact]
    public async Task Cancellation_is_an_outcome_but_never_a_failure()
    {
        const string pipeline = "canceling";
        using var recorder = new PipelineMetricRecorder(pipeline);
        var pipe = Pipe.Create<MeteredContext>(builder =>
            builder.Use(
                new InstrumentedFilter<MeteredContext>(
                    new CancellingFilter(),
                    pipeline,
                    null,
                    "stopped"
                )
            )
        );

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await pipe.SendAsync(new MeteredContext())
        );

        Assert.Empty(recorder.Measurements("hostloom.pipeline.filter.failures"));
        var duration = Assert.Single(recorder.Measurements("hostloom.pipeline.filter.duration"));
        Assert.Equal("canceled", duration.Tags["hostloom.pipeline.outcome"]);
    }

    [Fact]
    public void The_wrapper_is_transparent_to_the_probe()
    {
        var pipe = Pipe.Create<MeteredContext>(builder =>
            builder.Use(
                new InstrumentedFilter<MeteredContext>(new ThrowingFilter(), "probe", null, "inner")
            )
        );

        var probe = PipelineProbe.Inspect(pipe, TestContext.Current.CancellationToken);

        // The instrumented wrapper adds no scope of its own; the inner filter's shape is reported.
        Assert.Equal(["ThrowingFilter", "empty"], probe.Children.Select(child => child.Name));
    }

    private sealed class MeteredContext : PipeContext;

    private sealed class AdvancingFilter(TestClock clock, TimeSpan before, TimeSpan after)
        : IFilter<MeteredContext>
    {
        public async ValueTask SendAsync(MeteredContext context, IPipe<MeteredContext> next)
        {
            clock.Advance(before);
            await next.SendAsync(context);
            clock.Advance(after);
        }
    }

    private sealed class ThrowingFilter : IFilter<MeteredContext>
    {
        public ValueTask SendAsync(MeteredContext context, IPipe<MeteredContext> next) =>
            throw new InvalidOperationException("broken filter");
    }

    private sealed class CancellingFilter : IFilter<MeteredContext>
    {
        public ValueTask SendAsync(MeteredContext context, IPipe<MeteredContext> next) =>
            throw new OperationCanceledException();
    }

    /// <summary>
    /// Collects pipeline measurements for one pipeline name. The meter is process-wide and static,
    /// so filtering by pipeline keeps concurrently running tests from contaminating each other.
    /// </summary>
    internal sealed class PipelineMetricRecorder : IDisposable
    {
        private readonly string _pipelineName;
        private readonly MeterListener _listener = new();
        private readonly List<(
            string Name,
            double Value,
            Dictionary<string, object?> Tags
        )> _measurements = [];
        private readonly Lock _gate = new();

        public PipelineMetricRecorder(string pipelineName)
        {
            _pipelineName = pipelineName;
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == PipelineDiagnostics.MeterName)
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<double>(
                (instrument, value, tags, _) => Record(instrument, value, tags)
            );
            _listener.SetMeasurementEventCallback<long>(
                (instrument, value, tags, _) => Record(instrument, value, tags)
            );
            _listener.Start();
        }

        public List<(string Name, double Value, Dictionary<string, object?> Tags)> Measurements(
            string instrumentName
        )
        {
            lock (_gate)
            {
                return _measurements.Where(m => m.Name == instrumentName).ToList();
            }
        }

        public void Dispose() => _listener.Dispose();

        private void Record(
            Instrument instrument,
            double value,
            ReadOnlySpan<KeyValuePair<string, object?>> tags
        )
        {
            var captured = new Dictionary<string, object?>(StringComparer.Ordinal);
            foreach (var tag in tags)
            {
                captured[tag.Key] = tag.Value;
            }

            if (
                captured.TryGetValue("hostloom.pipeline.name", out var name)
                && (name as string) == _pipelineName
            )
            {
                lock (_gate)
                {
                    _measurements.Add((instrument.Name, value, captured));
                }
            }
        }
    }
}
