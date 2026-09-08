using HostLoom.Pipelines;
using Xunit;

namespace HostLoom.Tests;

public sealed partial class InstrumentedFilterTests
{
    [Theory]
    [InlineData("success")]
    [InlineData("failure")]
    [InlineData("canceled")]
    public async Task Overlapping_downstream_intervals_are_subtracted_once(string outcome)
    {
        string pipeline = "overlapping-" + outcome;
        var clock = new TestClock();
        using var recorder = new PipelineMetricRecorder(pipeline);
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var next = new ControlledPipe(first, second);
        var filter = new InstrumentedFilter<MeteredContext>(
            new OverlappingFilter(clock, first, second, outcome),
            pipeline,
            null,
            "parallel",
            clock
        );

        Exception? error = await Record.ExceptionAsync(() =>
            filter.SendAsync(new MeteredContext(), next).AsTask()
        );

        if (outcome == "success")
            Assert.Null(error);
        else if (outcome == "failure")
            Assert.IsType<InvalidOperationException>(error);
        else
            Assert.IsAssignableFrom<OperationCanceledException>(error);
        var measurement = Assert.Single(recorder.Measurements("hostloom.pipeline.filter.duration"));
        Assert.Equal(5, measurement.Value, precision: 9);
        Assert.Equal(outcome, measurement.Tags["hostloom.pipeline.outcome"]);
    }

    [Fact]
    public async Task Sequential_downstream_intervals_preserve_self_time_between_calls()
    {
        const string pipeline = "sequential-self-time";
        var clock = new TestClock();
        using var recorder = new PipelineMetricRecorder(pipeline);
        var filter = new InstrumentedFilter<MeteredContext>(
            new SequentialFilter(clock),
            pipeline,
            null,
            "sequential",
            clock
        );
        var next = Pipe.Create<MeteredContext>(builder =>
            builder.UseTerminal(_ =>
            {
                clock.Advance(TimeSpan.FromSeconds(10));
                return ValueTask.CompletedTask;
            })
        );

        await filter.SendAsync(new MeteredContext(), next);

        var measurement = Assert.Single(recorder.Measurements("hostloom.pipeline.filter.duration"));
        Assert.Equal(5, measurement.Value, precision: 9);
    }

    private sealed class ControlledPipe(TaskCompletionSource first, TaskCompletionSource second)
        : IPipe<MeteredContext>
    {
        private int _calls;

        public ValueTask SendAsync(MeteredContext context) =>
            new(
                (Interlocked.Increment(ref _calls) == 1 ? first : second).Task.WaitAsync(
                    TimeSpan.FromSeconds(5),
                    TestContext.Current.CancellationToken
                )
            );
    }

    private sealed class OverlappingFilter(
        TestClock clock,
        TaskCompletionSource first,
        TaskCompletionSource second,
        string outcome
    ) : IFilter<MeteredContext>
    {
        public async ValueTask SendAsync(MeteredContext context, IPipe<MeteredContext> next)
        {
            clock.Advance(TimeSpan.FromSeconds(2));
            Task a = next.SendAsync(new MeteredContext()).AsTask();
            clock.Advance(TimeSpan.FromSeconds(2));
            Task b = next.SendAsync(new MeteredContext()).AsTask();
            clock.Advance(TimeSpan.FromSeconds(8));
            first.SetResult();
            await a;
            clock.Advance(TimeSpan.FromSeconds(2));
            if (outcome == "failure")
                second.SetException(new InvalidOperationException("injected"));
            else if (outcome == "canceled")
                second.SetCanceled();
            else
                second.SetResult();
            try
            {
                await b;
            }
            finally
            {
                clock.Advance(TimeSpan.FromSeconds(3));
            }
        }
    }

    private sealed class SequentialFilter(TestClock clock) : IFilter<MeteredContext>
    {
        public async ValueTask SendAsync(MeteredContext context, IPipe<MeteredContext> next)
        {
            clock.Advance(TimeSpan.FromSeconds(2));
            await next.SendAsync(new MeteredContext());
            clock.Advance(TimeSpan.FromSeconds(3));
            await next.SendAsync(new MeteredContext());
        }
    }
}
