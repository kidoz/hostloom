using HostLoom.Pipelines;
using HostLoom.Pipelines.DependencyInjection;
using HostLoom.Pipelines.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Tests;

public sealed class PipelineTestingTests
{
    [Fact]
    public async Task Capture_pipe_lets_a_filter_be_tested_without_a_pipeline()
    {
        var next = new CapturePipe<HarnessContext>();
        var log = new ExecutionLog();
        var filter = new RecordingFilter<HarnessContext>("solo", log);
        var context = new HarnessContext();

        await filter.SendAsync(context, next);

        Assert.True(next.WasSent);
        Assert.Same(context, Assert.Single(next.Sent));
        Assert.Equal(["solo"], log.Entries);
    }

    [Fact]
    public async Task Capture_pipe_injects_a_downstream_fault()
    {
        var next = new CapturePipe<HarnessContext>
        {
            Fault = new InvalidOperationException("downstream"),
        };
        var log = new ExecutionLog();
        var filter = new RecordingFilter<HarnessContext>("solo", log);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await filter.SendAsync(new HarnessContext(), next)
        );

        Assert.Equal("downstream", exception.Message);
    }

    [Fact]
    public async Task Pipe_harness_captures_success_and_failure_instead_of_throwing()
    {
        var log = new ExecutionLog();
        var harness = PipeHarness.For<HarnessContext>(builder =>
        {
            builder.UseRetry(RetryPolicy.Immediate(1));
            builder.Use(new FaultFilter<HarnessContext>(failures: 1));
            builder.Use(new RecordingFilter<HarnessContext>("tail", log));
        });

        var recovered = await harness.SendAsync(new HarnessContext());
        Assert.True(recovered.Completed);
        Assert.Equal(["tail"], log.Entries);

        var failing = PipeHarness.For<HarnessContext>(builder =>
            builder.Use(new FaultFilter<HarnessContext>(failures: 1))
        );
        var faulted = await failing.SendAsync(new HarnessContext());
        Assert.False(faulted.Completed);
        Assert.IsType<InvalidOperationException>(faulted.Exception);
    }

    [Fact]
    public async Task Pipeline_harness_runs_a_registered_pipeline_against_fakes()
    {
        var log = new ExecutionLog();
        await using var harness = await PipelineHarness.CreateAsync<HarnessContext>(
            "profiled",
            services =>
            {
                services.AddSingleton(log);
                services.AddPipeline<HarnessContext>(
                    "profiled",
                    pipeline => pipeline.Stage("enrich", stage => stage.AddFilter<LoggingFilter>())
                );
            },
            TestContext.Current.CancellationToken
        );

        var result = await harness.RunAsync(new HarnessContext());

        Assert.True(result.Completed);
        Assert.Equal(["ran"], log.Entries);
        Assert.Equal("enrich[LoggingFilter]", harness.Topology.Describe());
    }

    [Fact]
    public async Task Pipeline_harness_surfaces_validation_failures_like_host_startup()
    {
        // LoggingFilter needs an ExecutionLog that is deliberately not registered.
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await PipelineHarness.CreateAsync<HarnessContext>(
                "broken",
                services =>
                    services.AddPipeline<HarnessContext>(
                        "broken",
                        pipeline =>
                            pipeline.Stage("enrich", stage => stage.AddFilter<LoggingFilter>())
                    ),
                TestContext.Current.CancellationToken
            )
        );

        Assert.Contains("broken", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Pipeline_validation_disposes_async_only_filters()
    {
        var disposals = new Disposals();
        await using var harness = await PipelineHarness.CreateAsync<HarnessContext>(
            "async-disposal",
            services =>
            {
                services.AddSingleton(disposals);
                services.AddPipeline<HarnessContext>(
                    "async-disposal",
                    pipeline =>
                        pipeline.Stage("only", stage => stage.AddFilter<AsyncDisposableFilter>())
                );
            },
            TestContext.Current.CancellationToken
        );

        Assert.Equal(1, disposals.Count);
        var result = await harness.RunAsync(new HarnessContext());
        Assert.True(result.Completed);
        Assert.Equal(2, disposals.Count);
    }

    public sealed class HarnessContext : PipeContext;

    public sealed class LoggingFilter(ExecutionLog log) : IFilter<HarnessContext>
    {
        public ValueTask SendAsync(HarnessContext context, IPipe<HarnessContext> next)
        {
            log.Record("ran");
            return next.SendAsync(context);
        }
    }

    public sealed class Disposals
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public void Record() => Interlocked.Increment(ref _count);
    }

    public sealed class AsyncDisposableFilter(Disposals disposals)
        : IFilter<HarnessContext>,
            IAsyncDisposable
    {
        public ValueTask SendAsync(HarnessContext context, IPipe<HarnessContext> next) =>
            next.SendAsync(context);

        public ValueTask DisposeAsync()
        {
            disposals.Record();
            return ValueTask.CompletedTask;
        }
    }
}
