using HostLoom.Pipelines;
using HostLoom.Pipelines.DependencyInjection;
using HostLoom.Pipelines.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.Tests;

public sealed class PipelineRegistrationTests
{
    [Fact]
    public async Task Filters_run_in_stage_then_registration_order_with_constructor_injection()
    {
        var log = new ExecutionLog();
        var services = new ServiceCollection();
        services.AddSingleton(log);
        services.AddPipeline<RunContext>(
            "profiling",
            pipeline =>
                pipeline
                    .Stage(
                        "enrich",
                        stage => stage.AddFilter<EnrichFilter>().AddFilter<SecondEnrichFilter>()
                    )
                    .Stage("score", stage => stage.AddFilter<ScoreFilter>())
        );
        await using var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredKeyedService<IPipelineRunner<RunContext>>("profiling");
        await runner.RunAsync(new RunContext());

        Assert.Equal(["enrich", "enrich-second", "score"], log.Entries);
        Assert.Equal("profiling", runner.PipelineName);
    }

    [Fact]
    public async Task Enabled_when_is_evaluated_once_per_run()
    {
        var log = new ExecutionLog();
        var toggle = new Toggle { Enabled = false };
        var services = new ServiceCollection();
        services.AddSingleton(log);
        services.AddSingleton(toggle);
        services.AddPipeline<RunContext>(
            "toggled",
            pipeline =>
                pipeline.Stage(
                    "enrich",
                    stage =>
                        stage
                            .AddFilter<EnrichFilter>(filter =>
                                filter.EnabledWhen(sp => sp.GetRequiredService<Toggle>().Enabled)
                            )
                            .AddFilter<ScoreFilter>()
                )
        );
        await using var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredKeyedService<IPipelineRunner<RunContext>>("toggled");

        await runner.RunAsync(new RunContext());
        toggle.Enabled = true;
        await runner.RunAsync(new RunContext());

        // First run skipped the disabled filter; the flip was picked up without re-registration.
        Assert.Equal(["score", "enrich", "score"], log.Entries);
    }

    [Fact]
    public async Task Every_run_resolves_fresh_transient_filters_in_its_own_scope()
    {
        var counter = new Constructions();
        var services = new ServiceCollection();
        services.AddSingleton(counter);
        services.AddSingleton(new ExecutionLog());
        services.AddPipeline<RunContext>(
            "fresh",
            pipeline => pipeline.Stage("only", stage => stage.AddFilter<CountingFilter>())
        );
        await using var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredKeyedService<IPipelineRunner<RunContext>>("fresh");

        await runner.RunAsync(new RunContext());
        await runner.RunAsync(new RunContext());

        Assert.Equal(2, counter.Count);
    }

    [Fact]
    public void A_filter_already_registered_with_another_lifetime_is_rejected()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new ExecutionLog());
        services.AddSingleton<CountingFilter>();
        services.AddSingleton(new Constructions());

        // Silently honouring the singleton would share one filter across concurrent runs and let it
        // capture scoped dependencies, both of which contradict the documented per-run lifetime.
        var exception = Assert.Throws<InvalidOperationException>(() =>
            services.AddPipeline<RunContext>(
                "conflicting",
                pipeline => pipeline.Stage("only", stage => stage.AddFilter<CountingFilter>())
            )
        );

        Assert.Contains("Singleton", exception.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(CountingFilter), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_filter_already_registered_as_transient_is_accepted()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new ExecutionLog());
        services.AddSingleton(new Constructions());
        services.AddTransient<CountingFilter>();

        services.AddPipeline<RunContext>(
            "compatible",
            pipeline => pipeline.Stage("only", stage => stage.AddFilter<CountingFilter>())
        );

        Assert.Single(services, descriptor => descriptor.ServiceType == typeof(CountingFilter));
    }

    [Fact]
    public async Task Unkeyed_resolution_requires_exactly_one_pipeline_for_the_context()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new ExecutionLog());
        services.AddPipeline<RunContext>(
            "first",
            pipeline => pipeline.Stage("only", stage => stage.AddFilter<EnrichFilter>())
        );
        services.AddPipeline<RunContext>(
            "second",
            pipeline => pipeline.Stage("only", stage => stage.AddFilter<ScoreFilter>())
        );
        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            provider.GetRequiredService<IPipelineRunner<RunContext>>()
        );

        Assert.Contains("first", exception.Message, StringComparison.Ordinal);
        Assert.Contains("second", exception.Message, StringComparison.Ordinal);
        Assert.NotNull(provider.GetRequiredKeyedService<IPipelineRunner<RunContext>>("second"));
    }

    [Fact]
    public void A_duplicate_pipeline_name_fails_at_registration()
    {
        var services = new ServiceCollection();
        services.AddPipeline<RunContext>(
            "dup",
            pipeline => pipeline.Stage("only", stage => stage.AddFilter<EnrichFilter>())
        );

        Assert.Throws<InvalidOperationException>(() =>
            services.AddPipeline<RunContext>(
                "dup",
                pipeline => pipeline.Stage("only", stage => stage.AddFilter<ScoreFilter>())
            )
        );
    }

    [Fact]
    public void Duplicate_filter_names_fail_at_registration()
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services.AddPipeline<RunContext>(
                "clash",
                pipeline =>
                    pipeline
                        .Stage(
                            "a",
                            stage =>
                                stage.AddFilter<EnrichFilter>(filter => filter.WithName("same"))
                        )
                        .Stage(
                            "b",
                            stage => stage.AddFilter<ScoreFilter>(filter => filter.WithName("same"))
                        )
            )
        );

        Assert.Contains("same", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Startup_validation_fails_the_host_when_a_filter_dependency_is_missing()
    {
        var builder = Host.CreateApplicationBuilder();
        // ExecutionLog is deliberately not registered, so EnrichFilter cannot be constructed.
        builder.Services.AddPipeline<RunContext>(
            "broken",
            pipeline => pipeline.Stage("only", stage => stage.AddFilter<EnrichFilter>())
        );
        using var host = builder.Build();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await host.StartAsync(TestContext.Current.CancellationToken)
        );

        Assert.Contains("broken", exception.Message, StringComparison.Ordinal);
        Assert.Contains("EnrichFilter", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Topology_reports_stages_filters_and_conditional_flags()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new ExecutionLog());
        services.AddPipeline<RunContext>(
            "shape",
            pipeline =>
                pipeline
                    .Stage(
                        "enrich",
                        stage =>
                            stage
                                .AddFilter<EnrichFilter>()
                                .AddFilter<SecondEnrichFilter>(filter =>
                                    filter.WithName("optional").EnabledWhen(_ => true)
                                )
                    )
                    .Stage("score", stage => stage.AddFilter<ScoreFilter>())
        );
        using var provider = services.BuildServiceProvider();

        var topology = provider
            .GetRequiredKeyedService<IPipelineRunner<RunContext>>("shape")
            .Topology;

        Assert.Equal("enrich[EnrichFilter, optional?] -> score[ScoreFilter]", topology.Describe());
        Assert.Equal(typeof(SecondEnrichFilter), topology.Stages[0].Filters[1].FilterType);
    }

    [Fact]
    public async Task With_retry_reruns_the_whole_pipeline()
    {
        var log = new ExecutionLog();
        var attempts = new Constructions();
        var services = new ServiceCollection();
        services.AddSingleton(log);
        services.AddSingleton(attempts);
        services.AddPipeline<RunContext>(
            "retried",
            pipeline =>
                pipeline
                    .WithRetry(RetryPolicy.Immediate(2))
                    .Stage("only", stage => stage.AddFilter<FlakyFilter>())
        );
        await using var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredKeyedService<IPipelineRunner<RunContext>>("retried");

        await runner.RunAsync(new RunContext());

        Assert.Equal(3, attempts.Count); // two failures, then success
    }

    [Fact]
    public async Task With_timeout_bounds_the_whole_run()
    {
        var clock = new TestClock();
        var services = new ServiceCollection();
        services.AddSingleton(clock);
        services.AddPipeline<RunContext>(
            "bounded",
            pipeline =>
                pipeline
                    .WithTimeout(TimeSpan.FromMinutes(5), clock)
                    .Stage("only", stage => stage.AddFilter<StallingFilter>())
        );
        await using var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredKeyedService<IPipelineRunner<RunContext>>("bounded");

        await Assert.ThrowsAsync<PipelineTimeoutException>(async () =>
            await runner.RunAsync(new RunContext())
        );
    }

    [Fact]
    public async Task A_run_records_duration_active_count_and_stage_tagged_filter_metrics()
    {
        const string name = "measured";
        using var recorder = new InstrumentedFilterTests.PipelineMetricRecorder(name);
        var services = new ServiceCollection();
        services.AddSingleton(new ExecutionLog());
        services.AddPipeline<RunContext>(
            name,
            pipeline => pipeline.Stage("enrich", stage => stage.AddFilter<EnrichFilter>())
        );
        await using var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredKeyedService<IPipelineRunner<RunContext>>(name);

        await runner.RunAsync(new RunContext());

        Assert.Single(recorder.Measurements("hostloom.pipeline.run.duration"));
        Assert.Equal(
            [1, -1],
            recorder.Measurements("hostloom.pipeline.run.active").Select(m => m.Value)
        );
        var filterDuration = Assert.Single(
            recorder.Measurements("hostloom.pipeline.filter.duration")
        );
        Assert.Equal("enrich", filterDuration.Tags["hostloom.pipeline.stage"]);
        Assert.Equal("EnrichFilter", filterDuration.Tags["hostloom.pipeline.filter"]);
    }

    [Fact]
    public async Task Instrumentation_can_be_disabled_per_pipeline()
    {
        const string name = "silent";
        using var recorder = new InstrumentedFilterTests.PipelineMetricRecorder(name);
        var services = new ServiceCollection();
        services.AddSingleton(new ExecutionLog());
        services.AddPipeline<RunContext>(
            name,
            pipeline =>
                pipeline
                    .WithoutInstrumentation()
                    .Stage("enrich", stage => stage.AddFilter<EnrichFilter>())
        );
        await using var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredKeyedService<IPipelineRunner<RunContext>>(name);

        await runner.RunAsync(new RunContext());

        Assert.Empty(recorder.Measurements("hostloom.pipeline.filter.duration"));
        Assert.Single(recorder.Measurements("hostloom.pipeline.run.duration")); // run metrics stay on
    }

    public sealed class RunContext : PipeContext;

    public sealed class Toggle
    {
        public bool Enabled { get; set; }
    }

    public sealed class Constructions
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public int Next() => Interlocked.Increment(ref _count);
    }

    public sealed class EnrichFilter(ExecutionLog log) : IFilter<RunContext>
    {
        public ValueTask SendAsync(RunContext context, IPipe<RunContext> next)
        {
            log.Record("enrich");
            return next.SendAsync(context);
        }
    }

    public sealed class SecondEnrichFilter(ExecutionLog log) : IFilter<RunContext>
    {
        public ValueTask SendAsync(RunContext context, IPipe<RunContext> next)
        {
            log.Record("enrich-second");
            return next.SendAsync(context);
        }
    }

    public sealed class ScoreFilter(ExecutionLog log) : IFilter<RunContext>
    {
        public ValueTask SendAsync(RunContext context, IPipe<RunContext> next)
        {
            log.Record("score");
            return next.SendAsync(context);
        }
    }

    public sealed class CountingFilter(Constructions constructions) : IFilter<RunContext>
    {
        private readonly int _instance = constructions.Next();

        public ValueTask SendAsync(RunContext context, IPipe<RunContext> next) =>
            _instance > 0 ? next.SendAsync(context) : ValueTask.CompletedTask;
    }

    public sealed class FlakyFilter(Constructions constructions) : IFilter<RunContext>
    {
        public ValueTask SendAsync(RunContext context, IPipe<RunContext> next) =>
            constructions.Next() < 3
                ? ValueTask.FromException(new InvalidOperationException("transient"))
                : next.SendAsync(context);
    }

    internal sealed class StallingFilter(TestClock clock) : IFilter<RunContext>
    {
        public async ValueTask SendAsync(RunContext context, IPipe<RunContext> next)
        {
            clock.Advance(TimeSpan.FromMinutes(6));
            await Task.Delay(Timeout.InfiniteTimeSpan, context.CancellationToken);
        }
    }
}
