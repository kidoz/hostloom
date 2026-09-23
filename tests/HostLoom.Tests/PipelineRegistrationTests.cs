using System.Collections.Concurrent;
using System.Diagnostics;
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
    public async Task Application_registrations_cannot_override_pipeline_filter_lifetime()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new ExecutionLog());
        services.AddSingleton<CountingFilter>();
        var constructions = new Constructions();
        services.AddSingleton(constructions);
        services.AddPipeline<RunContext>(
            "isolated",
            pipeline => pipeline.Stage("only", stage => stage.AddFilter<CountingFilter>())
        );
        services.AddSingleton<CountingFilter>();
        await using var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredKeyedService<IPipelineRunner<RunContext>>("isolated");

        await runner.RunAsync(new RunContext());
        await runner.RunAsync(new RunContext());

        Assert.Equal(2, constructions.Count);
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
    public async Task Startup_validation_skips_a_filter_that_is_switched_off()
    {
        var builder = Host.CreateApplicationBuilder();
        // ExecutionLog is deliberately not registered, exactly as in the failing case above. The
        // difference is that this filter is switched off, so the runner would never construct it —
        // and refusing to start over a filter that never executes defeats the point of the switch.
        builder.Services.AddPipeline<RunContext>(
            "environment-gated",
            pipeline =>
                pipeline.Stage(
                    "only",
                    stage => stage.AddFilter<EnrichFilter>(filter => filter.EnabledWhen(_ => false))
                )
        );
        using var host = builder.Build();

        await host.StartAsync(TestContext.Current.CancellationToken).ConfigureAwait(true);
        await host.StopAsync(TestContext.Current.CancellationToken).ConfigureAwait(true);
    }

    [Fact]
    public async Task Startup_validation_reports_a_constructor_failure_that_is_not_a_missing_service()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(new ExecutionLog());
        builder.Services.AddPipeline<RunContext>(
            "throwing",
            pipeline => pipeline.Stage("only", stage => stage.AddFilter<ThrowingFilter>())
        );
        using var host = builder.Build();

        // The container reports a missing dependency as InvalidOperationException; anything else a
        // constructor throws is the same startup problem and should carry the same guidance rather
        // than escaping raw.
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await host.StartAsync(TestContext.Current.CancellationToken)
        );

        Assert.Contains("throwing", exception.Message, StringComparison.Ordinal);
        Assert.Contains("ThrowingFilter", exception.Message, StringComparison.Ordinal);
        Assert.IsType<NotSupportedException>(exception.InnerException, exactMatch: false);
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
    public async Task With_retry_gives_every_attempt_a_fresh_scope_and_fresh_filters()
    {
        var ledger = new ScopeLedger();
        var attempts = new Attempts(failures: 2);
        var services = ScopedServices(ledger, attempts);
        services.AddPipeline<RunContext>(
            "rescoped",
            pipeline =>
                pipeline
                    .WithRetry(RetryPolicy.Immediate(2))
                    .Stage("only", stage => stage.AddFilter<ScopedFlakyFilter>())
        );
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var runner = provider.GetRequiredKeyedService<IPipelineRunner<RunContext>>("rescoped");

        await runner.RunAsync(new RunContext());

        var seen = attempts.Entries;
        Assert.Equal(3, seen.Count); // two failures, then success
        Assert.Equal(3, seen.Select(attempt => attempt.Work).Distinct().Count());
        Assert.Equal(3, seen.Select(attempt => attempt.Filter).Distinct().Count());
        // Each failed attempt's scope is disposed before the retry opens the next one.
        Assert.Equal(
            ["open 1", "close 1", "open 2", "close 2", "open 3", "close 3"],
            ledger.Events
        );
    }

    [Fact]
    public async Task Enabled_when_is_evaluated_per_attempt_against_that_attempts_scope()
    {
        var log = new ExecutionLog();
        var attempts = new Attempts(failures: 1);
        var evaluatedIn = new ConcurrentQueue<ScopedWork>();
        var services = ScopedServices(new ScopeLedger(), attempts);
        services.AddSingleton(log);
        services.AddPipeline<RunContext>(
            "retoggled",
            pipeline =>
                pipeline
                    .WithRetry(RetryPolicy.Immediate(1))
                    .Stage(
                        "only",
                        stage =>
                            stage
                                .AddFilter<EnrichFilter>(filter =>
                                    filter.EnabledWhen(sp =>
                                    {
                                        var work = sp.GetRequiredService<ScopedWork>();
                                        evaluatedIn.Enqueue(work);
                                        // Off for the first attempt only, as if a flag flipped mid-run.
                                        return work.Id > 1;
                                    })
                                )
                                .AddFilter<ScopedFlakyFilter>()
                    )
        );
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var runner = provider.GetRequiredKeyedService<IPipelineRunner<RunContext>>("retoggled");

        await runner.RunAsync(new RunContext());

        // Evaluated once per attempt, each time against the scope that attempt's filters use.
        Assert.Equal(2, evaluatedIn.Count);
        Assert.Equal(attempts.Entries.Select(attempt => attempt.Work), evaluatedIn);
        // Left out of the failed first attempt, composed into the retry.
        Assert.Equal(["enrich"], log.Entries);
    }

    [Fact]
    public async Task A_run_without_retry_opens_one_scope_shared_by_all_its_filters()
    {
        var ledger = new ScopeLedger();
        var attempts = new Attempts(failures: 0);
        var services = ScopedServices(ledger, attempts);
        services.AddPipeline<RunContext>(
            "single-scope",
            pipeline =>
                pipeline
                    .Stage(
                        "first",
                        stage => stage.AddFilter<ScopedFlakyFilter>(filter => filter.WithName("a"))
                    )
                    .Stage(
                        "second",
                        stage => stage.AddFilter<ScopedFlakyFilter>(filter => filter.WithName("b"))
                    )
        );
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var runner = provider.GetRequiredKeyedService<IPipelineRunner<RunContext>>("single-scope");

        await runner.RunAsync(new RunContext());

        Assert.Equal(2, attempts.Entries.Count);
        Assert.Single(attempts.Entries.Select(attempt => attempt.Work).Distinct());
        Assert.Equal(["open 1", "close 1"], ledger.Events);
    }

    [Fact]
    public async Task A_timeout_declared_after_the_retry_bounds_each_attempt_in_its_own_scope()
    {
        var clock = new TestClock();
        var ledger = new ScopeLedger();
        var attempts = new Attempts(failures: 1);
        var services = ScopedServices(ledger, attempts);
        services.AddSingleton(clock);
        services.AddPipeline<RunContext>(
            "attempt-bounded",
            pipeline =>
                pipeline
                    .WithRetry(RetryPolicy.Immediate(1))
                    .WithTimeout(TimeSpan.FromMinutes(5), clock)
                    .Stage("only", stage => stage.AddFilter<ScopedStallingFilter>())
        );
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var runner = provider.GetRequiredKeyedService<IPipelineRunner<RunContext>>(
            "attempt-bounded"
        );

        await runner.RunAsync(new RunContext());

        // The first attempt overran its deadline; the retry ran with a fresh deadline and scope.
        Assert.Equal(2, attempts.Entries.Count);
        Assert.False(attempts.Entries[1].Canceled);
        Assert.Equal(["open 1", "close 1", "open 2", "close 2"], ledger.Events);
    }

    [Fact]
    public async Task A_timeout_declared_before_the_retry_budgets_all_attempts_and_disposes_the_scope()
    {
        var clock = new TestClock();
        var ledger = new ScopeLedger();
        var attempts = new Attempts(failures: 1);
        var services = ScopedServices(ledger, attempts);
        services.AddSingleton(clock);
        services.AddPipeline<RunContext>(
            "run-bounded",
            pipeline =>
                pipeline
                    .WithTimeout(TimeSpan.FromMinutes(5), clock)
                    .WithRetry(RetryPolicy.Immediate(3))
                    .Stage("only", stage => stage.AddFilter<ScopedStallingFilter>())
        );
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var runner = provider.GetRequiredKeyedService<IPipelineRunner<RunContext>>("run-bounded");

        await Assert.ThrowsAsync<PipelineTimeoutException>(async () =>
            await runner.RunAsync(new RunContext())
        );

        // The spent budget cancels the context, and cancellation is never retried.
        Assert.Single(attempts.Entries);
        Assert.Equal(["open 1", "close 1"], ledger.Events);
    }

    [Fact]
    public async Task A_cancelled_attempt_disposes_its_scope_and_is_not_retried()
    {
        using var cancellation = new CancellationTokenSource();
        var ledger = new ScopeLedger();
        var attempts = new Attempts(failures: 0);
        var services = ScopedServices(ledger, attempts);
        services.AddSingleton(cancellation);
        services.AddPipeline<RunContext>(
            "cancelled",
            pipeline =>
                pipeline
                    .WithRetry(RetryPolicy.Immediate(3))
                    .Stage("only", stage => stage.AddFilter<ScopedCancellingFilter>())
        );
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var runner = provider.GetRequiredKeyedService<IPipelineRunner<RunContext>>("cancelled");

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await runner.RunAsync(new RunContext(cancellation.Token))
        );

        Assert.Single(attempts.Entries);
        Assert.Equal(["open 1", "close 1"], ledger.Events);
    }

    [Theory]
    [InlineData(2, "success")]
    [InlineData(1, "failure")]
    public async Task A_retried_run_records_one_run_with_its_final_outcome(
        int retryLimit,
        string outcome
    )
    {
        var name = $"retried-{outcome}";
        using var metrics = new InstrumentedFilterTests.PipelineMetricRecorder(name);
        using var traces = new PipelineActivityRecorder(name);
        var services = ScopedServices(new ScopeLedger(), new Attempts(failures: 2));
        services.AddPipeline<RunContext>(
            name,
            pipeline =>
                pipeline
                    .WithRetry(RetryPolicy.Immediate(retryLimit))
                    .Stage("only", stage => stage.AddFilter<ScopedFlakyFilter>())
        );
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var runner = provider.GetRequiredKeyedService<IPipelineRunner<RunContext>>(name);

        var fault = await Record.ExceptionAsync(async () =>
            await runner.RunAsync(new RunContext())
        );

        Assert.Equal(outcome == "failure", fault is InvalidOperationException);
        var run = Assert.Single(metrics.Measurements("hostloom.pipeline.run.duration"));
        Assert.Equal(outcome, run.Tags["hostloom.pipeline.outcome"]);
        Assert.Equal(
            [1, -1],
            metrics.Measurements("hostloom.pipeline.run.active").Select(m => m.Value)
        );
        // Filters are instrumented per attempt, so each attempt reports its own filter outcome.
        var filterRuns = metrics.Measurements("hostloom.pipeline.filter.duration");
        Assert.Equal(retryLimit + 1, filterRuns.Count);
        Assert.Equal(outcome, filterRuns[^1].Tags["hostloom.pipeline.outcome"]);
        Assert.Equal(2, metrics.Measurements("hostloom.pipeline.filter.failures").Count);

        var runSpan = Assert.Single(
            traces.Stopped,
            activity => activity.OperationName == "hostloom pipeline run"
        );
        var filterSpans = traces
            .Stopped.Where(activity => activity.OperationName == "hostloom pipeline filter")
            .ToList();
        Assert.Equal(retryLimit + 1, filterSpans.Count);
        Assert.All(filterSpans, span => Assert.Equal(runSpan.SpanId, span.ParentSpanId));
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

    public sealed class RunContext(CancellationToken cancellationToken = default)
        : PipeContext(cancellationToken);

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

    /// <summary>Fails construction with something other than a missing-service error.</summary>
    public sealed class ThrowingFilter : IFilter<RunContext>
    {
        public ThrowingFilter() =>
            throw new NotSupportedException("this filter cannot be constructed");

        public ValueTask SendAsync(RunContext context, IPipe<RunContext> next) =>
            next.SendAsync(context);
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

    private static ServiceCollection ScopedServices(ScopeLedger ledger, Attempts attempts)
    {
        var services = new ServiceCollection();
        services.AddSingleton(ledger);
        services.AddSingleton(attempts);
        services.AddScoped<ScopedWork>();
        return services;
    }

    /// <summary>Records, in order, each <see cref="ScopedWork"/> a scope creates and disposes.</summary>
    public sealed class ScopeLedger
    {
        private readonly ExecutionLog _events = new();
        private int _opened;

        public IReadOnlyList<string> Events => _events.Entries;

        public int Open()
        {
            var id = Interlocked.Increment(ref _opened);
            _events.Record($"open {id}");
            return id;
        }

        public void Close(int id) => _events.Record($"close {id}");
    }

    /// <summary>A scoped dependency standing in for a unit of work: one instance per scope.</summary>
    public sealed class ScopedWork : IAsyncDisposable
    {
        private readonly ScopeLedger _ledger;

        public ScopedWork(ScopeLedger ledger)
        {
            _ledger = ledger;
            Id = ledger.Open();
        }

        public int Id { get; }

        public ValueTask DisposeAsync()
        {
            _ledger.Close(Id);
            return ValueTask.CompletedTask;
        }
    }

    public sealed record Attempt(object Filter, ScopedWork Work, bool Canceled);

    /// <summary>Records every filter invocation and asks the first <c>failures</c> of them to fail.</summary>
    public sealed class Attempts(int failures)
    {
        private readonly ConcurrentQueue<Attempt> _entries = new();
        private int _count;

        public IReadOnlyList<Attempt> Entries => _entries.ToArray();

        public bool Enter(object filter, ScopedWork work, IPipeContext context)
        {
            _entries.Enqueue(
                new Attempt(filter, work, context.CancellationToken.IsCancellationRequested)
            );
            return Interlocked.Increment(ref _count) <= failures;
        }
    }

    public sealed class ScopedFlakyFilter(ScopedWork work, Attempts attempts) : IFilter<RunContext>
    {
        public ValueTask SendAsync(RunContext context, IPipe<RunContext> next) =>
            attempts.Enter(this, work, context)
                ? ValueTask.FromException(new InvalidOperationException("transient"))
                : next.SendAsync(context);
    }

    /// <summary>Overruns the deadline on a failing attempt; the test clock fires it synchronously.</summary>
    internal sealed class ScopedStallingFilter(TestClock clock, ScopedWork work, Attempts attempts)
        : IFilter<RunContext>
    {
        public ValueTask SendAsync(RunContext context, IPipe<RunContext> next)
        {
            if (!attempts.Enter(this, work, context))
            {
                return next.SendAsync(context);
            }

            clock.Advance(TimeSpan.FromMinutes(6));
            return context.CancellationToken.IsCancellationRequested
                ? ValueTask.FromCanceled(context.CancellationToken)
                : ValueTask.FromException(
                    new InvalidOperationException("The deadline never reached the filter.")
                );
        }
    }

    /// <summary>Cancels the caller's token mid-attempt, as a stopping host would.</summary>
    public sealed class ScopedCancellingFilter(
        CancellationTokenSource cancellation,
        ScopedWork work,
        Attempts attempts
    ) : IFilter<RunContext>
    {
        public async ValueTask SendAsync(RunContext context, IPipe<RunContext> next)
        {
            _ = attempts.Enter(this, work, context);
            await cancellation.CancelAsync();
            context.CancellationToken.ThrowIfCancellationRequested();
            await next.SendAsync(context);
        }
    }

    /// <summary>
    /// Collects stopped pipeline spans for one pipeline name. The listener is process-wide, so
    /// filtering by pipeline keeps concurrently running tests from contaminating each other.
    /// </summary>
    private sealed class PipelineActivityRecorder : IDisposable
    {
        private readonly Lock _gate = new();
        private readonly List<Activity> _stopped = [];
        private readonly ActivityListener _listener;

        public PipelineActivityRecorder(string pipelineName)
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == PipelineDiagnostics.ActivitySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                    ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity =>
                {
                    if (activity.GetTagItem("hostloom.pipeline.name") as string == pipelineName)
                    {
                        lock (_gate)
                        {
                            _stopped.Add(activity);
                        }
                    }
                },
            };
            ActivitySource.AddActivityListener(_listener);
        }

        public IReadOnlyList<Activity> Stopped
        {
            get
            {
                lock (_gate)
                {
                    return [.. _stopped];
                }
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
