using HostLoom.Pipelines;
using Xunit;

namespace HostLoom.Tests;

public sealed class PipelineTests
{
    [Fact]
    public async Task Filters_are_composed_in_registration_order()
    {
        var calls = new List<string>();
        var pipe = Pipe.Create<TestContext>(builder =>
        {
            builder.Use(
                async (context, next) =>
                {
                    calls.Add("first-before");
                    await next.SendAsync(context);
                    calls.Add("first-after");
                },
                "first"
            );
            builder.Use(
                async (context, next) =>
                {
                    calls.Add("second-before");
                    await next.SendAsync(context);
                    calls.Add("second-after");
                },
                "second"
            );
        });

        await pipe.SendAsync(new TestContext());
        Assert.Equal(["first-before", "second-before", "second-after", "first-after"], calls);
    }

    [Fact]
    public async Task Terminal_filter_short_circuits_the_remaining_pipeline()
    {
        var calls = new List<string>();
        var pipe = Pipe.Create<TestContext>(builder =>
        {
            builder.UseTerminal(_ =>
            {
                calls.Add("terminal");
                return ValueTask.CompletedTask;
            });
            builder.UseExecute(_ =>
            {
                calls.Add("unreachable");
                return ValueTask.CompletedTask;
            });
        });

        await pipe.SendAsync(new TestContext());
        Assert.Equal(["terminal"], calls);
    }

    [Fact]
    public async Task Conditional_branch_rejoins_the_main_pipeline()
    {
        var calls = new List<string>();
        var pipe = Pipe.Create<TestContext>(builder =>
        {
            builder.UseWhen(
                context => context.Enabled,
                branch =>
                    branch.UseExecute(_ =>
                    {
                        calls.Add("conditional");
                        return ValueTask.CompletedTask;
                    })
            );
            builder.UseExecute(_ =>
            {
                calls.Add("tail");
                return ValueTask.CompletedTask;
            });
        });

        await pipe.SendAsync(new TestContext { Enabled = true });
        Assert.Equal(["conditional", "tail"], calls);
    }

    [Fact]
    public void Payloads_are_lazy_atomic_and_resolvable_by_interface()
    {
        var context = new TestContext();
        var factoryCalls = 0;
        Parallel.For(
            0,
            32,
            _ =>
                context.GetOrAddPayload(() =>
                {
                    Interlocked.Increment(ref factoryCalls);
                    return new Marker("created");
                })
        );

        Assert.Equal(1, factoryCalls);
        Assert.True(context.TryGetPayload<IMarker>(out var marker));
        Assert.Equal("created", marker!.Value);
        var updated = context.AddOrUpdatePayload(
            () => new Marker("added"),
            existing => existing with { Value = "updated" }
        );
        Assert.Equal("updated", updated.Value);
    }

    [Fact]
    public void Probe_describes_pipeline_without_executing_it()
    {
        var pipe = Pipe.Create<TestContext>(builder =>
        {
            builder.UseExecute(_ => ValueTask.CompletedTask, "metrics");
            builder.UseConcurrencyLimit(4);
        });

        // Qualified: the nested TestContext below shadows Xunit.TestContext in this class.
        var probe = PipelineProbe.Inspect(pipe, Xunit.TestContext.Current.CancellationToken);
        Assert.Equal(["metrics", "concurrencyLimit", "empty"], probe.Children.Select(x => x.Name));
        Assert.Equal(4, probe.Children[1].Properties["limit"]);
    }

    [Fact]
    public void Probe_reports_the_type_name_when_a_filter_does_not_describe_itself()
    {
        var pipe = Pipe.Create<TestContext>(builder => builder.Use(new SilentFilter()));

        var probe = PipelineProbe.Inspect(pipe, Xunit.TestContext.Current.CancellationToken);

        Assert.Equal(["SilentFilter", "empty"], probe.Children.Select(x => x.Name));
    }

    [Fact]
    public async Task Cancellation_is_observed_before_a_filter_executes()
    {
        using var source = new CancellationTokenSource();
        await source.CancelAsync();
        var executed = false;
        var pipe = Pipe.Create<TestContext>(builder =>
            builder.UseExecute(_ =>
            {
                executed = true;
                return ValueTask.CompletedTask;
            })
        );

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await pipe.SendAsync(new TestContext(source.Token))
        );
        Assert.False(executed);
    }

    private sealed class TestContext(CancellationToken cancellationToken = default)
        : PipeContext(cancellationToken)
    {
        public bool Enabled { get; init; }
    }

    private interface IMarker
    {
        string Value { get; }
    }

    private sealed record Marker(string Value) : IMarker;

    /// <summary>Implements no Probe of its own, so the default implementation must describe it.</summary>
    private sealed class SilentFilter : IFilter<TestContext>
    {
        public ValueTask SendAsync(TestContext context, IPipe<TestContext> next) =>
            next.SendAsync(context);
    }
}
