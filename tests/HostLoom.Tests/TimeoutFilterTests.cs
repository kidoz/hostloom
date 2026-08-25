using HostLoom.Pipelines;
using Xunit;

namespace HostLoom.Tests;

public sealed class TimeoutFilterTests
{
    [Fact]
    public async Task A_run_over_budget_fails_with_a_pipeline_timeout()
    {
        var clock = new TestClock();
        var pipe = Pipe.Create<TimedContext>(builder =>
        {
            builder.UseTimeout(TimeSpan.FromSeconds(5), clock);
            builder.UseExecute(async context =>
            {
                clock.Advance(TimeSpan.FromSeconds(6));
                await Task.Delay(Timeout.InfiniteTimeSpan, context.CancellationToken);
            });
        });

        var exception = await Assert.ThrowsAsync<PipelineTimeoutException>(async () =>
            await pipe.SendAsync(new TimedContext())
        );

        Assert.Equal(TimeSpan.FromSeconds(5), exception.Timeout);
        Assert.IsAssignableFrom<OperationCanceledException>(exception.InnerException);
    }

    [Fact]
    public async Task A_terminal_filter_that_ignores_the_deadline_still_fails_the_run()
    {
        var clock = new TestClock();
        var pipe = Pipe.Create<TimedContext>(builder =>
        {
            builder.UseTimeout(TimeSpan.FromSeconds(5), clock);
            // Terminal: nothing runs after it, so the pipe's between-stage token check never fires.
            builder.UseTerminal(_ =>
            {
                clock.Advance(TimeSpan.FromSeconds(60));
                return ValueTask.CompletedTask;
            });
        });

        var exception = await Assert.ThrowsAsync<PipelineTimeoutException>(async () =>
            await pipe.SendAsync(new TimedContext())
        );

        Assert.Equal(TimeSpan.FromSeconds(5), exception.Timeout);
    }

    [Fact]
    public async Task A_run_inside_the_budget_is_not_reported_as_a_timeout()
    {
        var clock = new TestClock();
        var pipe = Pipe.Create<TimedContext>(builder =>
        {
            builder.UseTimeout(TimeSpan.FromSeconds(5), clock);
            builder.UseTerminal(_ =>
            {
                clock.Advance(TimeSpan.FromSeconds(1));
                return ValueTask.CompletedTask;
            });
        });

        await pipe.SendAsync(new TimedContext());
    }

    [Fact]
    public async Task Downstream_filters_observe_the_deadline_through_the_context_token()
    {
        var clock = new TestClock();
        var observed = CancellationToken.None;
        var context = new TimedContext();
        var pipe = Pipe.Create<TimedContext>(builder =>
        {
            builder.UseTimeout(TimeSpan.FromSeconds(5), clock);
            builder.UseExecute(ctx =>
            {
                observed = ctx.CancellationToken;
                return ValueTask.CompletedTask;
            });
        });

        await pipe.SendAsync(context);

        // The caller passed no token, so a cancellable one can only be the timeout's linked token.
        Assert.True(observed.CanBeCanceled);
        Assert.False(context.CancellationToken.CanBeCanceled); // restored after the send
    }

    [Fact]
    public async Task Caller_cancellation_stays_cancellation_even_inside_the_timeout()
    {
        var clock = new TestClock();
        using var source = new CancellationTokenSource();
        var pipe = Pipe.Create<TimedContext>(builder =>
        {
            builder.UseTimeout(TimeSpan.FromSeconds(5), clock);
            builder.UseExecute(async ctx =>
            {
                await source.CancelAsync();
                ctx.CancellationToken.ThrowIfCancellationRequested();
            });
        });

        // PipelineTimeoutException is not an OperationCanceledException, so this assertion
        // fails if caller cancellation were ever misreported as a timeout.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await pipe.SendAsync(new TimedContext(source.Token))
        );
    }

    [Fact]
    public async Task The_original_token_is_restored_after_a_timeout()
    {
        var clock = new TestClock();
        var context = new TimedContext();
        var pipe = Pipe.Create<TimedContext>(builder =>
        {
            builder.UseTimeout(TimeSpan.FromSeconds(5), clock);
            builder.UseExecute(async ctx =>
            {
                clock.Advance(TimeSpan.FromSeconds(6));
                await Task.Delay(Timeout.InfiniteTimeSpan, ctx.CancellationToken);
            });
        });

        await Assert.ThrowsAsync<PipelineTimeoutException>(async () =>
            await pipe.SendAsync(context)
        );

        Assert.False(context.CancellationToken.IsCancellationRequested);
    }

    [Fact]
    public void Probe_describes_the_timeout()
    {
        var pipe = Pipe.Create<TimedContext>(builder =>
            builder.UseTimeout(TimeSpan.FromSeconds(5))
        );

        var probe = PipelineProbe.Inspect(pipe, TestContext.Current.CancellationToken);

        Assert.Equal("timeout", probe.Children[0].Name);
        Assert.Equal(TimeSpan.FromSeconds(5), probe.Children[0].Properties["timeout"]);
    }

    private sealed class TimedContext(CancellationToken cancellationToken = default)
        : PipeContext(cancellationToken);
}
