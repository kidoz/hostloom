using HostLoom.Pipelines;
using Xunit;

namespace HostLoom.Tests;

public sealed class PartitionerTests
{
    [Fact]
    public async Task Different_keys_run_together()
    {
        var firstEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var pipe = Pipe.Create<KeyedContext>(builder =>
            builder
                .UsePartitioner(2, context => context.Key)
                .Use(
                    async (context, next) =>
                    {
                        if (context.Key == 0)
                        {
                            firstEntered.SetResult();
                            await release.Task;
                        }
                        else
                        {
                            secondEntered.SetResult();
                        }

                        await next.SendAsync(context);
                    }
                )
        );

        var first = pipe.SendAsync(new KeyedContext(0));
        await firstEntered.Task;
        var second = pipe.SendAsync(new KeyedContext(1));
        await secondEntered.Task.WaitAsync(
            TimeSpan.FromSeconds(2),
            TestContext.Current.CancellationToken
        );
        release.SetResult();
        await Task.WhenAll(first.AsTask(), second.AsTask());
    }

    [Fact]
    public async Task The_same_key_runs_one_at_a_time_in_arrival_order()
    {
        var entered = new List<int>();
        var releaseFirst = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var firstEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var pipe = Pipe.Create<KeyedContext>(builder =>
            builder
                .UsePartitioner(2, context => context.Key)
                .Use(
                    async (context, next) =>
                    {
                        lock (entered)
                        {
                            entered.Add(context.Order);
                        }

                        if (context.Order == 1)
                        {
                            firstEntered.SetResult();
                            await releaseFirst.Task;
                        }

                        await next.SendAsync(context);
                    }
                )
        );

        var first = pipe.SendAsync(new KeyedContext(0, 1));
        await firstEntered.Task;
        var second = pipe.SendAsync(new KeyedContext(0, 2));
        await Task.Delay(50, TestContext.Current.CancellationToken);
        lock (entered)
        {
            Assert.Equal([1], entered);
        }

        releaseFirst.SetResult();
        await Task.WhenAll(first.AsTask(), second.AsTask());
        Assert.Equal([1, 2], entered);
    }

    [Fact]
    public async Task Cancelling_a_queued_call_does_not_run_it_and_frees_the_lane()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var holding = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ran = 0;
        var pipe = Pipe.Create<KeyedContext>(builder =>
            builder
                .UsePartitioner(1, context => context.Key)
                .Use(
                    async (context, next) =>
                    {
                        if (context.Order == 1)
                        {
                            holding.SetResult();
                            await release.Task;
                        }
                        else
                        {
                            Interlocked.Increment(ref ran);
                        }

                        await next.SendAsync(context);
                    }
                )
        );

        var first = pipe.SendAsync(new KeyedContext(0, 1));
        await holding.Task;
        using var cancel = new CancellationTokenSource();
        var queued = pipe.SendAsync(new KeyedContext(0, 2, cancel.Token));
        await cancel.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await queued);
        release.SetResult();
        await first;
        await pipe.SendAsync(new KeyedContext(0, 3));
        Assert.Equal(1, ran);
    }

    [Fact]
    public async Task A_fault_releases_the_lane()
    {
        var failed = false;
        var pipe = Pipe.Create<KeyedContext>(builder =>
            builder
                .UsePartitioner(1, context => context.Key)
                .Use(
                    (context, next) =>
                        !failed
                            ? throw new InvalidOperationException("fail")
                            : next.SendAsync(context)
                )
        );

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pipe.SendAsync(new KeyedContext(0))
        );
        failed = true;
        await pipe.SendAsync(new KeyedContext(0));
    }

    [Fact]
    public async Task A_null_key_is_rejected()
    {
        var pipe = Pipe.Create<KeyedContext>(builder => builder.UsePartitioner(1, _ => null!));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pipe.SendAsync(new KeyedContext(0))
        );
        Assert.Contains("null", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_probe_names_the_partitioner_and_its_lane_count()
    {
        var pipe = Pipe.Create<KeyedContext>(builder =>
            builder.UsePartitioner(4, context => context.Key)
        );

        var probe = PipelineProbe.Inspect(pipe, TestContext.Current.CancellationToken);
        var partitioner = Assert.Single(probe.Children, child => child.Name == "partitioner");
        Assert.Equal(4, partitioner.Properties["partitionCount"]);
    }

    private sealed class KeyedContext(
        int key,
        int order = 0,
        CancellationToken cancellationToken = default
    ) : PipeContext(cancellationToken)
    {
        public int Key { get; } = key;

        public int Order { get; } = order;
    }
}
