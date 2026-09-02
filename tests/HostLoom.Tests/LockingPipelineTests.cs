using HostLoom.Locking;
using HostLoom.Locking.Pipelines;
using HostLoom.Pipelines;
using HostLoom.Pipelines.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Tests;

public sealed class LockingPipelineTests
{
    private readonly TestClock _clock = new();

    private DistributedLock Lock() =>
        new(new LockingOptions { Namespace = "pipe" }, new InMemoryLockProvider(_clock), _clock);

    [Fact]
    public async Task Lock_SerialisesRunsForOneKey()
    {
        await using var locks = Lock();
        var firstEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var firstRelease = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var secondEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var pipe = Pipe.Create<JobContext>(builder =>
        {
            builder.UseDistributedLock(
                locks,
                context => $"job:{context.Job}",
                new LockOptions
                {
                    Retry = LockRetryPolicy.Interval(20, TimeSpan.FromMilliseconds(10)),
                }
            );
            builder.UseExecute(async context =>
            {
                if (context.Attempt == 1)
                {
                    firstEntered.SetResult();
                    await firstRelease.Task;
                }
                else
                {
                    secondEntered.SetResult();
                }
            });
        });

        var first = pipe.SendAsync(new JobContext("a", 1)).AsTask();
        await firstEntered.Task;
        var second = pipe.SendAsync(new JobContext("a", 2)).AsTask();
        // The second run is waiting on the lock; its retries only fire when the clock moves.
        _clock.Advance(TimeSpan.FromMilliseconds(10));
        await Task.Yield();
        Assert.False(secondEntered.Task.IsCompleted);
        Assert.False(second.IsCompleted);

        firstRelease.SetResult();
        await first;
        while (!second.IsCompleted)
        {
            _clock.Advance(TimeSpan.FromMilliseconds(10));
            await Task.Yield();
        }

        await second;
        Assert.True(secondEntered.Task.IsCompleted);
    }

    [Fact]
    public async Task Lock_LetsDifferentKeysRunConcurrently()
    {
        await using var locks = Lock();
        var firstEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var firstRelease = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var secondEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var pipe = Pipe.Create<JobContext>(builder =>
        {
            builder.UseDistributedLock(locks, context => $"job:{context.Job}");
            builder.UseExecute(async context =>
            {
                if (context.Job == "a")
                {
                    firstEntered.SetResult();
                    await firstRelease.Task;
                }
                else
                {
                    secondEntered.SetResult();
                }
            });
        });

        var first = pipe.SendAsync(new JobContext("a", 1)).AsTask();
        await firstEntered.Task;
        await pipe.SendAsync(new JobContext("b", 1));

        Assert.True(secondEntered.Task.IsCompleted);
        firstRelease.SetResult();
        await first;
    }

    [Fact]
    public async Task Lock_LeavesAHeldLockPayloadForDownstream()
    {
        await using var locks = Lock();
        HeldLock? observed = null;
        var pipe = Pipe.Create<JobContext>(builder =>
        {
            builder.UseDistributedLock(locks, context => $"job:{context.Job}");
            builder.UseExecute(context =>
            {
                context.TryGetPayload(out observed);
                return ValueTask.CompletedTask;
            });
        });

        await pipe.SendAsync(new JobContext("a", 1));

        Assert.NotNull(observed);
        Assert.Equal("job:a", observed.Key);
        Assert.False(observed.CancellationToken.IsCancellationRequested);
    }

    [Fact]
    public async Task Lock_NotAcquired_PropagatesUnchanged()
    {
        await using var locks = Lock();
        var held = await locks.TryAcquireAsync(
            "job:a",
            new LockOptions { Lease = TimeSpan.FromMinutes(1) },
            TestContext.Current.CancellationToken
        );
        Assert.NotNull(held);
        var pipe = Pipe.Create<JobContext>(builder =>
        {
            builder.UseDistributedLock(
                locks,
                context => $"job:{context.Job}",
                new LockOptions { MaxWait = TimeSpan.Zero }
            );
            builder.UseExecute(_ => ValueTask.CompletedTask);
        });

        var exception = await Assert.ThrowsAsync<LockNotAcquiredException>(async () =>
            await pipe.SendAsync(new JobContext("a", 1))
        );

        Assert.Equal("job:a", exception.Key);
        await held.DisposeAsync();
    }

    [Fact]
    public async Task Lock_OnLostCancel_CancelsTheHeldLockToken()
    {
        await using var locks = Lock();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipe = Pipe.Create<JobContext>(builder =>
        {
            builder.UseDistributedLock(
                locks,
                context => $"job:{context.Job}",
                new LockOptions
                {
                    Lease = TimeSpan.FromSeconds(1),
                    OnLost = LostLeaseBehavior.Cancel,
                }
            );
            builder.UseExecute(async context =>
            {
                context.TryGetPayload<HeldLock>(out var held);
                entered.SetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, held!.CancellationToken);
            });
        });

        var run = pipe.SendAsync(new JobContext("a", 1)).AsTask();
        await entered.Task;
        _clock.Advance(TimeSpan.FromSeconds(1));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
    }

    [Fact]
    public async Task Probe_DescribesTheLockFilter()
    {
        await using var locks = Lock();
        var pipe = Pipe.Create<JobContext>(builder =>
            builder.UseDistributedLock(
                locks,
                context => $"job:{context.Job}",
                new LockOptions
                {
                    Lease = TimeSpan.FromSeconds(30),
                    MaxWait = TimeSpan.FromSeconds(3),
                    Retry = LockRetryPolicy.Immediate(2),
                    OnLost = LostLeaseBehavior.Cancel,
                }
            )
        );

        var probe = PipelineProbe.Inspect(pipe, TestContext.Current.CancellationToken);

        var node = Assert.Single(Flatten(probe), node => node.Name == "distributedLock");
        Assert.Equal(TimeSpan.FromSeconds(30), node.Properties["lease"]);
        Assert.Equal(TimeSpan.FromSeconds(3), node.Properties["maxWait"]);
        Assert.Equal(LockRetryPolicy.Immediate(2).Description, node.Properties["retry"]);
        Assert.Equal("Cancel", node.Properties["onLost"]);
    }

    [Fact]
    public async Task Lock_ResolvesFromTheContainerThroughAddFilter()
    {
        await using var locks = Lock();
        var services = new ServiceCollection();
        services.AddSingleton<IDistributedLock>(locks);
        services.AddSingleton(
            new DistributedLockFilterOptions<JobContext>
            {
                KeySelector = context => $"job:{context.Job}",
            }
        );
        services.AddPipeline<JobContext>(
            "jobs",
            pipeline =>
                pipeline.Stage(
                    "run",
                    stage =>
                        stage
                            .AddFilter<DistributedLockFilter<JobContext>>()
                            .AddFilter<ObserveFilter>()
                )
        );
        await using var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredKeyedService<IPipelineRunner<JobContext>>("jobs");

        var context = new JobContext("a", 1);
        await runner.RunAsync(context);

        Assert.True(context.TryGetPayload<HeldLock>(out var held));
        Assert.Equal("job:a", held!.Key);
        Assert.True(context.TryGetPayload<Observed>(out _));
    }

    private static IEnumerable<ProbeResult> Flatten(ProbeResult node)
    {
        yield return node;
        foreach (var child in node.Children)
        {
            foreach (var descendant in Flatten(child))
            {
                yield return descendant;
            }
        }
    }

    private sealed class JobContext(
        string job,
        int attempt,
        CancellationToken cancellationToken = default
    ) : PipeContext(cancellationToken)
    {
        public string Job { get; } = job;

        public int Attempt { get; } = attempt;
    }

    private sealed record Observed;

    private sealed class ObserveFilter : IFilter<JobContext>
    {
        public async ValueTask SendAsync(JobContext context, IPipe<JobContext> next)
        {
            context.GetOrAddPayload(() => new Observed());
            await next.SendAsync(context);
        }
    }
}
