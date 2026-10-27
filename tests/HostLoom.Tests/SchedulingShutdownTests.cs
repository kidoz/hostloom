using HostLoom.Scheduling;
using HostLoom.Scheduling.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace HostLoom.Tests;

public sealed class SchedulingShutdownTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    [InlineData(4_294_967_295L)]
    [InlineData(5_184_000_000L)]
    public async Task Invalid_shutdown_timeouts_fail_direct_and_hosted_validation(long milliseconds)
    {
        var options = new SchedulingOptions
        {
            ShutdownTimeout = TimeSpan.FromMilliseconds(milliseconds),
        };
        Assert.Contains(
            options.Validate(),
            problem => problem.Contains("Scheduling:ShutdownTimeout", StringComparison.Ordinal)
        );
        var directError = Assert.Throws<ArgumentException>(() => new Scheduler(options, []));
        Assert.Contains(
            "Scheduling:ShutdownTimeout",
            directError.Message,
            StringComparison.Ordinal
        );

        using var host = new HostBuilder()
            .ConfigureServices(services =>
                services.AddHostLoomScheduling(scheduling =>
                    scheduling.ShutdownTimeout = options.ShutdownTimeout
                )
            )
            .Build();
        var hostedError = await Assert.ThrowsAsync<OptionsValidationException>(() =>
            host.StartAsync(TestContext.Current.CancellationToken)
        );
        Assert.Contains(
            "Scheduling:ShutdownTimeout",
            hostedError.Message,
            StringComparison.Ordinal
        );
    }

    [Theory]
    [InlineData(1L)]
    [InlineData(4_294_967_294L)]
    public async Task Supported_shutdown_timeout_boundaries_allow_direct_disposal(long milliseconds)
    {
        var options = new SchedulingOptions
        {
            ShutdownTimeout = TimeSpan.FromMilliseconds(milliseconds),
        };
        Assert.Empty(options.Validate());
        await using var scheduler = new Scheduler(options, []);
        await scheduler.DisposeAsync();
    }

    [Fact(Timeout = 30_000)]
    public async Task Host_disposal_preserves_abandonment_after_a_bounded_stop()
    {
        var entered = Signal();
        var release = Signal();
        var completed = Signal();
        var host = new HostBuilder()
            .ConfigureServices(services =>
                services
                    .AddHostLoomScheduling()
                    .AddSchedule(
                        "catalog",
                        ScheduleTrigger.FixedRate(TimeSpan.FromHours(1)),
                        async (_, _, token) =>
                        {
                            entered.SetResult();
                            await release.Task;
                            Assert.True(token.IsCancellationRequested);
                            // Token resources remain valid until the abandoned run has ended.
                            using var registration = token.Register(static () => { });
                            completed.SetResult();
                        }
                    )
            )
            .Build();
        var scheduler = host.Services.GetRequiredService<Scheduler>();
        Task? disposing = null;
        try
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            await entered.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
            using var stopBound = new CancellationTokenSource();
            var stopping = host.StopAsync(stopBound.Token);
            await stopBound.CancelAsync();
            await stopping.WaitAsync(Bound, TestContext.Current.CancellationToken);

            disposing = ((IAsyncDisposable)host).DisposeAsync().AsTask();
            await disposing.WaitAsync(Bound, TestContext.Current.CancellationToken);
            Assert.True(scheduler.GetState("catalog").Running);
        }
        finally
        {
            release.TrySetResult();
            await ((IAsyncDisposable)host)
                .DisposeAsync()
                .AsTask()
                .WaitAsync(Bound, TestContext.Current.CancellationToken);
            if (disposing is not null)
                await disposing.WaitAsync(Bound, TestContext.Current.CancellationToken);
        }
        await completed.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => !scheduler.GetState("catalog").Running);
        Assert.Equal(ScheduleRunOutcome.Canceled, scheduler.GetState("catalog").LastOutcome);
    }

    [Fact(Timeout = 30_000)]
    public async Task Direct_disposal_still_waits_for_a_cooperating_run()
    {
        var entered = Signal();
        var canceled = Signal();
        var release = Signal();
        var scheduler = new Scheduler(
            new(),
            [
                new ScheduleDefinition(
                    "catalog",
                    ScheduleTrigger.FixedRate(TimeSpan.FromHours(1)),
                    async (_, token) =>
                    {
                        using var registration = token.Register(() => canceled.TrySetResult());
                        entered.SetResult();
                        await release.Task;
                    }
                ),
            ]
        );
        await scheduler.StartAsync(TestContext.Current.CancellationToken);
        await entered.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
        var disposing = scheduler.DisposeAsync().AsTask();
        try
        {
            await canceled.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
            Assert.False(disposing.IsCompleted);
        }
        finally
        {
            release.TrySetResult();
            await disposing.WaitAsync(Bound, TestContext.Current.CancellationToken);
        }
        Assert.False(scheduler.GetState("catalog").Running);
    }

    [Fact(Timeout = 30_000)]
    public async Task Direct_disposal_abandons_a_noncooperative_run_at_the_configured_bound()
    {
        var clock = new TestClock();
        var entered = Signal();
        var canceled = Signal();
        var release = Signal();
        var completed = Signal();
        var scheduler = new Scheduler(
            new SchedulingOptions { ShutdownTimeout = TimeSpan.FromSeconds(5) },
            [
                new ScheduleDefinition(
                    "catalog",
                    ScheduleTrigger.FixedRate(TimeSpan.FromHours(1)),
                    async (_, token) =>
                    {
                        using var registration = token.Register(() => canceled.TrySetResult());
                        entered.SetResult();
                        await release.Task;
                        completed.SetResult();
                    }
                ),
            ],
            timeProvider: clock
        );
        await scheduler.StartAsync(TestContext.Current.CancellationToken);
        await entered.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
        var disposing = scheduler.DisposeAsync().AsTask();
        try
        {
            await canceled.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
            await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
            clock.Advance(TimeSpan.FromSeconds(5));
            await disposing.WaitAsync(Bound, TestContext.Current.CancellationToken);
            Assert.True(scheduler.GetState("catalog").Running);
            Assert.False(completed.Task.IsCompleted);
        }
        finally
        {
            release.TrySetResult();
            await completed.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
            await SchedulingTests.WaitUntilAsync(() => !scheduler.GetState("catalog").Running);
        }
    }

    private static TaskCompletionSource Signal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}
