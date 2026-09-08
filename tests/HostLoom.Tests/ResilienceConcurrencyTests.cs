using HostLoom.Pipelines;
using Xunit;

namespace HostLoom.Tests;

public sealed class ResilienceConcurrencyTests
{
    [Theory]
    [InlineData("success")]
    [InlineData("failure")]
    [InlineData("canceled")]
    public async Task An_old_completion_cannot_end_the_current_half_open_trial(string outcome)
    {
        var clock = new TestClock();
        var pipe = Breaker(clock);
        var oldCompletion = Completion();
        Task old = pipe.SendAsync(Waiting(oldCompletion)).AsTask();
        await Fail(pipe);
        clock.Advance(TimeSpan.FromSeconds(30));
        var trialCompletion = Completion();
        Task trial = pipe.SendAsync(Waiting(trialCompletion)).AsTask();

        Complete(oldCompletion, outcome);
        await Observe(old, outcome);
        string state = State(pipe);
        Exception? rejected = await Record.ExceptionAsync(() => pipe.SendAsync(Success()).AsTask());
        trialCompletion.SetResult();
        await trial;

        Assert.Equal("HalfOpen", state);
        Assert.IsType<CircuitBreakerOpenException>(rejected);
        Assert.Equal("Closed", State(pipe));
    }

    [Theory]
    [InlineData("success")]
    [InlineData("failure")]
    [InlineData("canceled")]
    public async Task An_old_completion_cannot_close_or_extend_an_open_circuit(string outcome)
    {
        var clock = new TestClock();
        var pipe = Breaker(clock);
        var oldCompletion = Completion();
        Task old = pipe.SendAsync(Waiting(oldCompletion)).AsTask();
        await Fail(pipe);
        clock.Advance(TimeSpan.FromSeconds(15));

        Complete(oldCompletion, outcome);
        await Observe(old, outcome);
        string state = State(pipe);
        clock.Advance(TimeSpan.FromSeconds(15));
        Exception? trialError = await Record.ExceptionAsync(() =>
            pipe.SendAsync(Success()).AsTask()
        );

        Assert.Equal("Open", state);
        Assert.Null(trialError);
        Assert.Equal("Closed", State(pipe));
    }

    [Theory]
    [InlineData("success")]
    [InlineData("failure")]
    [InlineData("canceled")]
    public async Task An_old_completion_cannot_change_a_recovered_circuit(string outcome)
    {
        var clock = new TestClock();
        var pipe = Breaker(clock);
        var oldCompletion = Completion();
        Task old = pipe.SendAsync(Waiting(oldCompletion)).AsTask();
        await Fail(pipe);
        clock.Advance(TimeSpan.FromSeconds(30));
        await pipe.SendAsync(Success());

        Complete(oldCompletion, outcome);
        await Observe(old, outcome);

        Assert.Equal("Closed", State(pipe));
        await pipe.SendAsync(Success());
    }

    [Fact]
    public async Task An_old_success_cannot_reset_the_recovered_failure_count()
    {
        var clock = new TestClock();
        var pipe = Breaker(clock, threshold: 2);
        var oldCompletion = Completion();
        Task old = pipe.SendAsync(Waiting(oldCompletion)).AsTask();
        await Fail(pipe);
        await Fail(pipe);
        clock.Advance(TimeSpan.FromSeconds(30));
        await pipe.SendAsync(Success());
        await Fail(pipe);

        oldCompletion.SetResult();
        await old;
        await Fail(pipe);

        Assert.Equal("Open", State(pipe));
    }

    [Theory]
    [InlineData("failure")]
    [InlineData("canceled")]
    public async Task The_current_trial_reopens_the_circuit_when_it_does_not_succeed(string outcome)
    {
        var clock = new TestClock();
        var pipe = Breaker(clock);
        await Fail(pipe);
        clock.Advance(TimeSpan.FromSeconds(30));
        var trialCompletion = Completion();
        Task trial = pipe.SendAsync(Waiting(trialCompletion)).AsTask();

        Complete(trialCompletion, outcome);
        await Observe(trial, outcome);

        Assert.Equal("Open", State(pipe));
        await Assert.ThrowsAsync<CircuitBreakerOpenException>(() =>
            pipe.SendAsync(Success()).AsTask()
        );
        clock.Advance(TimeSpan.FromSeconds(30));
        await pipe.SendAsync(Success());
        Assert.Equal("Closed", State(pipe));
    }

    [Theory]
    [InlineData(-3600)]
    [InlineData(3600)]
    public async Task A_wall_clock_adjustment_does_not_change_the_breaker_reset(int seconds)
    {
        var clock = new AdjustableClock();
        var pipe = Breaker(clock);
        await Fail(pipe);
        clock.WallAdjustment = TimeSpan.FromSeconds(seconds);

        Exception? before = await Record.ExceptionAsync(() => pipe.SendAsync(Success()).AsTask());
        clock.Advance(TimeSpan.FromSeconds(30));
        Exception? after = await Record.ExceptionAsync(() => pipe.SendAsync(Success()).AsTask());

        Assert.IsType<CircuitBreakerOpenException>(before);
        Assert.Null(after);
    }

    [Theory]
    [InlineData(-3600)]
    [InlineData(3600)]
    public async Task A_wall_clock_adjustment_does_not_change_the_rate_window(int seconds)
    {
        var clock = new AdjustableClock();
        var pipe = Pipe.Create<WorkContext>(builder =>
        {
            builder.UseRateLimit(1, TimeSpan.FromSeconds(30), clock);
            builder.UseTerminal(context => context.Work());
        });
        await pipe.SendAsync(Success());
        clock.WallAdjustment = TimeSpan.FromSeconds(seconds);
        using var caller = new CancellationTokenSource();
        Task waiting = pipe.SendAsync(new WorkContext(() => ValueTask.CompletedTask, caller.Token))
            .AsTask();
        bool delayed = !waiting.IsCompleted;
        try
        {
            clock.Advance(TimeSpan.FromSeconds(30));
            await waiting.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.True(delayed);
        }
        finally
        {
            await caller.CancelAsync();
            _ = await Record.ExceptionAsync(() => waiting);
        }
    }

    private static IPipe<WorkContext> Breaker(TimeProvider clock, int threshold = 1) =>
        Pipe.Create<WorkContext>(builder =>
        {
            builder.UseCircuitBreaker(threshold, TimeSpan.FromSeconds(30), clock);
            builder.UseTerminal(context => context.Work());
        });

    private static TaskCompletionSource Completion() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static WorkContext Waiting(TaskCompletionSource completion) =>
        new(() =>
            new ValueTask(
                completion.Task.WaitAsync(
                    TimeSpan.FromSeconds(5),
                    TestContext.Current.CancellationToken
                )
            )
        );

    private static WorkContext Success() => new(() => ValueTask.CompletedTask);

    private static async Task Fail(IPipe<WorkContext> pipe) =>
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            pipe.SendAsync(
                    new WorkContext(() =>
                        ValueTask.FromException(new InvalidOperationException("injected"))
                    )
                )
                .AsTask()
        );

    private static string State(IPipe<WorkContext> pipe) =>
        (string)PipelineProbe.Inspect(pipe).Children[0].Properties["state"]!;

    private static void Complete(TaskCompletionSource completion, string outcome)
    {
        switch (outcome)
        {
            case "failure":
                completion.SetException(new InvalidOperationException("injected"));
                break;
            case "canceled":
                completion.SetCanceled();
                break;
            default:
                completion.SetResult();
                break;
        }
    }

    private static async Task Observe(Task task, string outcome)
    {
        Exception? exception = await Record.ExceptionAsync(() => task);
        switch (outcome)
        {
            case "failure":
                Assert.IsType<InvalidOperationException>(exception);
                break;
            case "canceled":
                Assert.IsAssignableFrom<OperationCanceledException>(exception);
                break;
            default:
                Assert.Null(exception);
                break;
        }
    }

    private sealed class WorkContext(Func<ValueTask> work, CancellationToken token = default)
        : PipeContext(token)
    {
        public Func<ValueTask> Work { get; } = work;
    }

    private sealed class AdjustableClock : TimeProvider
    {
        private readonly TestClock _clock = new();
        public TimeSpan WallAdjustment { get; set; }
        public override long TimestampFrequency => _clock.TimestampFrequency;

        public override long GetTimestamp() => _clock.GetTimestamp();

        public override DateTimeOffset GetUtcNow() => _clock.GetUtcNow() + WallAdjustment;

        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period
        ) => _clock.CreateTimer(callback, state, dueTime, period);

        public void Advance(TimeSpan elapsed) => _clock.Advance(elapsed);
    }
}
