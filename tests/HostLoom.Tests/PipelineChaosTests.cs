using HostLoom.Pipelines;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Controlled faults inside a composed pipeline: runs that throw while the concurrency limit is
/// saturated, a caller cancelled while queued for a permit, a filter that throws on the way back
/// out, a retry loop wrapped around a circuit that opens, and a run abandoned by its own timeout.
/// The hypothesis under every fault is the same: the limit is a ceiling and never a leak, every
/// permit comes back however the run ended, and a fault is bounded rather than amplified.
/// </summary>
public sealed class PipelineChaosTests
{
    private static readonly TimeSpan Bounded = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task A_saturated_limit_is_a_ceiling_and_every_faulting_run_gives_its_permit_back()
    {
        const int limit = 3;
        const int runs = 60;
        var token = TestContext.Current.CancellationToken;
        var meter = new Meter();
        var full = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipe = Pipe.Create<ChaosContext>(builder =>
        {
            builder.UseConcurrencyLimit(limit);
            builder.UseExecute(async context =>
            {
                if (context.Probe is { } probe)
                {
                    await probe();
                    return;
                }

                try
                {
                    if (meter.Enter() == limit)
                    {
                        full.TrySetResult();
                    }

                    // Nobody leaves until the limit is saturated, so the ceiling is measured
                    // rather than merely never reached.
                    await full.Task;
                }
                finally
                {
                    meter.Exit();
                }

                if (context.Index % 3 == 0)
                {
                    throw new InvalidOperationException("the inventory ledger refused the write");
                }
            });
        });

        var attempts = Enumerable
            .Range(0, runs)
            .Select(index =>
                Task.Run(
                    async () =>
                    {
                        try
                        {
                            await pipe.SendAsync(new ChaosContext(token) { Index = index });
                            return true;
                        }
                        catch (InvalidOperationException)
                        {
                            return false;
                        }
                    },
                    token
                )
            )
            .ToArray();
        var outcomes = await Task.WhenAll(attempts).WaitAsync(Bounded, token);

        Assert.Equal(limit, meter.Peak);
        Assert.Equal(0, meter.InFlight);
        Assert.Equal(40, outcomes.Count(succeeded => succeeded));
        Assert.Equal(20, outcomes.Count(succeeded => !succeeded));
        // Recovery: the limit still admits exactly as many callers as it did before the storm.
        await AssertAdmitsAsync(pipe, limit, token);
    }

    [Fact]
    public async Task A_caller_cancelled_while_queued_for_a_permit_takes_none()
    {
        var token = TestContext.Current.CancellationToken;
        var queued = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipe = Pipe.Create<ChaosContext>(builder =>
        {
            // Ahead of the limit, so the test knows the second caller reached it. Whether the
            // cancellation lands before or after the wait begins, the contract is the same: a
            // wait that ends in cancellation never takes the permit with it.
            builder.Use(
                (context, next) =>
                {
                    if (context.Index == 1)
                    {
                        queued.TrySetResult();
                    }

                    return next.SendAsync(context);
                },
                "arrival"
            );
            builder.UseConcurrencyLimit(1);
            builder.UseExecute(async context =>
            {
                if (context.Probe is { } probe)
                {
                    await probe();
                    return;
                }

                if (context.Index == 0)
                {
                    held.TrySetResult();
                    await release.Task;
                }
            });
        });

        var holder = pipe.SendAsync(new ChaosContext(token) { Index = 0 }).AsTask();
        await held.Task.WaitAsync(Bounded, token);
        using var waiter = new CancellationTokenSource();
        var queuedCall = pipe.SendAsync(new ChaosContext(waiter.Token) { Index = 1 }).AsTask();
        await queued.Task.WaitAsync(Bounded, token);
        await waiter.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queuedCall);
        release.SetResult();
        await holder.WaitAsync(Bounded, token);
        // Recovery: the single permit is whole, so the next caller is admitted at once.
        await AssertAdmitsAsync(pipe, 1, token);
    }

    [Fact]
    public async Task A_filter_that_throws_on_the_way_out_still_returns_the_permit()
    {
        var token = TestContext.Current.CancellationToken;
        var downstream = 0;
        var pipe = Pipe.Create<ChaosContext>(builder =>
        {
            builder.UseConcurrencyLimit(1);
            builder.Use(
                async (context, next) =>
                {
                    await next.SendAsync(context);
                    // The work is done and committed; the failure is in the unwind above it.
                    throw new InvalidOperationException("the audit filter failed after the write");
                },
                "audit"
            );
            builder.UseExecute(_ =>
            {
                Interlocked.Increment(ref downstream);
                return ValueTask.CompletedTask;
            });
        });

        // A permit stranded by the first fault would leave the second call waiting forever, so
        // every attempt is bounded and the timeout would report the leak.
        for (var attempt = 0; attempt < 3; attempt++)
        {
            var run = pipe.SendAsync(new ChaosContext(token)).AsTask();
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                run.WaitAsync(Bounded, token)
            );
        }

        Assert.Equal(3, Volatile.Read(ref downstream));
    }

    [Fact]
    public async Task Retry_around_a_circuit_that_opens_stops_calling_downstream()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var calls = 0;
        var unreachable = true;
        var pipe = Pipe.Create<ChaosContext>(builder =>
        {
            builder.UseRetry(RetryPolicy.Immediate(4), timeProvider: clock);
            builder.UseCircuitBreaker(2, TimeSpan.FromSeconds(30), clock);
            builder.UseExecute(_ =>
            {
                Interlocked.Increment(ref calls);
                return Volatile.Read(ref unreachable)
                    ? throw new InvalidOperationException("the downstream ledger is unreachable")
                    : ValueTask.CompletedTask;
            });
        });

        await Assert.ThrowsAsync<CircuitBreakerOpenException>(async () =>
            await pipe.SendAsync(new ChaosContext(token))
        );

        // Five invocations were allowed and only two reached the failing dependency: the retry
        // budget is spent against the open circuit instead of against the outage.
        Assert.Equal(2, Volatile.Read(ref calls));

        // Still open, and still bounded: the reset interval admits exactly one trial per caller
        // burst, however many retries that burst has left.
        clock.Advance(TimeSpan.FromSeconds(30));
        await Assert.ThrowsAsync<CircuitBreakerOpenException>(async () =>
            await pipe.SendAsync(new ChaosContext(token))
        );
        Assert.Equal(3, Volatile.Read(ref calls));

        // Recovery: the dependency answers, the trial closes the circuit, and the caller sees a
        // normal run rather than a rejection.
        Volatile.Write(ref unreachable, false);
        clock.Advance(TimeSpan.FromSeconds(30));
        await pipe.SendAsync(new ChaosContext(token));
        Assert.Equal(4, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task A_run_abandoned_by_its_own_timeout_gives_the_permit_back()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipe = Pipe.Create<ChaosContext>(builder =>
        {
            builder.UseConcurrencyLimit(1);
            builder.UseTimeout(TimeSpan.FromSeconds(5), clock);
            builder.UseExecute(async context =>
            {
                if (context.Probe is { } probe)
                {
                    await probe();
                    return;
                }

                entered.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, context.CancellationToken);
            });
        });

        var stuck = pipe.SendAsync(new ChaosContext(token)).AsTask();
        await entered.Task.WaitAsync(Bounded, token);
        // Advance only once the deadline is armed, so the timer cannot be set after the step.
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers >= 1);
        clock.Advance(TimeSpan.FromSeconds(5));

        var timeout = await Assert.ThrowsAsync<PipelineTimeoutException>(() =>
            stuck.WaitAsync(Bounded, token)
        );
        Assert.Equal(TimeSpan.FromSeconds(5), timeout.Timeout);
        // The abandoned run held the only permit; the caller behind it must not inherit the outage.
        await AssertAdmitsAsync(pipe, 1, token);
    }

    /// <summary>
    /// Fails unless <paramref name="limit"/> callers can be inside the pipeline at once, which is
    /// how a leaked permit is caught: a pipeline short of one permit never fills.
    /// </summary>
    private static async Task AssertAdmitsAsync(
        IPipe<ChaosContext> pipe,
        int limit,
        CancellationToken cancellationToken
    )
    {
        var admitted = 0;
        var full = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var probes = Enumerable
            .Range(0, limit)
            .Select(_ =>
                pipe.SendAsync(
                        new ChaosContext(cancellationToken)
                        {
                            Probe = async () =>
                            {
                                if (Interlocked.Increment(ref admitted) == limit)
                                {
                                    full.TrySetResult();
                                }

                                await full.Task;
                            },
                        }
                    )
                    .AsTask()
            )
            .ToArray();

        await Task.WhenAll(probes).WaitAsync(Bounded, cancellationToken);
        Assert.Equal(limit, Volatile.Read(ref admitted));
    }

    private sealed class ChaosContext(CancellationToken cancellationToken = default)
        : PipeContext(cancellationToken)
    {
        public int Index { get; init; }

        /// <summary>Work an admission check runs inside the pipeline instead of the test's own body.</summary>
        public Func<Task>? Probe { get; init; }
    }

    /// <summary>Counts how many runs are inside the limited section, and the most ever seen there.</summary>
    private sealed class Meter
    {
        private int _inFlight;
        private int _peak;

        public int InFlight => Volatile.Read(ref _inFlight);

        public int Peak => Volatile.Read(ref _peak);

        public int Enter()
        {
            var current = Interlocked.Increment(ref _inFlight);
            var peak = Volatile.Read(ref _peak);
            while (current > peak)
            {
                var actual = Interlocked.CompareExchange(ref _peak, current, peak);
                if (actual == peak)
                {
                    break;
                }

                peak = actual;
            }

            return current;
        }

        public void Exit() => Interlocked.Decrement(ref _inFlight);
    }
}
