using System.Diagnostics;
using HostLoom.Pipelines;
using Xunit;

namespace HostLoom.Tests;

public sealed class ResilienceTests
{
    [Fact]
    public async Task Retry_reinvokes_the_pipeline_until_it_succeeds()
    {
        var calls = 0;
        var pipe = Pipe.Create<TestContext>(builder =>
        {
            builder.UseRetry(RetryPolicy.Immediate(3));
            builder.UseExecute(_ =>
            {
                if (++calls < 3)
                    throw new InvalidOperationException("transient");
                return ValueTask.CompletedTask;
            });
        });

        await pipe.SendAsync(new TestContext());
        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task Retry_rethrows_the_original_fault_once_the_limit_is_exhausted()
    {
        var calls = 0;
        var pipe = Pipe.Create<TestContext>(builder =>
        {
            builder.UseRetry(RetryPolicy.Immediate(2));
            builder.UseExecute(_ =>
            {
                calls++;
                throw new InvalidOperationException("always");
            });
        });

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pipe.SendAsync(new TestContext())
        );

        Assert.Equal("always", exception.Message);
        Assert.Equal(3, calls); // the original attempt plus two retries
    }

    [Fact]
    public async Task Retry_limit_of_zero_invokes_the_pipeline_exactly_once()
    {
        var calls = 0;
        var pipe = Pipe.Create<TestContext>(builder =>
        {
            builder.UseRetry(RetryPolicy.Immediate(0));
            builder.UseExecute(_ =>
            {
                calls++;
                throw new InvalidOperationException("always");
            });
        });

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pipe.SendAsync(new TestContext())
        );
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task Retry_publishes_the_attempt_number_as_a_context_payload()
    {
        var observed = new List<int?>();
        var calls = 0;
        var pipe = Pipe.Create<TestContext>(builder =>
        {
            builder.UseRetry(RetryPolicy.Immediate(3));
            builder.UseExecute(context =>
            {
                observed.Add(
                    context.TryGetPayload<RetryAttempt>(out var attempt) ? attempt!.Number : null
                );
                if (++calls < 3)
                    throw new InvalidOperationException("transient");
                return ValueTask.CompletedTask;
            });
        });

        await pipe.SendAsync(new TestContext());
        Assert.Equal([null, 1, 2], observed);
    }

    [Fact]
    public async Task Retry_never_retries_cancellation()
    {
        var calls = 0;
        var pipe = Pipe.Create<TestContext>(builder =>
        {
            builder.UseRetry(RetryPolicy.Immediate(5));
            builder.UseExecute(_ =>
            {
                calls++;
                throw new OperationCanceledException();
            });
        });

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await pipe.SendAsync(new TestContext())
        );
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task Retry_honours_the_should_retry_predicate()
    {
        var calls = 0;
        var pipe = Pipe.Create<TestContext>(builder =>
        {
            builder.UseRetry(
                RetryPolicy.Immediate(5),
                exception => exception is InvalidOperationException
            );
            builder.UseExecute(_ =>
            {
                calls++;
                throw new InvalidDataException("not retryable");
            });
        });

        await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await pipe.SendAsync(new TestContext())
        );
        Assert.Equal(1, calls);
    }

    [Fact]
    public void Immediate_policy_never_delays()
    {
        var policy = RetryPolicy.Immediate(3);
        Assert.Equal(TimeSpan.Zero, policy.GetDelay(1));
        Assert.Equal(TimeSpan.Zero, policy.GetDelay(3));
    }

    [Fact]
    public void Interval_policy_returns_a_constant_delay()
    {
        var policy = RetryPolicy.Interval(3, TimeSpan.FromMilliseconds(250));
        Assert.Equal(TimeSpan.FromMilliseconds(250), policy.GetDelay(1));
        Assert.Equal(TimeSpan.FromMilliseconds(250), policy.GetDelay(3));
    }

    [Fact]
    public void Exponential_policy_grows_by_the_factor_and_clamps_to_the_maximum()
    {
        var policy = RetryPolicy.Exponential(
            retryLimit: 5,
            minInterval: TimeSpan.FromMilliseconds(100),
            maxInterval: TimeSpan.FromMilliseconds(800)
        );

        Assert.Equal(TimeSpan.FromMilliseconds(100), policy.GetDelay(1));
        Assert.Equal(TimeSpan.FromMilliseconds(200), policy.GetDelay(2));
        Assert.Equal(TimeSpan.FromMilliseconds(400), policy.GetDelay(3));
        Assert.Equal(TimeSpan.FromMilliseconds(800), policy.GetDelay(4));
        Assert.Equal(TimeSpan.FromMilliseconds(800), policy.GetDelay(5));
    }

    [Fact]
    public void Exponential_policy_clamps_instead_of_overflowing_on_a_far_attempt()
    {
        var policy = RetryPolicy.Exponential(
            retryLimit: 2000,
            minInterval: TimeSpan.FromSeconds(1),
            maxInterval: TimeSpan.FromSeconds(30)
        );

        // 2^1000 seconds overflows TimeSpan multiplication; the clamp must be applied first.
        Assert.Equal(TimeSpan.FromSeconds(30), policy.GetDelay(1000));
    }

    [Fact]
    public void Jitter_keeps_every_delay_inside_the_configured_band()
    {
        var policy = RetryPolicy.Interval(3, TimeSpan.FromSeconds(1)).WithJitter(0.2);
        var delays = Enumerable.Range(0, 200).Select(_ => policy.GetDelay(1)).ToList();

        Assert.All(
            delays,
            delay =>
            {
                Assert.InRange(
                    delay,
                    TimeSpan.FromMilliseconds(800),
                    TimeSpan.FromMilliseconds(1200)
                );
            }
        );
        Assert.True(delays.Distinct().Count() > 1, "jitter should vary the delay between calls");
    }

    [Fact]
    public async Task Circuit_opens_after_the_configured_consecutive_failures()
    {
        var time = new TestTimeProvider();
        var calls = 0;
        var pipe = BuildBreaker(
            time,
            () =>
            {
                calls++;
                throw new InvalidOperationException("down");
            }
        );

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pipe.SendAsync(new TestContext())
        );
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pipe.SendAsync(new TestContext())
        );

        // Third call is rejected without reaching the pipeline.
        await Assert.ThrowsAsync<CircuitBreakerOpenException>(async () =>
            await pipe.SendAsync(new TestContext())
        );
        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task Open_circuit_admits_one_trial_call_after_the_reset_interval()
    {
        var time = new TestTimeProvider();
        var calls = 0;
        var pipe = BuildBreaker(
            time,
            () =>
            {
                calls++;
                throw new InvalidOperationException("down");
            }
        );

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pipe.SendAsync(new TestContext())
        );
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pipe.SendAsync(new TestContext())
        );
        await Assert.ThrowsAsync<CircuitBreakerOpenException>(async () =>
            await pipe.SendAsync(new TestContext())
        );

        time.Advance(TimeSpan.FromSeconds(30));

        // The trial reaches the pipeline, fails, and reopens the circuit.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pipe.SendAsync(new TestContext())
        );
        Assert.Equal(3, calls);
        await Assert.ThrowsAsync<CircuitBreakerOpenException>(async () =>
            await pipe.SendAsync(new TestContext())
        );
        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task Successful_trial_call_closes_the_circuit()
    {
        var time = new TestTimeProvider();
        var fail = true;
        var pipe = BuildBreaker(
            time,
            () =>
            {
                if (fail)
                    throw new InvalidOperationException("down");
            }
        );

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pipe.SendAsync(new TestContext())
        );
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pipe.SendAsync(new TestContext())
        );
        await Assert.ThrowsAsync<CircuitBreakerOpenException>(async () =>
            await pipe.SendAsync(new TestContext())
        );

        time.Advance(TimeSpan.FromSeconds(30));
        fail = false;

        await pipe.SendAsync(new TestContext());
        await pipe.SendAsync(new TestContext()); // circuit is closed, no rejection
    }

    [Fact]
    public async Task Success_resets_the_consecutive_failure_count()
    {
        var time = new TestTimeProvider();
        var fail = true;
        var pipe = BuildBreaker(
            time,
            () =>
            {
                if (fail)
                    throw new InvalidOperationException("down");
            }
        );

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pipe.SendAsync(new TestContext())
        );
        fail = false;
        await pipe.SendAsync(new TestContext());
        fail = true;

        // The earlier failure was cleared, so this one alone must not trip the two-failure threshold.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pipe.SendAsync(new TestContext())
        );
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pipe.SendAsync(new TestContext())
        );
        await Assert.ThrowsAsync<CircuitBreakerOpenException>(async () =>
            await pipe.SendAsync(new TestContext())
        );
    }

    [Fact]
    public async Task Circuit_breaker_reports_its_state_through_the_probe()
    {
        var time = new TestTimeProvider();
        var pipe = BuildBreaker(time, () => throw new InvalidOperationException("down"));

        var before = PipelineProbe.Inspect(pipe, Xunit.TestContext.Current.CancellationToken);
        Assert.Equal("Closed", before.Children[0].Properties["state"]);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pipe.SendAsync(new TestContext())
        );
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pipe.SendAsync(new TestContext())
        );

        var after = PipelineProbe.Inspect(pipe, Xunit.TestContext.Current.CancellationToken);
        Assert.Equal("circuitBreaker", after.Children[0].Name);
        Assert.Equal("Open", after.Children[0].Properties["state"]);
    }

    [Fact]
    public async Task Rate_limit_admits_the_burst_then_delays_the_next_call()
    {
        var interval = TimeSpan.FromMilliseconds(400);
        var calls = 0;
        var pipe = Pipe.Create<TestContext>(builder =>
        {
            builder.UseRateLimit(2, interval);
            builder.UseExecute(_ =>
            {
                calls++;
                return ValueTask.CompletedTask;
            });
        });

        var stopwatch = Stopwatch.StartNew();
        await pipe.SendAsync(new TestContext());
        await pipe.SendAsync(new TestContext());
        var burst = stopwatch.Elapsed;
        await pipe.SendAsync(new TestContext());
        stopwatch.Stop();

        Assert.Equal(3, calls);
        Assert.True(burst < interval, $"the first two calls should not be delayed, took {burst}");
        Assert.True(
            stopwatch.Elapsed >= TimeSpan.FromMilliseconds(200),
            $"the third call should have waited for the next window, total {stopwatch.Elapsed}"
        );
    }

    private static IPipe<TestContext> BuildBreaker(TimeProvider timeProvider, Action body) =>
        Pipe.Create<TestContext>(builder =>
        {
            builder.UseCircuitBreaker(2, TimeSpan.FromSeconds(30), timeProvider);
            builder.UseExecute(_ =>
            {
                body();
                return ValueTask.CompletedTask;
            });
        });

    private sealed class TestContext(CancellationToken cancellationToken = default)
        : PipeContext(cancellationToken);

    /// <summary>Advances only when the test says so, so breaker timing needs no real waiting.</summary>
    private sealed class TestTimeProvider : TimeProvider
    {
        private DateTimeOffset _now = DateTimeOffset.UnixEpoch;

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan delta) => _now += delta;
    }
}
