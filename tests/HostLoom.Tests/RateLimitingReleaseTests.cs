using System.Net;
using System.Threading.RateLimiting;
using HostLoom.Conformance;
using HostLoom.RateLimiting;
using HostLoom.RateLimiting.HttpClient;
using Xunit;

namespace HostLoom.Tests;

public sealed class RateLimitingReleaseTests
{
    [Fact]
    public async Task SlowAcquisition_CannotSendAfterMaxWait()
    {
        var clock = new ManualTimeProvider();
        using var limiter = new DelegateLimiter(_ =>
        {
            clock.Advance(TimeSpan.FromSeconds(2));
            return ValueTask.FromResult<RateLimitLease>(new Lease(true));
        });
        using var downstream = new DelegateHandler(_ =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK))
        );
        using var handler = Handler(limiter, downstream, clock);
        using var client = new HttpClient(handler);

        await Assert.ThrowsAsync<RateLimitExceededException>(async () =>
        {
            using var response = await client.GetAsync(
                new Uri("https://catalog.example"),
                TestContext.Current.CancellationToken
            );
        });

        Assert.Equal(0, downstream.Calls);
    }

    [Fact]
    public async Task PendingAcquisition_IsBoundedByMaxWait()
    {
        var clock = new ManualTimeProvider();
        var completion = new TaskCompletionSource<RateLimitLease>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        using var limiter = new DelegateLimiter(token => new ValueTask<RateLimitLease>(
            completion.Task.WaitAsync(token)
        ));
        using var downstream = new DelegateHandler(_ =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK))
        );
        using var handler = Handler(limiter, downstream, clock);
        using var client = new HttpClient(handler);

#pragma warning disable CA2025 // The pending request is cancelled and awaited in finally.
        var pending = client.GetAsync(new Uri("https://catalog.example"), cancellation.Token);
        clock.Advance(TimeSpan.FromSeconds(2));
        try
        {
            await Assert.ThrowsAsync<RateLimitExceededException>(async () =>
            {
                using var response = await pending.WaitAsync(
                    TimeSpan.FromSeconds(2),
                    TestContext.Current.CancellationToken
                );
            });
            Assert.Equal(0, downstream.Calls);
        }
        finally
        {
            await cancellation.CancelAsync();
            try
            {
                using var response = await pending;
            }
            catch (Exception exception)
                when (exception is OperationCanceledException or RateLimitExceededException) { }
        }
#pragma warning restore CA2025
    }

    [Fact]
    public async Task AcquiredRequest_ReleasesWaitSlotBeforeDownstreamCompletes()
    {
        var clock = new ManualTimeProvider();
        var calls = 0;
        using var limiter = new DelegateLimiter(_ =>
            ValueTask.FromResult<RateLimitLease>(new Lease(Interlocked.Increment(ref calls) == 2))
        );
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var downstream = new DelegateHandler(async token =>
        {
            entered.TrySetResult();
            await release.Task.WaitAsync(token);
            return new HttpResponseMessage(HttpStatusCode.OK);
        });
        using var handler = Handler(limiter, downstream, clock);
        using var client = new HttpClient(handler);
        using var secondCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );

#pragma warning disable CA2025 // Both requests are awaited before their owners leave scope.
        var first = client.GetAsync(
            new Uri("https://catalog.example/first"),
            TestContext.Current.CancellationToken
        );
        clock.Advance(TimeSpan.FromMilliseconds(100));
        await entered.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken
        );
        var second = client.GetAsync(
            new Uri("https://catalog.example/second"),
            secondCancellation.Token
        );
        var rejectedWhileOnlyDownstreamWasBusy = second.IsCompleted;
        try
        {
            await secondCancellation.CancelAsync();
            try
            {
                using var response = await second;
            }
            catch (Exception exception)
                when (exception is OperationCanceledException or RateLimitExceededException) { }
        }
        finally
        {
            release.TrySetResult();
            using var response = await first;
        }
#pragma warning restore CA2025

        Assert.False(
            rejectedWhileOnlyDownstreamWasBusy,
            "A request already sending downstream must not occupy a permit wait-queue entry."
        );
        Assert.Equal(1, downstream.Calls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AbandonedAcquisition_DisposesLateLeaseWithoutSending(bool callerCancels)
    {
        var clock = new ManualTimeProvider();
        var completion = new TaskCompletionSource<RateLimitLease>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var disposed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        using var limiter = new DelegateLimiter(_ =>
        {
            Interlocked.Increment(ref calls);
            return new ValueTask<RateLimitLease>(completion.Task);
        });
        using var downstream = new DelegateHandler(_ =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK))
        );
        using var handler = Handler(limiter, downstream, clock);
        using var client = new HttpClient(handler);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        using var lateLease = new Lease(true, () => disposed.TrySetResult());

#pragma warning disable CA2025 // The pending request and its late lease are completed in finally.
        var pending = client.GetAsync(new Uri("https://catalog.example"), cancellation.Token);
        try
        {
            if (callerCancels)
            {
                await cancellation.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                    pending.WaitAsync(
                        TimeSpan.FromSeconds(2),
                        TestContext.Current.CancellationToken
                    )
                );
            }
            else
            {
                clock.Advance(TimeSpan.FromSeconds(2));
                await Assert.ThrowsAsync<RateLimitExceededException>(() =>
                    pending.WaitAsync(
                        TimeSpan.FromSeconds(2),
                        TestContext.Current.CancellationToken
                    )
                );
            }

            Assert.False(completion.Task.IsCompleted);
        }
        finally
        {
            completion.SetResult(lateLease);
            try
            {
                using var response = await pending;
            }
            catch (Exception exception)
                when (exception is OperationCanceledException or RateLimitExceededException) { }
        }
#pragma warning restore CA2025

        await disposed.Task.WaitAsync(
            TimeSpan.FromSeconds(2),
            TestContext.Current.CancellationToken
        );
        Assert.Equal(1, calls);
        Assert.Equal(0, downstream.Calls);
    }

    [Fact]
    public async Task AcquisitionDeadline_DoesNotCancelDownstreamOrDisposeItsLeaseEarly()
    {
        var clock = new ManualTimeProvider();
        var disposed = false;
        using var limiter = new DelegateLimiter(_ =>
            ValueTask.FromResult<RateLimitLease>(new Lease(true, () => disposed = true))
        );
        using var downstream = new DelegateHandler(token =>
        {
            clock.Advance(TimeSpan.FromSeconds(2));
            token.ThrowIfCancellationRequested();
            Assert.False(disposed);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
        });
        using var handler = Handler(limiter, downstream, clock);
        using var client = new HttpClient(handler);

        using var response = await client.GetAsync(
            new Uri("https://catalog.example"),
            TestContext.Current.CancellationToken
        );

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(disposed);
    }

    [Fact]
    public async Task RetryAcquisition_UsesOnlyTheRemainingBudget()
    {
        var clock = new ManualTimeProvider();
        var completion = new TaskCompletionSource<RateLimitLease>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var scheduled = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var timeProvider = new TimerTrackingTimeProvider(
            clock,
            delay =>
            {
                if (delay == TimeSpan.FromMilliseconds(900))
                {
                    scheduled.TrySetResult();
                }
            }
        );
        var calls = 0;
        using var limiter = new DelegateLimiter(_ =>
        {
            if (++calls == 1)
            {
                return ValueTask.FromResult<RateLimitLease>(new Lease(false));
            }

            return new ValueTask<RateLimitLease>(completion.Task);
        });
        using var downstream = new DelegateHandler(_ =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK))
        );
        using var handler = Handler(limiter, downstream, timeProvider);
        using var client = new HttpClient(handler);
        using var lateLease = new Lease(true);

#pragma warning disable CA2025 // The request and acquisition complete before their owners leave scope.
        var pending = client.GetAsync(
            new Uri("https://catalog.example"),
            TestContext.Current.CancellationToken
        );
        try
        {
            clock.Advance(TimeSpan.FromMilliseconds(100));
            await scheduled.Task.WaitAsync(
                TimeSpan.FromSeconds(2),
                TestContext.Current.CancellationToken
            );
            clock.Advance(TimeSpan.FromMilliseconds(900));
            await Assert.ThrowsAsync<RateLimitExceededException>(() =>
                pending.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken)
            );
        }
        finally
        {
            completion.SetResult(lateLease);
            try
            {
                using var response = await pending;
            }
            catch (RateLimitExceededException) { }
        }
#pragma warning restore CA2025

        Assert.Equal(2, calls);
        Assert.Equal(0, downstream.Calls);
    }

    [Fact]
    public async Task LimiterTimeout_IsNotReclassifiedAsBudgetExhaustion()
    {
        var failure = new TimeoutException("Limiter acquisition failed.");
        using var limiter = new DelegateLimiter(_ =>
            ValueTask.FromException<RateLimitLease>(failure)
        );
        using var downstream = new DelegateHandler(_ =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK))
        );
        using var handler = Handler(limiter, downstream, new ManualTimeProvider());
        using var client = new HttpClient(handler);

        var actual = await Assert.ThrowsAsync<TimeoutException>(() =>
            client.GetAsync(
                new Uri("https://catalog.example"),
                TestContext.Current.CancellationToken
            )
        );

        Assert.Same(failure, actual);
        Assert.Equal(0, downstream.Calls);
    }

    [Fact]
    public async Task LongBudget_TimerChunksDoNotReplayAcquisitionOrRejectEarly()
    {
        var clock = new ManualTimeProvider();
        var completion = new TaskCompletionSource<RateLimitLease>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var calls = 0;
        using var limiter = new DelegateLimiter(_ =>
        {
            Interlocked.Increment(ref calls);
            return new ValueTask<RateLimitLease>(completion.Task);
        });
        using var downstream = new DelegateHandler(_ =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK))
        );
        using var handler = new RateLimitedHttpMessageHandler(
            limiter,
            new HttpRateLimitOptions { MaxWait = TimeSpan.FromDays(60), QueueLimit = 1 },
            clock
        )
        {
            InnerHandler = downstream,
        };
        using var client = new HttpClient(handler);
        using var grantedLease = new Lease(true);

#pragma warning disable CA2025 // The acquisition is completed before awaiting the request.
        var pending = client.GetAsync(
            new Uri("https://catalog.example"),
            TestContext.Current.CancellationToken
        );
        clock.Advance(TimeSpan.FromMilliseconds(uint.MaxValue - 1));
        completion.SetResult(grantedLease);
        using var response = await pending.WaitAsync(
            TimeSpan.FromSeconds(2),
            TestContext.Current.CancellationToken
        );
#pragma warning restore CA2025

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(1, calls);
        Assert.Equal(1, downstream.Calls);
    }

    [Fact]
    public async Task WeightedRetryAfter_PreservesPartialPeriod()
    {
        var clock = new ManualTimeProvider();
        var store = new InMemoryRateLimitStore(clock);
        var policy = new TokenBucketRateLimitPolicy
        {
            TokenLimit = 5,
            TokensPerPeriod = 2,
            ReplenishmentPeriod = TimeSpan.FromSeconds(1),
        };
        var token = TestContext.Current.CancellationToken;
        Assert.True(
            (await store.AcquireAsync("catalog:rate:weighted", policy, 5, token)).IsAcquired
        );
        clock.Advance(TimeSpan.FromMilliseconds(250));

        var rejected = await store.AcquireAsync("catalog:rate:weighted", policy, 5, token);
        Assert.False(rejected.IsAcquired);
        Assert.Equal(TimeSpan.FromMilliseconds(2750), rejected.RetryAfter);
        clock.Advance(TimeSpan.FromMilliseconds(2749));
        Assert.False(
            (await store.AcquireAsync("catalog:rate:weighted", policy, 5, token)).IsAcquired
        );
        clock.Advance(TimeSpan.FromMilliseconds(1));
        Assert.True(
            (await store.AcquireAsync("catalog:rate:weighted", policy, 5, token)).IsAcquired
        );
    }

    [Fact]
    public async Task SubMillisecondPeriod_RoundsUpBeforeRefilling()
    {
        var clock = new ManualTimeProvider();
        var store = new InMemoryRateLimitStore(clock);
        var policy = new TokenBucketRateLimitPolicy { ReplenishmentPeriod = TimeSpan.FromTicks(1) };
        var token = TestContext.Current.CancellationToken;
        Assert.True(
            (await store.AcquireAsync("catalog:rate:rounding", policy, 1, token)).IsAcquired
        );
        clock.Advance(TimeSpan.FromTicks(9999));
        Assert.False(
            (await store.AcquireAsync("catalog:rate:rounding", policy, 1, token)).IsAcquired
        );
        clock.Advance(TimeSpan.FromTicks(1));
        Assert.True(
            (await store.AcquireAsync("catalog:rate:rounding", policy, 1, token)).IsAcquired
        );
    }

    private static RateLimitedHttpMessageHandler Handler(
        RateLimiter limiter,
        HttpMessageHandler downstream,
        TimeProvider clock
    ) =>
        new(
            limiter,
            new HttpRateLimitOptions { MaxWait = TimeSpan.FromSeconds(1), QueueLimit = 1 },
            clock
        )
        {
            InnerHandler = downstream,
        };

    private sealed class DelegateLimiter(Func<CancellationToken, ValueTask<RateLimitLease>> acquire)
        : RateLimiter
    {
        public override TimeSpan? IdleDuration => null;

        public override RateLimiterStatistics? GetStatistics() => null;

        protected override RateLimitLease AttemptAcquireCore(int permitCount) => new Lease(false);

        protected override ValueTask<RateLimitLease> AcquireAsyncCore(
            int permitCount,
            CancellationToken cancellationToken
        ) => acquire(cancellationToken);
    }

    private sealed class Lease(bool acquired, Action? onDispose = null) : RateLimitLease
    {
        public override bool IsAcquired => acquired;

        public override IEnumerable<string> MetadataNames => [MetadataName.RetryAfter.Name];

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                onDispose?.Invoke();
            }

            base.Dispose(disposing);
        }

        public override bool TryGetMetadata(string metadataName, out object? metadata)
        {
            metadata =
                metadataName == MetadataName.RetryAfter.Name
                    ? TimeSpan.FromMilliseconds(100)
                    : null;
            return metadata is not null;
        }
    }

    private sealed class DelegateHandler(Func<CancellationToken, Task<HttpResponseMessage>> send)
        : HttpMessageHandler
    {
        public int Calls { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken
        )
        {
            Calls++;
            return send(cancellationToken);
        }
    }

    private sealed class TimerTrackingTimeProvider(
        ManualTimeProvider clock,
        Action<TimeSpan> timerCreated
    ) : TimeProvider
    {
        public override long TimestampFrequency => clock.TimestampFrequency;

        public override long GetTimestamp() => clock.GetTimestamp();

        public override DateTimeOffset GetUtcNow() => clock.GetUtcNow();

        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period
        )
        {
            var timer = clock.CreateTimer(callback, state, dueTime, period);
            timerCreated(dueTime);
            return timer;
        }
    }
}
