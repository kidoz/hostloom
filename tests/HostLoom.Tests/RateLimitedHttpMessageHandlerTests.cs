using System.Net;
using System.Threading.RateLimiting;
using HostLoom.Conformance;
using HostLoom.RateLimiting;
using HostLoom.RateLimiting.DependencyInjection;
using HostLoom.RateLimiting.HttpClient;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Tests;

public sealed class RateLimitedHttpMessageHandlerTests
{
    [Fact]
    public async Task ImmediateRejection_ThrowsWithoutSending()
    {
        using var limiter = new ScriptedRateLimiter(
            TestRateLimitLease.Rejected(TimeSpan.FromSeconds(1))
        );
        using var downstream = new StubHttpHandler(HttpStatusCode.OK);
        using var client = Client(limiter, downstream);

        var exception = await Assert.ThrowsAsync<RateLimitExceededException>(() =>
            Get(client, "https://catalog.example")
        );

        Assert.Equal(TimeSpan.FromSeconds(1), exception.RetryAfter);
        Assert.Equal(0, downstream.Calls);
    }

    [Fact]
    public async Task BoundedWait_AcquiresAfterRetryDelay()
    {
        var clock = new ManualTimeProvider();
        using var limiter = new ScriptedRateLimiter(
            TestRateLimitLease.Rejected(TimeSpan.FromSeconds(2)),
            TestRateLimitLease.Acquired()
        );
        using var downstream = new StubHttpHandler(HttpStatusCode.OK);
        using var client = Client(
            limiter,
            downstream,
            new HttpRateLimitOptions { MaxWait = TimeSpan.FromSeconds(3), QueueLimit = 1 },
            clock
        );

#pragma warning disable CA2025 // Each pending request is awaited before its owners leave scope.
        var pending = Get(client, "https://catalog.example");
        Assert.False(pending.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(2));
        using var response = await pending;
#pragma warning restore CA2025

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(2, limiter.Calls);
        Assert.Equal(1, downstream.Calls);
    }

    [Fact]
    public async Task QueueLimit_RejectsAnotherWaitingRequest()
    {
        var clock = new ManualTimeProvider();
        using var limiter = new ScriptedRateLimiter(
            TestRateLimitLease.Rejected(TimeSpan.FromSeconds(2)),
            TestRateLimitLease.Rejected(TimeSpan.FromSeconds(2)),
            TestRateLimitLease.Acquired()
        );
        using var downstream = new StubHttpHandler(HttpStatusCode.OK);
        using var client = Client(
            limiter,
            downstream,
            new HttpRateLimitOptions { MaxWait = TimeSpan.FromSeconds(3), QueueLimit = 1 },
            clock
        );

#pragma warning disable CA2025 // Both requests are awaited before their owners leave scope.
        var first = Get(client, "https://catalog.example/first");
        var second = Get(client, "https://catalog.example/second");
        await Assert.ThrowsAsync<RateLimitExceededException>(() => second);
        clock.Advance(TimeSpan.FromSeconds(2));
        using var response = await first;
#pragma warning restore CA2025

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(3, limiter.Calls);
        Assert.Equal(1, downstream.Calls);
    }

    [Fact]
    public async Task RetryAfterBeyondBudget_ThrowsImmediately()
    {
        using var limiter = new ScriptedRateLimiter(
            TestRateLimitLease.Rejected(TimeSpan.FromSeconds(5))
        );
        using var downstream = new StubHttpHandler(HttpStatusCode.OK);
        using var client = Client(
            limiter,
            downstream,
            new HttpRateLimitOptions { MaxWait = TimeSpan.FromSeconds(1), QueueLimit = 1 }
        );

        await Assert.ThrowsAsync<RateLimitExceededException>(() =>
            Get(client, "https://catalog.example")
        );
        Assert.Equal(1, limiter.Calls);
        Assert.Equal(0, downstream.Calls);
    }

    [Fact]
    public async Task Cancellation_StopsTheWait()
    {
        var clock = new ManualTimeProvider();
        using var limiter = new ScriptedRateLimiter(
            TestRateLimitLease.Rejected(TimeSpan.FromSeconds(5))
        );
        using var downstream = new StubHttpHandler(HttpStatusCode.OK);
        using var client = Client(
            limiter,
            downstream,
            new HttpRateLimitOptions { MaxWait = TimeSpan.FromSeconds(10), QueueLimit = 1 },
            clock
        );
        using var cancellation = new CancellationTokenSource();

#pragma warning disable CA2025 // Each pending request is awaited before its owners leave scope.
        var pending = GetWithCancellation(client, "https://catalog.example", cancellation.Token);
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
#pragma warning restore CA2025
        Assert.Equal(0, downstream.Calls);
    }

    [Fact]
    public async Task UnavailableDecision_ThrowsBackendException()
    {
        using var limiter = new ScriptedRateLimiter(TestRateLimitLease.Unavailable());
        using var downstream = new StubHttpHandler(HttpStatusCode.OK);
        using var client = Client(limiter, downstream);

        await Assert.ThrowsAsync<RateLimiterUnavailableException>(() =>
            Get(client, "https://catalog.example")
        );
        Assert.Equal(0, downstream.Calls);
    }

    [Fact]
    public async Task DegradedDecision_AllowsTheRequest()
    {
        using var limiter = new ScriptedRateLimiter(TestRateLimitLease.Acquired(degraded: true));
        using var downstream = new StubHttpHandler(HttpStatusCode.OK);
        using var client = Client(limiter, downstream);

        using var response = await Get(client, "https://catalog.example");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(1, downstream.Calls);
    }

    [Fact]
    public async Task Downstream429_PassesThroughUnchanged()
    {
        using var limiter = new ScriptedRateLimiter(TestRateLimitLease.Acquired());
        using var downstream = new StubHttpHandler(HttpStatusCode.TooManyRequests);
        using var client = Client(limiter, downstream);

        using var response = await Get(client, "https://catalog.example");

        Assert.Equal(HttpStatusCode.TooManyRequests, response.StatusCode);
        Assert.Equal(1, downstream.Calls);
    }

    [Fact]
    public async Task RetryHandler_RateLimitsEveryPhysicalAttempt()
    {
        using var limiter = new ScriptedRateLimiter(
            TestRateLimitLease.Acquired(),
            TestRateLimitLease.Acquired()
        );
        using var downstream = new StubHttpHandler(
            HttpStatusCode.TooManyRequests,
            HttpStatusCode.OK
        );
        using var limited = new RateLimitedHttpMessageHandler(limiter)
        {
            InnerHandler = downstream,
        };
        using var retry = new Retry429Handler(limited);
        using var client = new HttpClient(retry);

        using var response = await Get(client, "https://catalog.example");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(2, limiter.Calls);
        Assert.Equal(2, downstream.Calls);
    }

    [Fact]
    public void HandlerDisposal_DoesNotDisposeTheSharedLimiter()
    {
        using var limiter = new ScriptedRateLimiter();
        using var handler = new RateLimitedHttpMessageHandler(limiter);

        handler.Dispose();

        Assert.False(limiter.Disposed);
    }

    [Fact]
    public async Task HttpClientBuilder_UsesTheExactNamedProfile()
    {
        using var downstream = new StubHttpHandler(HttpStatusCode.OK);
        var services = new ServiceCollection();
        services
            .AddHostLoomRateLimiting(
                "catalog",
                options =>
                {
                    options.Namespace = "catalog";
                    options.TokenBucket = new TokenBucketRateLimitPolicy
                    {
                        TokenLimit = 1,
                        ReplenishmentPeriod = TimeSpan.FromMinutes(1),
                    };
                }
            )
            .UseInMemory();
        services
            .AddHttpClient("catalog")
            .ConfigurePrimaryHttpMessageHandler(() => downstream)
            .AddHostLoomRateLimit("catalog");
        await using var container = services.BuildServiceProvider();
        var client = container.GetRequiredService<IHttpClientFactory>().CreateClient("catalog");

        using var first = await Get(client, "https://catalog.example/first");
        await Assert.ThrowsAsync<RateLimitExceededException>(() =>
            Get(client, "https://catalog.example/second")
        );
        Assert.Equal(1, downstream.Calls);
    }

    private static Task<HttpResponseMessage> Get(HttpClient client, string url) =>
        client.GetAsync(new Uri(url), TestContext.Current.CancellationToken);

    private static Task<HttpResponseMessage> GetWithCancellation(
        HttpClient client,
        string url,
        CancellationToken cancellationToken
    ) => client.GetAsync(new Uri(url), cancellationToken);

#pragma warning disable CA2000 // The returned client owns the handler and its downstream chain.
    private static HttpClient Client(
        RateLimiter limiter,
        HttpMessageHandler downstream,
        HttpRateLimitOptions? options = null,
        TimeProvider? timeProvider = null
    ) =>
        new(
            new RateLimitedHttpMessageHandler(limiter, options, timeProvider)
            {
                InnerHandler = downstream,
            }
        );
#pragma warning restore CA2000

    private sealed class ScriptedRateLimiter(params RateLimitLease[] leases) : RateLimiter
    {
        private static readonly TestRateLimitLease AlwaysAcquired = TestRateLimitLease.Acquired();
        private readonly Queue<RateLimitLease> _leases = new(leases);

        public int Calls { get; private set; }

        public bool Disposed { get; private set; }

        public override TimeSpan? IdleDuration => null;

        public override RateLimiterStatistics? GetStatistics() => null;

        protected override RateLimitLease AttemptAcquireCore(int permitCount) =>
            TestRateLimitLease.Rejected(TimeSpan.Zero);

        protected override ValueTask<RateLimitLease> AcquireAsyncCore(
            int permitCount,
            CancellationToken cancellationToken
        )
        {
            Calls++;
            return ValueTask.FromResult(_leases.Count > 0 ? _leases.Dequeue() : AlwaysAcquired);
        }

        protected override void Dispose(bool disposing)
        {
            Disposed = true;
            base.Dispose(disposing);
        }
    }

    private sealed class TestRateLimitLease : RateLimitLease
    {
        private readonly Dictionary<string, object?> _metadata = new(StringComparer.Ordinal);

        private TestRateLimitLease(bool acquired) => IsAcquired = acquired;

        public override bool IsAcquired { get; }

        public override IEnumerable<string> MetadataNames => _metadata.Keys;

        public static TestRateLimitLease Acquired(bool degraded = false)
        {
            var lease = new TestRateLimitLease(true);
            if (degraded)
            {
                lease._metadata[RateLimitMetadata.DegradedName] = true;
            }

            return lease;
        }

        public static TestRateLimitLease Rejected(TimeSpan retryAfter)
        {
            var lease = new TestRateLimitLease(false);
            if (retryAfter > TimeSpan.Zero)
            {
                lease._metadata[MetadataName.RetryAfter.Name] = retryAfter;
            }

            return lease;
        }

        public static TestRateLimitLease Unavailable()
        {
            var lease = new TestRateLimitLease(false);
            lease._metadata[RateLimitMetadata.UnavailableName] = true;
            return lease;
        }

        public override bool TryGetMetadata(string metadataName, out object? metadata) =>
            _metadata.TryGetValue(metadataName, out metadata);
    }

    private sealed class StubHttpHandler(params HttpStatusCode[] statuses) : HttpMessageHandler
    {
        private readonly Queue<HttpStatusCode> _statuses = new(statuses);

        public int Calls { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken
        )
        {
            Calls++;
            return Task.FromResult(
                new HttpResponseMessage(
                    _statuses.Count > 0 ? _statuses.Dequeue() : HttpStatusCode.OK
                )
            );
        }
    }

    private sealed class Retry429Handler(HttpMessageHandler inner) : DelegatingHandler(inner)
    {
        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken
        )
        {
            using var first = await base.SendAsync(request, cancellationToken);
            return first.StatusCode == HttpStatusCode.TooManyRequests
                ? await base.SendAsync(request, cancellationToken)
                : first;
        }
    }
}
