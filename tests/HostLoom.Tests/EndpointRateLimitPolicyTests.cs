using System.Diagnostics.Metrics;
using System.Net;
using System.Security.Claims;
using System.Threading.RateLimiting;
using HostLoom.AspNetCore.RateLimiting;
using HostLoom.RateLimiting;
using HostLoom.RateLimiting.DependencyInjection;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(RateLimitingDiagnosticsTests))]
public sealed class EndpointRateLimitPolicyTests
{
    private const string ClaimType = "tenant_id";
    private static readonly IPAddress Ingress = IPAddress.Parse("192.0.2.10");
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task TenantPolicy_ChargesOnlyThePrimaryAuthenticatedIdentity()
    {
        var store = new TestStore();
        await using var app = await StartAsync(store);
        var north = Identity("north");
        Assert.Equal(
            401,
            (
                await SendAsync(
                    app,
                    principal: new ClaimsPrincipal(
                        new ClaimsIdentity([new Claim(ClaimType, "north")])
                    )
                )
            ).StatusCode
        );
        foreach (
            var principal in new[]
            {
                new ClaimsPrincipal(Identity()),
                new ClaimsPrincipal(new[] { Identity(), north }),
                new ClaimsPrincipal(Identity("north", "south")),
                new ClaimsPrincipal(Identity("bad key")),
                new ClaimsPrincipal(Identity("\ud800")),
                new ClaimsPrincipal(Identity(new string('x', 513))),
            }
        )
        {
            Assert.Equal(403, (await SendAsync(app, principal: principal)).StatusCode);
        }
        Assert.Equal(0, store.Calls);
        Assert.Equal(
            200,
            (
                await SendAsync(app, principal: new ClaimsPrincipal(Identity("north", "north")))
            ).StatusCode
        );
        var rejection = await SendAsync(app, principal: new ClaimsPrincipal(north));
        Assert.Equal(429, rejection.StatusCode);
        Assert.Matches("^[1-9][0-9]*$", rejection.Headers.RetryAfter.ToString());
        Assert.False(rejection.Headers.ContainsKey("X-Action"));
        Assert.Equal(
            200,
            (await SendAsync(app, principal: new ClaimsPrincipal(Identity("south")))).StatusCode
        );
        Assert.Equal(3, store.Calls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnonymousPolicy_UsesConsumerProxyTrustAndHonorsExemption(bool trustIngress)
    {
        var store = new TestStore();
        await using var app = await StartAsync(store, trustIngress);
        Assert.Equal(200, (await SendAsync(app, "/auth", forwarded: "192.0.2.11")).StatusCode);
        Assert.Equal(
            trustIngress ? 200 : 429,
            (await SendAsync(app, "/auth", forwarded: "192.0.2.12")).StatusCode
        );
        Assert.Equal(429, (await SendAsync(app, "/auth", forwarded: "192.0.2.11")).StatusCode);
        store.Unavailable = true;
        Assert.Equal(200, (await SendAsync(app, "/refresh")).StatusCode);
        Assert.Equal(3, store.Calls);
    }

    [Theory]
    [InlineData(RateLimitUnavailableBehavior.Deny, 503)]
    [InlineData(RateLimitUnavailableBehavior.Allow, 200)]
    public async Task BackendOutage_UsesProfileBehaviorAndPassesMetadataToHandler(
        RateLimitUnavailableBehavior behavior,
        int expected
    )
    {
        var store = new TestStore { Unavailable = true };
        await using var app = await StartAsync(store, behavior: behavior);
        var response = await SendAsync(app, "/auth");
        Assert.Equal(expected, response.StatusCode);
        Assert.Equal(expected == 503, response.Headers.ContainsKey("X-Unavailable"));
        Assert.Equal(expected == 200, response.Headers.ContainsKey("X-Action"));
        Assert.False(response.Headers.ContainsKey("Retry-After"));
        Assert.Equal(1, store.Calls);
    }

    [Fact]
    public async Task CanceledRequest_AfterStoreDispatchDoesNotRetry()
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Token);
        var store = new TestStore { CancelAfterDispatch = cancellation.CancelAsync };
        await using var app = await StartAsync(store);
        var response = await SendAsync(app, "/auth", requestAborted: cancellation.Token);
        Assert.False(response.Headers.ContainsKey("X-Action"));
        await store.Canceled.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        Assert.Equal(1, store.Calls);
    }

    [Fact]
    public async Task EndpointMetrics_ReportDecisionsWithoutIdentityTags()
    {
        var observations = new List<(string Name, KeyValuePair<string, object?>[] Tags)>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == RateLimitingDiagnostics.MeterName)
                meterListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>(
            (i, _, tags, _) => observations.Add((i.Name, tags.ToArray()))
        );
        listener.SetMeasurementEventCallback<double>(
            (i, _, tags, _) => observations.Add((i.Name, tags.ToArray()))
        );
        listener.Start();
        var store = new TestStore();
        await using var app = await StartAsync(store);
        var tenant = new ClaimsPrincipal(Identity("private-operator"));
        await SendAsync(app, principal: tenant);
        await SendAsync(app, principal: tenant);
        store.Unavailable = true;
        await SendAsync(app, principal: tenant);
        var acquisitions = observations
            .Where(o => o.Name == "hostloom.ratelimit.acquisitions")
            .Select(o => o.Tags.Single(t => t.Key == RateLimitingDiagnostics.OutcomeTag).Value);
        Assert.Equal(new object?[] { "acquired", "rejected", "unavailable" }, acquisitions);
        Assert.Contains(observations, o => o.Name == "hostloom.ratelimit.store.errors");
        Assert.All(
            observations,
            observation =>
                Assert.All(
                    observation.Tags,
                    tag =>
                    {
                        Assert.Contains(
                            tag.Key,
                            new[]
                            {
                                RateLimitingDiagnostics.NamespaceTag,
                                RateLimitingDiagnostics.OutcomeTag,
                                RateLimitingDiagnostics.FailureKindTag,
                            }
                        );
                        Assert.Contains(
                            tag.Value,
                            new object?[] { "orders", "acquired", "rejected", "unavailable" }
                        );
                    }
                )
        );
    }

    [Fact]
    public async Task DynamicPartitions_KeepFixedBucketsAndDelimitedNamesSeparate()
    {
        var store = new InMemoryRateLimitStore();
        var options = new RateLimitingOptions
        {
            Namespace = "orders",
            TokenBucket = new() { ReplenishmentPeriod = TimeSpan.FromHours(1) },
        };
        using var first = new DistributedRateLimiter(options, "read", store);
        using var second = new DistributedRateLimiter(options, "read:north", store);
        using var legacy = new DistributedRateLimiter(options, "read:north:west", store);
        using var a = await first.AcquirePartitionAsync("north:west", cancellationToken: Token);
        using var b = await second.AcquirePartitionAsync("west", cancellationToken: Token);
        using var c = await legacy.AcquireAsync(cancellationToken: Token);
        Assert.True(a.IsAcquired && b.IsAcquired && c.IsAcquired);
        Assert.Equal(3, store.Count);
    }

    [Fact]
    public void EndpointPolicy_ReusesOneLocalPartitionAcrossDistinctRequests()
    {
        using var limiter = new DistributedRateLimiter(
            new RateLimitingOptions { Namespace = "orders" },
            "read",
            new InMemoryRateLimitStore()
        );
        var policy = new EndpointRateLimitPolicy(
            limiter,
            new HttpContextAccessor(),
            ctx => ctx.User.Identity?.Name,
            null
        );
        var initial = policy.GetPartition(new DefaultHttpContext());
        for (var i = 0; i < 1000; i++)
        {
            var partition = policy.GetPartition(
                new DefaultHttpContext { User = new ClaimsPrincipal(Identity($"tenant-{i}")) }
            );
            Assert.Equal(initial.PartitionKey, partition.PartitionKey);
            Assert.Same(initial.Factory, partition.Factory);
        }
    }

    private static ClaimsIdentity Identity(params string[] tenants) =>
        new(
            tenants.Select(tenant => new Claim(ClaimType, tenant)),
            "test",
            ClaimType,
            ClaimTypes.Role
        );

    private static async Task<WebApplication> StartAsync(
        TestStore store,
        bool trustIngress = false,
        RateLimitUnavailableBehavior behavior = RateLimitUnavailableBehavior.Deny
    )
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        foreach (var name in new[] { "orders-read", "auth" })
        {
            builder.Services.AddKeyedSingleton(name, store);
            var profile = builder
                .Services.AddHostLoomRateLimiting(
                    name,
                    o =>
                    {
                        o.Namespace = "orders";
                        o.OnUnavailable = behavior;
                        o.TokenBucket.ReplenishmentPeriod = TimeSpan.FromHours(1);
                    }
                )
                .UseStore<TestStore>("test");
            if (name == "orders-read")
                profile.AddTenantRateLimitPolicy(ClaimType);
            else
                profile.AddEndpointRateLimitPolicy(
                    ctx => trustIngress ? ctx.Connection.RemoteIpAddress?.ToString() : "anonymous",
                    (ctx, _) =>
                    {
                        if (
                            ctx.Lease.TryGetMetadata(RateLimitMetadata.Unavailable, out bool down)
                            && down
                        )
                            ctx.HttpContext.Response.Headers["X-Unavailable"] = "true";
                        return ValueTask.CompletedTask;
                    }
                );
        }
        var app = builder.Build();
        if (trustIngress)
        {
            var forwarded = new ForwardedHeadersOptions
            {
                ForwardedHeaders = ForwardedHeaders.XForwardedFor,
            };
            forwarded.KnownProxies.Add(Ingress);
            app.UseForwardedHeaders(forwarded);
        }
        app.UseRouting();
        app.UseRateLimiter();
        app.MapGet("/orders", MarkAction).RequireRateLimiting("orders-read");
        app.MapGet("/auth", MarkAction).RequireRateLimiting("auth");
        app.MapGet("/refresh", MarkAction).RequireRateLimiting("auth").DisableRateLimiting();
        await app.StartAsync(Token);
        return app;
    }

    private static Task MarkAction(HttpContext context)
    {
        context.Response.Headers["X-Action"] = "true";
        return Task.CompletedTask;
    }

    private static async Task<HttpResponse> SendAsync(
        WebApplication app,
        string path = "/orders",
        ClaimsPrincipal? principal = null,
        string? forwarded = null,
        CancellationToken? requestAborted = null
    )
    {
        var result = await app.GetTestServer()
            .SendAsync(
                ctx =>
                {
                    if (requestAborted.HasValue)
                        ctx.RequestAborted = requestAborted.Value;
                    ctx.Request.Method = "GET";
                    ctx.Request.Path = path;
                    if (principal is not null)
                        ctx.User = principal;
                    ctx.Connection.RemoteIpAddress = Ingress;
                    if (forwarded is not null)
                        ctx.Request.Headers["X-Forwarded-For"] = forwarded;
                },
                Token
            );
        return result.Response;
    }

    private sealed class TestStore : IRateLimitStore
    {
        private readonly InMemoryRateLimitStore _inner = new();
        public bool Unavailable { get; set; }
        public Func<Task>? CancelAfterDispatch { get; init; }
        public int Calls;
        public TaskCompletionSource Canceled { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async ValueTask<RateLimitDecision> AcquireAsync(
            string key,
            TokenBucketRateLimitPolicy policy,
            int permitCount,
            CancellationToken cancellationToken
        )
        {
            Interlocked.Increment(ref Calls);
            if (CancelAfterDispatch is { } cancel)
            {
                await cancel();
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                }
                finally
                {
                    Canceled.TrySetResult();
                }
            }
            if (Unavailable)
                throw new RateLimitStoreException(RateLimitFailureKind.Unavailable, "offline");
            return await _inner.AcquireAsync(key, policy, permitCount, cancellationToken);
        }
    }
}
