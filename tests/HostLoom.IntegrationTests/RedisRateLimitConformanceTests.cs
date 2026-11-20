using System.Net;
using HostLoom.AspNetCore.RateLimiting;
using HostLoom.Conformance;
using HostLoom.RateLimiting;
using HostLoom.RateLimiting.DependencyInjection;
using HostLoom.Redis;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>Runs shared token-bucket scenarios against real Redis.</summary>
[Collection(nameof(RedisRateLimitConformanceTests))]
[CollectionDefinition(nameof(RedisRateLimitConformanceTests), DisableParallelization = true)]
public sealed class RedisRateLimitConformanceTests
{
    public static bool Available => RedisAvailability.Redis;

    public static TheoryData<string, string> Scenarios
    {
        get
        {
            var data = new TheoryData<string, string>();
            foreach (var scenario in RateLimitConformance.Scenarios.Keys)
            {
                data.Add(scenario, "new");
                data.Add(scenario, "container");
            }

            return data;
        }
    }

    [Fact(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task EndpointQuota_IsSharedAcrossHostsAndSurvivesReplacement()
    {
        var ns = "orders-" + Guid.NewGuid().ToString("N");
        var token = TestContext.Current.CancellationToken;
        await using var first = await StartEndpointHostAsync(ns, token);
        await using var second = await StartEndpointHostAsync(ns, token);
        using var client = new HttpClient();

        async Task<HttpStatusCode> RequestAsync(WebApplication host, string route)
        {
            using var response = await client.GetAsync(
                new Uri(new Uri(host.Urls.Single()), route),
                token
            );
            if (response.StatusCode == HttpStatusCode.TooManyRequests)
                Assert.True(response.Headers.RetryAfter?.Delta > TimeSpan.Zero);
            return response.StatusCode;
        }

        var requests = Enumerable
            .Range(0, 12)
            .Select(i => RequestAsync(i % 2 == 0 ? first : second, "/orders/north"));
        var statuses = await Task.WhenAll(requests);
        Assert.Equal(5, statuses.Count(status => status == HttpStatusCode.OK));
        Assert.Equal(7, statuses.Count(status => status == HttpStatusCode.TooManyRequests));
        await first.StopAsync(token);
        await using var replacement = await StartEndpointHostAsync(ns, token);
        Assert.Equal(
            HttpStatusCode.TooManyRequests,
            await RequestAsync(replacement, "/orders/north")
        );
        Assert.Equal(HttpStatusCode.OK, await RequestAsync(replacement, "/orders/south"));
        Assert.Equal(HttpStatusCode.OK, await RequestAsync(replacement, "/catalog/north"));
    }

    private static async Task<WebApplication> StartEndpointHostAsync(
        string ns,
        CancellationToken cancellationToken
    )
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        foreach (var profile in new[] { "orders", "catalog" })
        {
            builder
                .Services.AddHostLoomRateLimiting(
                    profile,
                    options =>
                    {
                        options.Namespace = ns;
                        options.TokenBucket.TokenLimit = 5;
                        options.TokenBucket.ReplenishmentPeriod = TimeSpan.FromMinutes(1);
                    }
                )
                .UseRedis(redis => redis.Configuration = RedisAvailability.Configuration)
                .AddEndpointRateLimitPolicy(context =>
                    (string?)context.Request.RouteValues["fixtureId"]
                );
        }
        var app = builder.Build();
        app.UseRouting();
        app.UseRateLimiter();
        app.MapGet("/orders/{fixtureId}", () => "ok").RequireRateLimiting("orders");
        app.MapGet("/catalog/{fixtureId}", () => "ok").RequireRateLimiting("catalog");
        await app.StartAsync(cancellationToken);
        return app;
    }

    [Theory(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesOnRedis(string scenario, string composition)
    {
        var ns = "rate-conf-" + Guid.NewGuid().ToString("N")[..8];
        await using var connection = new RedisConnection(
            new RedisOptions
            {
                Configuration = RedisAvailability.Configuration,
                ClientName = "hostloom-rate-conformance",
            }
        );
        await using var store = new RedisRateLimitStore(connection);
        var containers = new List<ServiceProvider>();
        var fixture = new RateLimitConformanceFixture
        {
            Clock = new RealConformanceClock(),
            CreateLimiter =
                composition == "new"
                    ? (key, policy) =>
                        new DistributedRateLimiter(
                            new RateLimitingOptions { Namespace = ns, TokenBucket = policy },
                            key,
                            store
                        )
                    : (key, policy) =>
                    {
                        var services = new ServiceCollection();
                        services.AddKeyedSingleton(key, connection);
                        services
                            .AddHostLoomRateLimiting(
                                key,
                                options =>
                                {
                                    options.Namespace = ns;
                                    options.TokenBucket = policy;
                                }
                            )
                            .UseRedis();
                        var container = services.BuildServiceProvider();
                        containers.Add(container);
                        return container.GetRequiredKeyedService<DistributedRateLimiter>(key);
                    },
        };

        try
        {
            await RateLimitConformance
                .Scenarios[scenario](fixture)
                .WaitAsync(TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);
        }
        finally
        {
            foreach (var container in containers)
            {
                await container.DisposeAsync();
            }
        }
    }
}
