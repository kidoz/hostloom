using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Caching.Testing;
using HostLoom.Conformance;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Valkey;
using Microsoft.Extensions.DependencyInjection;
using ValkeyDotNet;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Runs the same cache scenarios the unit suite runs on the in-process backends against a real
/// Valkey, composed with <c>new</c> and through the container, on the wall clock. The invalidation
/// scenarios also run with a channel that does not flush on reconnect.
/// </summary>
/// <remarks>
/// The Valkey channel is explicit Pub/Sub only, so it reports removals and no store writes. A
/// restored subscription is simulated by killing the channel's own subscriber socket, which its
/// worker replaces.
/// </remarks>
[Collection(nameof(ValkeyCacheConformanceTests))]
[CollectionDefinition(nameof(ValkeyCacheConformanceTests), DisableParallelization = true)]
public sealed class ValkeyCacheConformanceTests
{
    private const string Resubscribed = "hostloom.cache.invalidation.resubscribed";

    public static bool Available => ValkeyAvailability.Available;

    public static TheoryData<string, string> Scenarios
    {
        get
        {
            var data = new TheoryData<string, string>();
            foreach (var scenario in CacheConformance.Scenarios.Keys)
            {
                data.Add(scenario, "new");
                data.Add(scenario, "container");
            }

            foreach (var scenario in CacheConformance.InvalidationScenarios.Keys)
            {
                data.Add(scenario, "no-flush");
            }

            return data;
        }
    }

    [Theory(Skip = ValkeyAvailability.Skip, SkipUnless = nameof(Available))]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesOnValkey(string scenario, string composition)
    {
        var token = TestContext.Current.CancellationToken;
        var ns = "conf-" + Guid.NewGuid().ToString("N")[..8];
        var options = ValkeyAvailability.Options();
        var channelOptions = new CachingOptions { Namespace = ns };
        channelOptions.Invalidation.FlushLocalOnReconnect = composition != "no-flush";
        await using var connection = new ValkeyConnection(options);
        var channel = new ValkeyCacheInvalidationChannel(connection, channelOptions);
        await using (channel)
        {
            var store = new ValkeyCacheStore(connection);
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await channel.StartAsync(timeout.Token);
            var faults = new FaultingCacheStore(store, channel);
            var containers = new List<ServiceProvider>();
            var instances = new List<IAsyncDisposable>();
            var fixture = new CacheConformanceFixture
            {
                Clock = new RealConformanceClock(),
                Namespace = ns,
                Faults = faults,
                FlushLocalOnReconnect = channelOptions.Invalidation.FlushLocalOnReconnect,
                RestoreSubscriptionAsync = () => RestoreSubscriptionAsync(options, ns, token),
                CreateCache =
                    composition == "container"
                        ? () => FromContainer(ns, faults, containers)
                        : () =>
                            Track(new TieredCache(Options(ns), faults, Serializer()), instances),
            };

            try
            {
                await CacheConformance
                    .Scenarios[scenario](fixture)
                    .WaitAsync(TimeSpan.FromSeconds(60), token);
            }
            finally
            {
                foreach (var instance in instances)
                    await instance.DisposeAsync();
                foreach (var container in containers)
                    await container.DisposeAsync();
            }
        }
    }

    /// <summary>
    /// Kills the channel's subscriber socket and returns once its replacement is acknowledged. The
    /// channel counts the resubscription before it hands over its flush and before it reads any
    /// message on the new socket, so anything published afterwards arrives after the flush.
    /// </summary>
    private static async Task RestoreSubscriptionAsync(
        ValkeyOptions options,
        string ns,
        CancellationToken token
    )
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(TimeSpan.FromSeconds(30));
        using var metrics = new MeterCapture(
            CachingDiagnostics.MeterName,
            "hostloom.cache.namespace",
            ns
        );
        await using var admin = await ValkeyClient.ConnectAsync(
            ValkeyAvailability.Options().Connection,
            deadline.Token
        );
        var clients = await admin.ExecuteAsync(
            new ValkeyCommand("CLIENT", "LIST", "TYPE", "pubsub"),
            deadline.Token
        );
        var line = Assert.Single(
            clients.AsString()!.Split('\n', StringSplitOptions.RemoveEmptyEntries),
            item => item.Split(' ').Contains("name=" + options.Connection.ClientName)
        );
        var id = line.Split(' ').Single(item => item.StartsWith("id=", StringComparison.Ordinal))[
            3..
        ];
        await admin.ExecuteAsync(new ValkeyCommand("CLIENT", "KILL", "ID", id), deadline.Token);
        await metrics.WaitForAsync(Resubscribed, 1, deadline.Token);
    }

    private static TieredCache Track(TieredCache cache, List<IAsyncDisposable> instances)
    {
        instances.Add(cache);
        return cache;
    }

    private static CachingOptions Options(string ns)
    {
        var options = new CachingOptions { Namespace = ns };
        // The stampede lease is a real round trip here; keep the re-check pause short.
        options.Stampede.WaitBeforeFallback = TimeSpan.FromMilliseconds(20);
        return options;
    }

    private static SystemTextJsonCacheValueSerializer Serializer() =>
        new(new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() });

    private static ICache FromContainer(
        string ns,
        FaultingCacheStore faults,
        List<ServiceProvider> containers
    )
    {
        var services = new ServiceCollection();
        services.AddSingleton(faults);
        services
            .AddHostLoomCaching(caching =>
            {
                caching.Namespace = ns;
                caching.Stampede.WaitBeforeFallback = TimeSpan.FromMilliseconds(20);
            })
            .UseStore<FaultingCacheStore>("Faulting")
            .UseSystemTextJson(
                new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() }
            );
        var container = services.BuildServiceProvider();
        containers.Add(container);
        return container.GetRequiredService<ICache>();
    }
}
