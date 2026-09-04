using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Caching.Pipelines;
using HostLoom.Pipelines;
using HostLoom.Pipelines.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Xunit;

namespace HostLoom.Tests;

public sealed class CachingPipelineTests
{
    private static readonly CacheEntryOptions Entry = new(TimeSpan.FromMinutes(5));

    private static CachingOptions Options() => new() { Namespace = "pipe" };

    private static SystemTextJsonCacheValueSerializer Serializer() =>
        new(new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() });

    [Fact]
    public async Task Cache_Miss_RunsDownstreamAndCachesThePayload()
    {
        await using var cache = new TieredCache(Options());
        var runs = 0;
        var pipe = Pipe.Create<CatalogContext>(builder =>
        {
            builder.UseCache<CatalogContext, Catalog>(
                cache,
                context => $"catalog:{context.Region}",
                Entry
            );
            builder.UseExecute(context =>
            {
                runs++;
                context.GetOrAddPayload(() => new Catalog(context.Region, 2));
                return ValueTask.CompletedTask;
            });
        });

        var context = new CatalogContext("eu");
        await pipe.SendAsync(context);

        Assert.Equal(1, runs);
        Assert.True(context.TryGetPayload<CacheFilterResult>(out var result));
        Assert.False(result!.Hit);
        Assert.Equal("catalog:eu", result.Key);
        var cached = await cache.TryGetAsync<Catalog>(
            "catalog:eu",
            TestContext.Current.CancellationToken
        );
        Assert.True(cached.Found);
        Assert.Equal(new Catalog("eu", 2), cached.Value);
    }

    [Fact]
    public async Task Cache_Hit_SkipsDownstreamAndSuppliesThePayload()
    {
        await using var cache = new TieredCache(Options());
        await cache.SetAsync(
            "catalog:eu",
            new Catalog("eu", 9),
            Entry,
            TestContext.Current.CancellationToken
        );
        var runs = 0;
        var pipe = Pipe.Create<CatalogContext>(builder =>
        {
            builder.UseCache<CatalogContext, Catalog>(
                cache,
                context => $"catalog:{context.Region}",
                Entry
            );
            builder.UseExecute(_ =>
            {
                runs++;
                return ValueTask.CompletedTask;
            });
        });

        var context = new CatalogContext("eu");
        await pipe.SendAsync(context);

        Assert.Equal(0, runs);
        Assert.True(context.TryGetPayload<Catalog>(out var catalog));
        Assert.Equal(9, catalog!.Items);
        Assert.True(context.TryGetPayload<CacheFilterResult>(out var result));
        Assert.True(result!.Hit);
        Assert.Equal(CacheTier.L1, result.Tier);
    }

    [Fact]
    public async Task Cache_DownstreamWithoutPayload_CachesNothing()
    {
        await using var cache = new TieredCache(Options());
        var pipe = Pipe.Create<CatalogContext>(builder =>
        {
            builder.UseCache<CatalogContext, Catalog>(
                cache,
                context => $"catalog:{context.Region}",
                Entry
            );
            builder.UseExecute(_ => ValueTask.CompletedTask);
        });

        await pipe.SendAsync(new CatalogContext("eu"));

        Assert.False(
            (
                await cache.TryGetAsync<Catalog>(
                    "catalog:eu",
                    TestContext.Current.CancellationToken
                )
            ).Found
        );
    }

    [Fact]
    public async Task Cache_PassesTheContextTokenToEveryCall()
    {
        var cache = Substitute.For<ICache>();
        using var cts = new CancellationTokenSource();
        cache
            .TryGetAsync<Catalog>("catalog:eu", cts.Token)
            .Returns(new ValueTask<CacheLookup<Catalog>>(CacheLookup.Miss<Catalog>()));
        var pipe = Pipe.Create<CatalogContext>(builder =>
        {
            builder.UseCache<CatalogContext, Catalog>(
                cache,
                context => $"catalog:{context.Region}",
                Entry
            );
            builder.UseExecute(context =>
            {
                context.GetOrAddPayload(() => new Catalog(context.Region, 1));
                return ValueTask.CompletedTask;
            });
        });

        await pipe.SendAsync(new CatalogContext("eu", cts.Token));

        await cache.Received(1).TryGetAsync<Catalog>("catalog:eu", cts.Token);
        await cache.Received(1).SetAsync("catalog:eu", new Catalog("eu", 1), Entry, cts.Token);
    }

    [Fact]
    public async Task Deduplication_RunsTheFirstIdAndSkipsTheSecondInsideTheWindow()
    {
        await using var cache = new TieredCache(Options());
        var runs = 0;
        var pipe = Pipe.Create<CatalogContext>(builder =>
        {
            builder.UseDeduplication(cache, context => context.MessageId, TimeSpan.FromMinutes(1));
            builder.UseExecute(_ =>
            {
                runs++;
                return ValueTask.CompletedTask;
            });
        });

        var first = new CatalogContext("eu") { MessageId = "m-1" };
        var second = new CatalogContext("eu") { MessageId = "m-1" };
        var other = new CatalogContext("eu") { MessageId = "m-2" };
        await pipe.SendAsync(first);
        await pipe.SendAsync(second);
        await pipe.SendAsync(other);

        Assert.Equal(2, runs);
        Assert.False(first.HasPayload(typeof(Deduplicated)));
        Assert.True(second.TryGetPayload<Deduplicated>(out var duplicate));
        Assert.Equal("m-1", duplicate!.Id);
        Assert.False(other.HasPayload(typeof(Deduplicated)));
    }

    [Fact]
    public async Task Deduplication_RunsAnywayWhenTheStoreIsUnavailable()
    {
        await using var cache = new TieredCache(Options(), new UnavailableStore(), Serializer());
        var runs = 0;
        var pipe = Pipe.Create<CatalogContext>(builder =>
        {
            builder.UseDeduplication(cache, context => context.MessageId, TimeSpan.FromMinutes(1));
            builder.UseExecute(_ =>
            {
                runs++;
                return ValueTask.CompletedTask;
            });
        });

        var context = new CatalogContext("eu") { MessageId = "m-1" };
        await pipe.SendAsync(context);
        await pipe.SendAsync(new CatalogContext("eu") { MessageId = "m-1" });

        Assert.Equal(2, runs);
        Assert.True(context.TryGetPayload<DeduplicationSkipped>(out var skipped));
        Assert.Equal(CacheFailureKind.Unavailable, skipped!.Kind);
        Assert.False(context.HasPayload(typeof(Deduplicated)));
    }

    [Fact]
    public async Task Probe_DescribesBothFilters()
    {
        await using var cache = new TieredCache(Options());
        var pipe = Pipe.Create<CatalogContext>(builder =>
        {
            builder.UseDeduplication(cache, context => context.MessageId, TimeSpan.FromSeconds(30));
            builder.UseCache<CatalogContext, Catalog>(
                cache,
                context => $"catalog:{context.Region}",
                new CacheEntryOptions(TimeSpan.FromMinutes(5)) { Tags = ["catalog"] }
            );
        });

        var probe = PipelineProbe.Inspect(pipe, TestContext.Current.CancellationToken);

        var deduplication = Assert.Single(Flatten(probe), node => node.Name == "deduplication");
        Assert.Equal(TimeSpan.FromSeconds(30), deduplication.Properties["window"]);
        Assert.Equal("run", deduplication.Properties["onUnavailable"]);
        var cacheNode = Assert.Single(Flatten(probe), node => node.Name == "cache");
        Assert.Equal(nameof(Catalog), cacheNode.Properties["payload"]);
        Assert.Equal(TimeSpan.FromMinutes(5), cacheNode.Properties["expiration"]);
        Assert.Equal(1, cacheNode.Properties["tags"]);
    }

    [Fact]
    public async Task Cache_ResolvesFromTheContainerThroughAddFilter()
    {
        await using var cache = new TieredCache(Options());
        var services = new ServiceCollection();
        services.AddSingleton<ICache>(cache);
        services.AddSingleton(
            new CacheFilterOptions<CatalogContext, Catalog>
            {
                KeySelector = context => $"catalog:{context.Region}",
                Entry = Entry,
            }
        );
        services.AddSingleton<LoadCounter>();
        services.AddPipeline<CatalogContext>(
            "catalog",
            pipeline =>
                pipeline.Stage(
                    "load",
                    stage =>
                        stage
                            .AddFilter<CacheFilter<CatalogContext, Catalog>>()
                            .AddFilter<LoadFilter>()
                )
        );
        await using var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredKeyedService<IPipelineRunner<CatalogContext>>("catalog");

        var first = new CatalogContext("eu");
        var second = new CatalogContext("eu");
        await runner.RunAsync(first);
        await runner.RunAsync(second);

        Assert.Equal(1, provider.GetRequiredService<LoadCounter>().Loads);
        Assert.True(second.TryGetPayload<CacheFilterResult>(out var result));
        Assert.True(result!.Hit);
        Assert.True(second.TryGetPayload<Catalog>(out var catalog));
        Assert.Equal("eu", catalog!.Region);
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

    private sealed class CatalogContext(
        string region,
        CancellationToken cancellationToken = default
    ) : PipeContext(cancellationToken)
    {
        public string Region { get; } = region;

        public string MessageId { get; init; } = "";
    }

    private sealed record Catalog(string Region, int Items);

    private sealed class LoadCounter
    {
        public int Loads { get; set; }
    }

    private sealed class LoadFilter(LoadCounter counter) : IFilter<CatalogContext>
    {
        public async ValueTask SendAsync(CatalogContext context, IPipe<CatalogContext> next)
        {
            counter.Loads++;
            context.GetOrAddPayload(() => new Catalog(context.Region, 3));
            await next.SendAsync(context);
        }
    }

    /// <summary>A store whose every call fails as unreachable, so the fail-open paths can be driven.</summary>
    private sealed class UnavailableStore : IDistributedCacheStore
    {
        public CacheStoreCapabilities Capabilities => CacheStoreCapabilities.None;

        public ValueTask<CacheStoreEntry?> GetAsync(
            string key,
            CancellationToken cancellationToken = default
        ) => throw Down();

        public ValueTask SetAsync(
            string key,
            ReadOnlyMemory<byte> payload,
            TimeSpan timeToLive,
            IReadOnlyCollection<string>? tagKeys = null,
            CancellationToken cancellationToken = default
        ) => throw Down();

        public ValueTask<bool> SetIfAbsentAsync(
            string key,
            ReadOnlyMemory<byte> payload,
            TimeSpan timeToLive,
            IReadOnlyCollection<string>? tagKeys = null,
            CancellationToken cancellationToken = default
        ) => throw Down();

        public ValueTask RemoveAsync(
            IReadOnlyCollection<string> keys,
            CancellationToken cancellationToken = default
        ) => throw Down();

        public ValueTask<IReadOnlyDictionary<string, CacheStoreEntry>> GetManyAsync(
            IReadOnlyCollection<string> keys,
            CancellationToken cancellationToken = default
        ) => throw Down();

        public ValueTask SetManyAsync(
            IReadOnlyCollection<KeyValuePair<string, ReadOnlyMemory<byte>>> entries,
            TimeSpan timeToLive,
            CancellationToken cancellationToken = default
        ) => throw Down();

        public ValueTask RemoveByTagAsync(
            string tagKey,
            CancellationToken cancellationToken = default
        ) => throw Down();

        private static CacheStoreException Down() =>
            new(CacheFailureKind.Unavailable, "The store is unreachable.");
    }
}
