using System.Buffers;
using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Caching.Testing;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

public sealed class DistributedCacheAdapterTests
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private static JsonSerializerOptions Json() =>
        new() { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };

    private static ServiceProvider Provider(
        InMemoryDistributedCacheStore store,
        TimeProvider clock,
        Action<IServiceCollection>? more = null,
        bool enableSlidingExpiration = false
    )
    {
        var services = new ServiceCollection();
        services.AddSingleton(store);
        services.AddSingleton(clock);
        services
            .AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseStore<InMemoryDistributedCacheStore>("InMemoryDistributed")
            .UseSystemTextJson(Json())
            .AddDistributedCacheAdapter(TimeSpan.FromMinutes(5), enableSlidingExpiration);
        more?.Invoke(services);
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task AsyncMembers_RoundTripUnderTheExternalKeyDomain()
    {
        var clock = new TestClock();
        var store = new InMemoryDistributedCacheStore(clock);
        await using var provider = Provider(store, clock);
        var cache = provider.GetRequiredService<IDistributedCache>();

        await cache.SetAsync(
            "k",
            "hello"u8.ToArray(),
            new DistributedCacheEntryOptions
            {
                AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(30),
            },
            Token
        );

        Assert.Equal("hello", Encoding.UTF8.GetString((await cache.GetAsync("k", Token))!));
        Assert.NotNull(await store.GetAsync("svc:cache:external:k", Token));
        Assert.Null(await store.GetAsync("svc:cache:data:k", Token));
        clock.Advance(TimeSpan.FromSeconds(31));
        Assert.Null(await cache.GetAsync("k", Token));

        await cache.SetAsync("k", "again"u8.ToArray(), new DistributedCacheEntryOptions(), Token);
        await cache.RefreshAsync("k", Token);
        await cache.RemoveAsync("k", Token);
        Assert.Null(await cache.GetAsync("k", Token));
    }

    [Fact]
    public async Task OptedAbsoluteEntries_DoNotRefresh()
    {
        var clock = new TestClock();
        var faults = new FaultingCacheStore(new InMemoryDistributedCacheStore(clock));
        var services = new ServiceCollection();
        services.AddSingleton(faults);
        services.AddSingleton<TimeProvider>(clock);
        services
            .AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseStore<FaultingCacheStore>("Faulting")
            .UseSystemTextJson(Json())
            .AddDistributedCacheAdapter(enableSlidingExpiration: true);
        await using var provider = services.BuildServiceProvider();
        var cache = provider.GetRequiredService<IDistributedCache>();
        var buffered = provider.GetRequiredService<IBufferDistributedCache>();
        var options = new DistributedCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(1),
        };
        await cache.SetAsync("catalog", [1], options, Token);
        faults.FailOperation(CacheFailureKind.Unavailable, "refresh");

        Assert.Equal([1], await cache.GetAsync("catalog", Token));
        Assert.True(await buffered.TryGetAsync("catalog", new ArrayBufferWriter<byte>(), Token));
        await cache.RefreshAsync("catalog", Token);
        Assert.Equal(0, faults.Faulted);
    }

    [Fact]
    public async Task OperationFailures_KeepTheirFilterUntilTheCountIsExhausted()
    {
        var store = new FaultingCacheStore(new InMemoryDistributedCacheStore());
        store.FailOperation(CacheFailureKind.Unavailable, "refresh", count: 2);

        await Assert.ThrowsAsync<CacheStoreException>(async () =>
            await store.RefreshAsync(
                "missing",
                ReadOnlyMemory<byte>.Empty,
                TimeSpan.FromMinutes(1),
                null,
                Token
            )
        );
        Assert.Null(await store.GetAsync("missing", Token));
        await Assert.ThrowsAsync<CacheStoreException>(async () =>
            await store.RefreshAsync(
                "missing",
                ReadOnlyMemory<byte>.Empty,
                TimeSpan.FromMinutes(1),
                null,
                Token
            )
        );
        Assert.Null(await store.GetAsync("missing", Token));
        Assert.Equal(2, store.Faulted);
        Assert.Equal(2, store.Calls);
    }

    [Fact]
    public async Task SynchronousMembers_ThrowNotSupported()
    {
        var clock = new TestClock();
        await using var provider = Provider(new InMemoryDistributedCacheStore(clock), clock);
        var cache = provider.GetRequiredService<IDistributedCache>();
        var buffered = provider.GetRequiredService<IBufferDistributedCache>();

        Assert.Throws<NotSupportedException>(() => cache.Get("k"));
        Assert.Throws<NotSupportedException>(() =>
            cache.Set("k", [], new DistributedCacheEntryOptions())
        );
        Assert.Throws<NotSupportedException>(() => cache.Refresh("k"));
        Assert.Throws<NotSupportedException>(() => cache.Remove("k"));
        Assert.Throws<NotSupportedException>(() =>
            buffered.TryGet("k", new ArrayBufferWriter<byte>())
        );
        Assert.Same(cache, buffered);
    }

    [Fact]
    public async Task Expiration_MapsAbsoluteAndDefault_SlidingThrowsWithoutOptIn()
    {
        var clock = new TestClock();
        var store = new InMemoryDistributedCacheStore(clock);
        await using var provider = Provider(store, clock);
        var cache = provider.GetRequiredService<IDistributedCache>();

        await cache.SetAsync(
            "absolute",
            [1],
            new DistributedCacheEntryOptions
            {
                AbsoluteExpiration = clock.GetUtcNow().AddMinutes(2),
            },
            Token
        );
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            cache.SetAsync(
                "sliding",
                [1],
                new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromMinutes(1) },
                Token
            )
        );
        await cache.SetAsync("default", [1], new DistributedCacheEntryOptions(), Token);
        await cache.SetAsync(
            "expired",
            [1],
            new DistributedCacheEntryOptions
            {
                AbsoluteExpiration = clock.GetUtcNow().AddMinutes(-1),
            },
            Token
        );

        Assert.Equal(
            TimeSpan.FromMinutes(2),
            (await store.GetAsync("svc:cache:external:absolute", Token))!.Value.RemainingTimeToLive
        );
        Assert.Null(await store.GetAsync("svc:cache:external:sliding", Token));
        Assert.Equal(
            TimeSpan.FromMinutes(5),
            (await store.GetAsync("svc:cache:external:default", Token))!.Value.RemainingTimeToLive
        );
        Assert.Null(await store.GetAsync("svc:cache:external:expired", Token));
    }

    [Fact]
    public async Task StoreFailure_IsAMissAndNeverThrows()
    {
        var clock = new TestClock();
        var faults = new FaultingCacheStore(new InMemoryDistributedCacheStore(clock));
        var services = new ServiceCollection();
        services.AddSingleton(faults);
        services.AddSingleton<TimeProvider>(clock);
        services
            .AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseStore<FaultingCacheStore>("Faulting")
            .UseSystemTextJson(Json())
            .AddDistributedCacheAdapter();
        await using var provider = services.BuildServiceProvider();
        var cache = provider.GetRequiredService<IDistributedCache>();
        faults.FailAll(CacheFailureKind.Unavailable);

        await cache.SetAsync("k", [1], new DistributedCacheEntryOptions(), Token);
        var read = await cache.GetAsync("k", Token);
        var buffered = await provider
            .GetRequiredService<IBufferDistributedCache>()
            .TryGetAsync("k", new ArrayBufferWriter<byte>(), Token);
        await cache.RemoveAsync("k", Token);

        Assert.Null(read);
        Assert.False(buffered);
    }

    [Fact]
    public async Task RepeatedStoreFailures_AreCountedAndLoggedOncePerInterval()
    {
        var clock = new TestClock();
        var faults = new FaultingCacheStore(new InMemoryDistributedCacheStore(clock));
        using var logs = new CapturingLoggerProvider();
        var services = new ServiceCollection();
        services.AddSingleton(faults);
        services.AddSingleton<TimeProvider>(clock);
        services.AddLogging(logging => logging.AddProvider(logs));
        services
            .AddHostLoomCaching(caching => caching.Namespace = "adapter-storm")
            .UseStore<FaultingCacheStore>("Faulting")
            .UseSystemTextJson(Json())
            .AddDistributedCacheAdapter();
        await using var provider = services.BuildServiceProvider();
        var cache = provider.GetRequiredService<IDistributedCache>();
        using var errors = new ErrorRecorder("adapter-storm");
        faults.FailAll(CacheFailureKind.Unavailable);

        for (var i = 0; i < 5; i++)
        {
            await cache.GetAsync("k", Token);
        }

        var duringTheOutage = logs.Count(1007);
        clock.Advance(TimeSpan.FromMinutes(1));
        await cache.GetAsync("k", Token);

        // An outage must not turn every request into a log line; the metric still counts them all.
        Assert.Equal(1, duringTheOutage);
        Assert.Equal(2, logs.Count(1007));
        Assert.Equal(6, errors.Total);
        Assert.Equal(["unavailable"], errors.Kinds);
    }

    [Fact]
    public async Task Write_AboveMaxPayloadBytes_IsNotStoredAndIsCounted()
    {
        var clock = new TestClock();
        var store = new InMemoryDistributedCacheStore(clock);
        using var logs = new CapturingLoggerProvider();
        var services = new ServiceCollection();
        services.AddSingleton(store);
        services.AddSingleton<TimeProvider>(clock);
        services.AddLogging(logging => logging.AddProvider(logs));
        services
            .AddHostLoomCaching(caching =>
            {
                caching.Namespace = "adapter-bound";
                caching.MaxPayloadBytes = 16;
            })
            .UseStore<InMemoryDistributedCacheStore>("InMemoryDistributed")
            .UseSystemTextJson(Json())
            .AddDistributedCacheAdapter();
        await using var provider = services.BuildServiceProvider();
        var cache = provider.GetRequiredService<IDistributedCache>();
        using var errors = new ErrorRecorder("adapter-bound");

        await cache.SetAsync("small", new byte[16], new DistributedCacheEntryOptions(), Token);
        await cache.SetAsync("large", new byte[17], new DistributedCacheEntryOptions(), Token);
        await cache.SetAsync("large", new byte[64], new DistributedCacheEntryOptions(), Token);

        // The consumer's bytes are stored as they are, so the bound applies to them directly;
        // one error line per key per interval, every rejection counted.
        Assert.NotNull(await cache.GetAsync("small", Token));
        Assert.Null(await cache.GetAsync("large", Token));
        Assert.Null(await store.GetAsync("adapter-bound:cache:external:large", Token));
        Assert.Equal(1, logs.Count(1008));
        Assert.Equal(2, errors.Total);
        Assert.Equal(["payload"], errors.Kinds);
    }

    [Fact]
    public async Task OptedWrite_MaxPayloadBytesIncludesTheEnvelope()
    {
        var clock = new TestClock();
        var store = new InMemoryDistributedCacheStore(clock);
        using var logs = new CapturingLoggerProvider();
        var services = new ServiceCollection();
        services.AddSingleton(store);
        services.AddSingleton<TimeProvider>(clock);
        services.AddLogging(logging => logging.AddProvider(logs));
        services
            .AddHostLoomCaching(caching =>
            {
                caching.Namespace = "adapter-envelope";
                caching.MaxPayloadBytes = 48;
            })
            .UseStore<InMemoryDistributedCacheStore>("InMemoryDistributed")
            .UseSystemTextJson(Json())
            .AddDistributedCacheAdapter(enableSlidingExpiration: true);
        await using var provider = services.BuildServiceProvider();
        var cache = provider.GetRequiredService<IDistributedCache>();
        using var errors = new ErrorRecorder("adapter-envelope");
        var options = new DistributedCacheEntryOptions
        {
            SlidingExpiration = TimeSpan.FromMinutes(1),
        };

        await cache.SetAsync("small", new byte[8], options, Token);
        await cache.SetAsync("large", new byte[9], options, Token);

        var small = await store.GetAsync("adapter-envelope:cache:external-v2:small", Token);
        Assert.Equal(48, small!.Value.Payload.Length);
        Assert.Null(await store.GetAsync("adapter-envelope:cache:external-v2:large", Token));
        Assert.Equal(1, logs.Count(1008));
        Assert.Equal(1, errors.Total);
    }

    [Fact]
    public async Task OptedEntries_AreMissesForTheLegacyAdapter()
    {
        var clock = new TestClock();
        var store = new InMemoryDistributedCacheStore(clock);
        await using var opted = Provider(store, clock, enableSlidingExpiration: true);
        await using var legacy = Provider(store, clock);
        var options = new DistributedCacheEntryOptions
        {
            SlidingExpiration = TimeSpan.FromMinutes(1),
        };

        await opted
            .GetRequiredService<IDistributedCache>()
            .SetAsync("catalog", [1], options, Token);

        Assert.Null(
            await legacy.GetRequiredService<IDistributedCache>().GetAsync("catalog", Token)
        );
        Assert.Equal(
            [1],
            await opted.GetRequiredService<IDistributedCache>().GetAsync("catalog", Token)
        );
    }

    [Fact]
    public async Task VersionMismatch_IsAnObservableMissWithoutLegacyFallback()
    {
        var clock = new TestClock();
        var store = new InMemoryDistributedCacheStore(clock);
        using var logs = new CapturingLoggerProvider();
        await using var provider = Provider(
            store,
            clock,
            services => services.AddLogging(logging => logging.AddProvider(logs)),
            enableSlidingExpiration: true
        );
        var cache = provider.GetRequiredService<IDistributedCache>();
        using var errors = new ErrorRecorder("svc");
        await cache.SetAsync(
            "catalog",
            [1],
            new DistributedCacheEntryOptions { SlidingExpiration = TimeSpan.FromMinutes(1) },
            Token
        );
        const string key = "svc:cache:external-v2:catalog";
        var payload = (await store.GetAsync(key, Token))!.Value.Payload.ToArray();
        payload[4] = 2;
        await store.SetAsync(key, payload, TimeSpan.FromMinutes(1), cancellationToken: Token);
        await store.SetAsync(
            "svc:cache:external:catalog",
            new byte[] { 2 },
            TimeSpan.FromMinutes(1),
            cancellationToken: Token
        );

        Assert.Null(await cache.GetAsync("catalog", Token));
        Assert.Equal(1, logs.Count(1009));
        Assert.Equal(1, errors.Total);
        Assert.Equal(["payload"], errors.Kinds);
    }

    [Fact]
    public void UnsupportedStore_ResolutionNamesTheProfileAndCapability()
    {
        var faults = new FaultingCacheStore(NSubstitute.Substitute.For<IDistributedCacheStore>());
        var services = new ServiceCollection();
        services.AddKeyedSingleton("catalog", faults);
        services
            .AddHostLoomCaching("catalog", caching => caching.Namespace = "catalog")
            .UseStore<FaultingCacheStore>("Faulting")
            .UseSystemTextJson(Json())
            .AddDistributedCacheAdapter(enableSlidingExpiration: true);
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            provider.GetRequiredKeyedService<IDistributedCache>("catalog")
        );

        Assert.Contains("catalog", exception.Message, StringComparison.Ordinal);
        Assert.Contains("SlidingExpiration", exception.Message, StringComparison.Ordinal);
        Assert.Contains(
            "ISlidingExpirationCacheStore",
            exception.Message,
            StringComparison.Ordinal
        );
    }

    [Fact]
    public void WithoutADistributedStore_ResolvingTheAdapterExplainsWhat()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseInMemory()
            .AddDistributedCacheAdapter();
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            provider.GetRequiredService<IDistributedCache>()
        );

        Assert.Contains("UseStore", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task HybridCache_UsesTheHostLoomStoreAsItsDistributedTier()
    {
        var clock = new TestClock();
        var store = new InMemoryDistributedCacheStore(clock);
        await using var first = Provider(store, clock, services => services.AddHybridCache());
        await using var second = Provider(store, clock, services => services.AddHybridCache());
        var options = new HybridCacheEntryOptions { Expiration = TimeSpan.FromMinutes(5) };
        var factoryRuns = 0;

        // SetAsync completes only after HybridCache has written the distributed tier, whereas
        // GetOrCreateAsync releases its caller before that write lands. Seeding through SetAsync
        // keeps the second provider's read deterministic.
        await first
            .GetRequiredService<HybridCache>()
            .SetAsync("catalog", new Catalog("eu", 3), options, cancellationToken: Token);
        var fromStore = await second
            .GetRequiredService<HybridCache>()
            .GetOrCreateAsync(
                "catalog",
                _ =>
                {
                    factoryRuns++;
                    return ValueTask.FromResult(new Catalog("miss", 0));
                },
                options,
                cancellationToken: Token
            );

        Assert.NotNull(await store.GetAsync("svc:cache:external:catalog", Token));
        Assert.Equal(new Catalog("eu", 3), fromStore);
        Assert.Equal(0, factoryRuns);
    }

    private sealed record Catalog(string Region, int Items);

    /// <summary>Captures every event the adapter logs, whatever category it was created under.</summary>
    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        private readonly Lock _gate = new();
        private readonly List<EventId> _events = [];

        public int Count(int eventId)
        {
            lock (_gate)
            {
                return _events.Count(entry => entry.Id == eventId);
            }
        }

        public ILogger CreateLogger(string categoryName) => new Capturing(this);

        public void Dispose() { }

        private void Add(EventId eventId)
        {
            lock (_gate)
            {
                _events.Add(eventId);
            }
        }

        private sealed class Capturing(CapturingLoggerProvider owner) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter
            ) => owner.Add(eventId);
        }
    }

    /// <summary>Reads <c>hostloom.cache.errors</c> for one namespace.</summary>
    private sealed class ErrorRecorder : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly Lock _gate = new();
        private readonly List<string> _kinds = [];

        public ErrorRecorder(string @namespace)
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Name == "hostloom.cache.errors")
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>(
                (_, _, tags, _) =>
                {
                    string? ns = null;
                    string? kind = null;
                    foreach (var tag in tags)
                    {
                        if (tag.Key == "hostloom.cache.namespace")
                        {
                            ns = tag.Value as string;
                        }
                        else if (tag.Key == "hostloom.cache.kind")
                        {
                            kind = tag.Value as string;
                        }
                    }

                    if (ns != @namespace)
                    {
                        return;
                    }

                    lock (_gate)
                    {
                        _kinds.Add(kind ?? "");
                    }
                }
            );
            _listener.Start();
        }

        public int Total
        {
            get
            {
                lock (_gate)
                {
                    return _kinds.Count;
                }
            }
        }

        public IReadOnlyList<string> Kinds
        {
            get
            {
                lock (_gate)
                {
                    return [.. _kinds.Distinct()];
                }
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
