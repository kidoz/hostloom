using System.Collections;
using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using HostLoom.Caching;
using HostLoom.Caching.Internal;
using HostLoom.Caching.Testing;
using HostLoom.Conformance;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

public sealed class CachingOptionsTests
{
    [Fact]
    public void Validate_ReportsEachProblemNamingTheOptionKey()
    {
        var options = new CachingOptions { Namespace = "Bad Name", MaxKeyLength = 0 };
        options.L1.MaxEntries = 0;
        options.Stampede.LeaseDuration = TimeSpan.Zero;
        options.Invalidation.MaxPending = 0;

        var problems = options.Validate();

        Assert.Contains(problems, p => p.StartsWith("Caching:Namespace", StringComparison.Ordinal));
        Assert.Contains(
            problems,
            p => p.StartsWith("Caching:MaxKeyLength", StringComparison.Ordinal)
        );
        Assert.Contains(
            problems,
            p => p.StartsWith("Caching:L1:MaxEntries", StringComparison.Ordinal)
        );
        Assert.Contains(
            problems,
            p => p.StartsWith("Caching:Stampede:LeaseDuration", StringComparison.Ordinal)
        );
        Assert.Contains(
            problems,
            p => p.StartsWith("Caching:Invalidation:MaxPending", StringComparison.Ordinal)
        );
    }

    [Fact]
    public void Defaults_ReproduceThePlatformConstants()
    {
        var options = new CachingOptions { Namespace = "svc" };

        Assert.Empty(options.Validate());
        Assert.Equal(10_000, options.L1.MaxEntries);
        Assert.Equal(TimeSpan.FromMinutes(30), options.L1.MaxEntryAge);
        Assert.Equal(TimeSpan.FromMinutes(1), options.L1.CleanupInterval);
        Assert.Equal(TimeSpan.FromSeconds(30), options.Stampede.LeaseDuration);
        Assert.Equal(1_024, options.Compression.ThresholdBytes);
        Assert.Equal(100, options.Warmup.BatchSize);
        Assert.Equal(512, options.MaxKeyLength);
    }
}

public sealed class CacheKeyTests
{
    [Fact]
    public void FromSensitive_HashesTo32HexCharactersDeterministically()
    {
        var first = CacheKey.FromSensitive("refresh-token-1");
        var again = CacheKey.FromSensitive("refresh-token-1");
        var other = CacheKey.FromSensitive("refresh-token-2");

        Assert.Equal(32, first.Length);
        Assert.Matches("^[0-9a-f]{32}$", first);
        Assert.Equal(first, again);
        Assert.NotEqual(first, other);
        Assert.DoesNotContain("refresh", first, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData("has space")]
    [InlineData("has\ttab")]
    [InlineData("has\u0001control")]
    public void Validate_RejectsEmptyWhitespaceAndControlCharacters(string key) =>
        Assert.Throws<ArgumentException>(() => CacheKey.Validate(key, 512));

    [Fact]
    public void Validate_RejectsKeysOverTheMaximumLength() =>
        Assert.Throws<ArgumentException>(() => CacheKey.Validate(new string('k', 513), 512));

    [Fact]
    public void Versioned_AppendsTheVersionAsAnOrdinaryKeySegment() =>
        Assert.Equal("catalog:eu:v2", CacheKey.Versioned("catalog:eu", "2"));
}

public sealed class LocalCacheStoreTests
{
    private readonly TestClock _clock = new();

    [Fact]
    public void TryGet_ValueOfAnotherType_IsAMissAndEvicts()
    {
        using var store = new LocalCacheStore(new CacheL1Options(), _clock);
        store.Set("k", "text", TimeSpan.FromMinutes(1));

        Assert.False(store.TryGet<int>("k", out _));
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public void TryGet_AfterExpiry_IsAMiss()
    {
        using var store = new LocalCacheStore(new CacheL1Options(), _clock);
        store.Set("k", 5, TimeSpan.FromSeconds(10));

        _clock.Advance(TimeSpan.FromSeconds(9));
        Assert.True(store.TryGet<int>("k", out var value));
        Assert.Equal(5, value);

        _clock.Advance(TimeSpan.FromSeconds(2));
        Assert.False(store.TryGet<int>("k", out _));
    }

    [Fact]
    public void Set_AtCapacity_EvictsASampledLeastRecentlyAccessedFraction()
    {
        var options = new CacheL1Options { MaxEntries = 8, EvictionFraction = 0.25 };
        using var store = new LocalCacheStore(options, _clock);
        for (var i = 0; i < 8; i++)
        {
            store.Set($"k{i}", i, TimeSpan.FromMinutes(1));
            _clock.Advance(TimeSpan.FromMilliseconds(1));
        }

        // Touch the oldest entries so recency, not insertion order, decides what survives.
        store.TryGet<int>("k0", out _);
        store.TryGet<int>("k1", out _);
        _clock.Advance(TimeSpan.FromMilliseconds(1));
        store.Set("k8", 8, TimeSpan.FromMinutes(1));

        Assert.True(store.Count <= 7);
        Assert.True(store.TryGet<int>("k0", out _));
        Assert.True(store.TryGet<int>("k1", out _));
        Assert.True(store.TryGet<int>("k8", out _));
    }

    [Fact]
    public void CleanupTimer_RemovesExpiredEntriesOnTheClock()
    {
        var options = new CacheL1Options { CleanupInterval = TimeSpan.FromSeconds(30) };
        using var store = new LocalCacheStore(options, _clock);
        store.Set("k", 1, TimeSpan.FromSeconds(5));

        _clock.Advance(TimeSpan.FromSeconds(30));

        Assert.Equal(0, store.Count);
    }

    [Fact]
    public void RemoveByTag_EvictsOnlyTaggedEntries()
    {
        using var store = new LocalCacheStore(new CacheL1Options(), _clock);
        store.Set("a", 1, TimeSpan.FromMinutes(1), ["t"]);
        store.Set("b", 2, TimeSpan.FromMinutes(1));

        store.RemoveByTag("t");

        Assert.False(store.TryGet<int>("a", out _));
        Assert.True(store.TryGet<int>("b", out _));
    }

    [Fact]
    public void SetIfAbsent_KeepsALiveEntryAndReplacesAnExpiredOne()
    {
        using var store = new LocalCacheStore(new CacheL1Options(), _clock);
        Assert.True(store.SetIfAbsent("k", 1, TimeSpan.FromSeconds(5)));
        Assert.False(store.SetIfAbsent("k", 2, TimeSpan.FromSeconds(5)));

        _clock.Advance(TimeSpan.FromSeconds(6));

        Assert.True(store.SetIfAbsent("k", 3, TimeSpan.FromSeconds(5)));
        Assert.True(store.TryGet<int>("k", out var value));
        Assert.Equal(3, value);
    }

    [Fact]
    public void MaxBytes_EvictsWhenTheApproximateSizeIsExceeded()
    {
        var options = new CacheL1Options { MaxBytes = 100 };
        using var store = new LocalCacheStore(options, _clock);
        for (var i = 0; i < 5; i++)
        {
            store.Set($"k{i}", i, TimeSpan.FromMinutes(1), size: 40);
            _clock.Advance(TimeSpan.FromMilliseconds(1));
        }

        Assert.True(store.ApproximateBytes <= 100);
    }
}

public sealed class CachePayloadCodecTests
{
    private const long Limit = 10 * 1024 * 1024;

    private static readonly ICacheValueSerializer Serializer =
        SystemTextJsonCacheValueSerializer.CreateReflectionBased();

    [Fact]
    public void Encode_SmallPayload_WritesVersionHeaderWithoutCompression()
    {
        using var writer = new PooledBufferWriter();
        var compressed = CachePayloadCodec.Encode(
            Serializer,
            new Sample("x"),
            null,
            1_024,
            writer,
            out var bodyLength
        );

        Assert.False(compressed);
        Assert.Equal(writer.WrittenCount - 1, bodyLength);
        Assert.Equal(0x10, writer.WrittenSpan[0]);
        var status = CachePayloadCodec.TryDecode<Sample>(
            Serializer,
            writer.WrittenSpan,
            Limit,
            out var value,
            out var tags,
            out _
        );
        Assert.Equal(PayloadDecodeStatus.Ok, status);
        Assert.Equal("x", value!.Text);
        Assert.Null(tags);
    }

    [Fact]
    public void Encode_LargePayload_CompressesAndRoundTrips()
    {
        var sample = new Sample(new string('a', 5_000));
        using var writer = new PooledBufferWriter();
        var compressed = CachePayloadCodec.Encode(
            Serializer,
            sample,
            ["a", "b"],
            1_024,
            writer,
            out var bodyLength
        );

        Assert.True(compressed);
        Assert.Equal(0x13, writer.WrittenSpan[0]);
        Assert.True(writer.WrittenCount < 5_000);
        Assert.True(bodyLength > 5_000);
        var status = CachePayloadCodec.TryDecode<Sample>(
            Serializer,
            writer.WrittenSpan,
            Limit,
            out var value,
            out var tags,
            out _
        );
        Assert.Equal(PayloadDecodeStatus.Ok, status);
        Assert.Equal(sample, value);
        Assert.Equal(["a", "b"], tags!);
    }

    [Fact]
    public void TryDecode_OtherFormatVersion_IsASilentVersionMismatch()
    {
        byte[] payload = [0x20, (byte)'{', (byte)'}'];
        var status = CachePayloadCodec.TryDecode<Sample>(
            Serializer,
            payload,
            Limit,
            out _,
            out _,
            out var failure
        );

        Assert.Equal(PayloadDecodeStatus.VersionMismatch, status);
        Assert.Null(failure);
    }

    [Fact]
    public void TryDecode_MalformedBody_ReportsCorruptWithTheFailure()
    {
        var payload = new byte[] { 0x10 }
            .Concat(Encoding.UTF8.GetBytes("not json"))
            .ToArray();
        var status = CachePayloadCodec.TryDecode<Sample>(
            Serializer,
            payload,
            Limit,
            out _,
            out _,
            out var failure
        );

        Assert.Equal(PayloadDecodeStatus.Corrupt, status);
        Assert.NotNull(failure);
    }

    [Fact]
    public void TryDecode_DeclaredLengthAboveTheLimit_IsCorruptAndAllocatesNothing()
    {
        // A poisoned entry: the compressed-flag header, then a declared uncompressed length of
        // 4 GB. Trusting it would rent the buffer before a single byte is decompressed.
        byte[] payload = [0x11, 0xFF, 0xFF, 0xFF, 0xFF, 0x00];

        var status = CachePayloadCodec.TryDecode<Sample>(
            Serializer,
            payload,
            1_024,
            out var value,
            out _,
            out var failure
        );

        Assert.Equal(PayloadDecodeStatus.Corrupt, status);
        Assert.Null(value);
        Assert.IsType<InvalidDataException>(failure);
        Assert.Contains("Caching:MaxPayloadBytes", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TryDecode_BodyWithinTheLimit_StillRoundTrips()
    {
        var sample = new Sample(new string('a', 5_000));
        using var writer = new PooledBufferWriter();
        CachePayloadCodec.Encode(Serializer, sample, null, 1_024, writer, out var bodyLength);

        var status = CachePayloadCodec.TryDecode<Sample>(
            Serializer,
            writer.WrittenSpan,
            bodyLength,
            out var value,
            out _,
            out _
        );

        Assert.Equal(PayloadDecodeStatus.Ok, status);
        Assert.Equal(sample, value);
    }

    private sealed record Sample(string Text);
}

public sealed class SystemTextJsonCacheValueSerializerTests
{
    [Fact]
    public void Constructor_WithoutTypeInfoResolver_Throws()
    {
        var exception = Assert.Throws<ArgumentException>(() =>
            new SystemTextJsonCacheValueSerializer(new JsonSerializerOptions())
        );
        Assert.Contains("TypeInfoResolver", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceGeneratedContext_RoundTripsWithoutReflection()
    {
        var serializer = new SystemTextJsonCacheValueSerializer(
            new JsonSerializerOptions { TypeInfoResolver = CachingTestJsonContext.Default }
        );
        using var writer = new PooledBufferWriter();
        serializer.Serialize(writer, new CachingTestPayload("hello", 3));

        var value = serializer.Deserialize<CachingTestPayload>(writer.WrittenSpan);

        Assert.Equal(new CachingTestPayload("hello", 3), value);
    }

    [Fact]
    public void PlatformProfile_UsesCamelCaseAndOmitsNulls()
    {
        var serializer = SystemTextJsonCacheValueSerializer.CreateReflectionBased();
        using var writer = new PooledBufferWriter();
        serializer.Serialize(writer, new CachingTestPayload("hello", 3) { Optional = null });

        var json = Encoding.UTF8.GetString(writer.WrittenSpan);

        Assert.Contains("\"text\"", json, StringComparison.Ordinal);
        Assert.DoesNotContain("optional", json, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(JsonNamingPolicy.CamelCase, serializer.Options.PropertyNamingPolicy);
    }
}

public sealed record CachingTestPayload(string Text, int Number)
{
    public string? Optional { get; init; }
}

[JsonSerializable(typeof(CachingTestPayload))]
internal sealed partial class CachingTestJsonContext : JsonSerializerContext;

public sealed class TieredCacheTests
{
    private readonly TestClock _clock = new();
    private readonly ICacheValueSerializer _serializer =
        SystemTextJsonCacheValueSerializer.CreateReflectionBased();

    private static CachingOptions Options(string ns = "svc") => new() { Namespace = ns };

    [Fact]
    public void Constructor_StoreWithoutSerializer_Throws()
    {
        var store = new InMemoryDistributedCacheStore(_clock);
        Assert.Throws<ArgumentException>(() =>
            new TieredCache(Options(), store, timeProvider: _clock)
        );
    }

    [Fact]
    public void Constructor_L1DisabledWithoutStore_Throws()
    {
        var options = Options();
        options.L1.Enabled = false;
        var exception = Assert.Throws<ArgumentException>(() =>
            new TieredCache(options, timeProvider: _clock)
        );
        Assert.Contains("Caching:L1:Enabled", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Constructor_InvalidOptions_ThrowsNamingTheOptionKey()
    {
        var exception = Assert.Throws<ArgumentException>(() =>
            new TieredCache(Options("Bad Name"))
        );
        Assert.Contains("Caching:Namespace", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetOrCreate_L1Hit_RecordsAHitL1Outcome()
    {
        using var metrics = new CacheMetricRecorder("metrics-l1");
        await using var cache = new TieredCache(Options("metrics-l1"), timeProvider: _clock);
        await cache.GetOrCreateAsync(
            "k",
            _ => ValueTask.FromResult(1),
            TimeSpan.FromMinutes(1),
            cancellationToken: TestContext.Current.CancellationToken
        );

        await cache.GetOrCreateAsync(
            "k",
            _ => ValueTask.FromResult(2),
            TimeSpan.FromMinutes(1),
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Contains(("hostloom.cache.operation.duration", "hit_l1"), metrics.Outcomes);
        Assert.Contains(("hostloom.cache.operation.duration", "miss"), metrics.Outcomes);
        Assert.Single(metrics.Values("hostloom.cache.factory.duration"));
    }

    [Fact]
    public async Task GetOrCreate_LeaseHeldByAnotherInstance_RechecksThenRunsTheFactory()
    {
        using var metrics = new CacheMetricRecorder("lease");
        var store = new InMemoryDistributedCacheStore(_clock);
        var options = Options("lease");
        options.Stampede.WaitBeforeFallback = TimeSpan.Zero;
        await using var cache = new TieredCache(options, store, _serializer, timeProvider: _clock);
        await store.SetIfAbsentAsync(
            "lease:cache:lease:k",
            new byte[] { 1 },
            TimeSpan.FromSeconds(30),
            cancellationToken: TestContext.Current.CancellationToken
        );

        var value = await cache.GetOrCreateAsync(
            "k",
            _ => ValueTask.FromResult(7),
            TimeSpan.FromMinutes(1),
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Equal(7, value);
        Assert.Equal(1, metrics.Values("hostloom.cache.stampede.lease_missed").Sum());
        Assert.NotNull(
            await store.GetAsync("lease:cache:data:k", TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task GetOrCreate_ReleasesTheLeaseAfterWriting()
    {
        var store = new InMemoryDistributedCacheStore(_clock);
        await using var cache = new TieredCache(
            Options(),
            store,
            _serializer,
            timeProvider: _clock
        );

        await cache.GetOrCreateAsync(
            "k",
            _ => ValueTask.FromResult(7),
            TimeSpan.FromMinutes(1),
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Null(
            await store.GetAsync("svc:cache:lease:k", TestContext.Current.CancellationToken)
        );
        Assert.NotNull(
            await store.GetAsync("svc:cache:data:k", TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task PayloadVersion_IsAppendedToTheDataKey()
    {
        var store = new InMemoryDistributedCacheStore(_clock);
        var options = Options();
        options.PayloadVersion = "2";
        await using var cache = new TieredCache(options, store, _serializer, timeProvider: _clock);

        await cache.SetAsync(
            "k",
            1,
            new CacheEntryOptions(TimeSpan.FromMinutes(1)),
            TestContext.Current.CancellationToken
        );

        Assert.NotNull(
            await store.GetAsync("svc:cache:data:k:2", TestContext.Current.CancellationToken)
        );
        Assert.Null(
            await store.GetAsync("svc:cache:data:k", TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task Tags_UseTheTagDomainInTheStore()
    {
        var store = new InMemoryDistributedCacheStore(_clock);
        await using var cache = new TieredCache(
            Options(),
            store,
            _serializer,
            timeProvider: _clock
        );
        await cache.SetAsync(
            "k",
            1,
            new CacheEntryOptions(TimeSpan.FromMinutes(1)) { Tags = ["t"] },
            TestContext.Current.CancellationToken
        );

        await store.RemoveByTagAsync("svc:cache:tag:t", TestContext.Current.CancellationToken);

        Assert.Null(
            await store.GetAsync("svc:cache:data:k", TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task Compression_AboveThreshold_CompressesAndCounts()
    {
        using var metrics = new CacheMetricRecorder("compress");
        var store = new InMemoryDistributedCacheStore(_clock);
        await using var cache = new TieredCache(
            Options("compress"),
            store,
            _serializer,
            timeProvider: _clock
        );
        var big = new CachingTestPayload(new string('z', 4_000), 1);

        await cache.SetAsync(
            "k",
            big,
            new CacheEntryOptions(TimeSpan.FromMinutes(1)),
            TestContext.Current.CancellationToken
        );

        var stored = await store.GetAsync(
            "compress:cache:data:k",
            TestContext.Current.CancellationToken
        );
        Assert.True(stored!.Value.Payload.Length < 1_000);
        Assert.Equal(1, metrics.Values("hostloom.cache.compressions").Sum());
        await using var other = new TieredCache(
            Options("compress"),
            store,
            _serializer,
            timeProvider: _clock
        );
        Assert.Equal(
            big,
            await other.GetAsync<CachingTestPayload>("k", TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task MaxPayloadBytes_Exceeded_KeepsTheValueInL1Only()
    {
        var store = new InMemoryDistributedCacheStore(_clock);
        var options = Options();
        options.MaxPayloadBytes = 64;
        await using var cache = new TieredCache(options, store, _serializer, timeProvider: _clock);

        await cache.SetAsync(
            "k",
            new CachingTestPayload(new string('z', 200), 1),
            new CacheEntryOptions(TimeSpan.FromMinutes(1)),
            TestContext.Current.CancellationToken
        );

        Assert.Null(
            await store.GetAsync("svc:cache:data:k", TestContext.Current.CancellationToken)
        );
        Assert.Equal(
            CacheTier.L1,
            (
                await cache.TryGetAsync<CachingTestPayload>(
                    "k",
                    TestContext.Current.CancellationToken
                )
            ).Tier
        );
    }

    [Fact]
    public async Task MaxPayloadBytes_CountsTheBodyBeforeCompression()
    {
        var store = new InMemoryDistributedCacheStore(_clock);
        var options = Options();
        options.MaxPayloadBytes = 1_000;
        var logger = new RecordingLogger<TieredCache>();
        await using var cache = new TieredCache(
            options,
            store,
            _serializer,
            timeProvider: _clock,
            logger: logger
        );

        // Compresses to well under the bound, but a reader would have to rent the whole body: the
        // two sides of the bound have to agree or such an entry could never be read back.
        await cache.SetAsync(
            "k",
            new CachingTestPayload(new string('z', 8_000), 1),
            new CacheEntryOptions(TimeSpan.FromMinutes(1)),
            TestContext.Current.CancellationToken
        );

        Assert.Null(
            await store.GetAsync("svc:cache:data:k", TestContext.Current.CancellationToken)
        );
        Assert.Equal(1003, Assert.Single(logger.Entries).Event.Id);
    }

    [Fact]
    public async Task PoisonedLengthPrefix_IsAMissAndRentsNothing()
    {
        var logger = new RecordingLogger<TieredCache>();
        var store = new InMemoryDistributedCacheStore(_clock);
        var options = Options();
        options.MaxPayloadBytes = 4_096;
        await using var cache = new TieredCache(
            options,
            store,
            _serializer,
            timeProvider: _clock,
            logger: logger
        );

        // Compressed flag, then a declared uncompressed length of 4 GB.
        await store.SetAsync(
            "svc:cache:data:k",
            new byte[] { 0x11, 0xFF, 0xFF, 0xFF, 0xFF, 0x00 },
            TimeSpan.FromMinutes(1),
            cancellationToken: TestContext.Current.CancellationToken
        );

        var lookup = await cache.TryGetAsync<CachingTestPayload>(
            "k",
            TestContext.Current.CancellationToken
        );

        Assert.False(lookup.Found);
        var entry = Assert.Single(logger.Entries);
        Assert.Equal(1002, entry.Event.Id);
        Assert.IsType<InvalidDataException>(entry.Exception);
    }

    [Fact]
    public async Task SetIfAbsent_WithTags_IsRemovedByTagInBothTiers()
    {
        var store = new InMemoryDistributedCacheStore(_clock);
        await using var cache = new TieredCache(
            Options(),
            store,
            _serializer,
            timeProvider: _clock
        );

        Assert.True(
            await cache.SetIfAbsentAsync(
                "k",
                1,
                new CacheEntryOptions(TimeSpan.FromMinutes(1)) { Tags = ["t"] },
                TestContext.Current.CancellationToken
            )
        );
        await cache.RemoveByTagAsync("t", TestContext.Current.CancellationToken);

        Assert.Null(
            await store.GetAsync("svc:cache:data:k", TestContext.Current.CancellationToken)
        );
        Assert.False(
            (await cache.TryGetAsync<int>("k", TestContext.Current.CancellationToken)).Found
        );
    }

    [Fact]
    public async Task SetIfAbsent_WithoutAStore_TagsTheInProcessEntry()
    {
        await using var cache = new TieredCache(Options(), timeProvider: _clock);
        await cache.SetIfAbsentAsync(
            "k",
            1,
            new CacheEntryOptions(TimeSpan.FromMinutes(1)) { Tags = ["t"] },
            TestContext.Current.CancellationToken
        );

        await cache.RemoveByTagAsync("t", TestContext.Current.CancellationToken);

        Assert.False(
            (await cache.TryGetAsync<int>("k", TestContext.Current.CancellationToken)).Found
        );
    }

    [Fact]
    public async Task AFactoryThatOutlivesTheLease_DoesNotReleaseItsSuccessor()
    {
        var store = new InMemoryDistributedCacheStore(_clock);
        var options = Options();
        options.Stampede.LeaseDuration = TimeSpan.FromSeconds(30);
        await using var cache = new TieredCache(options, store, _serializer, timeProvider: _clock);

        await cache.GetOrCreateAsync(
            "k",
            async _ =>
            {
                // The lease runs out while the factory is still working, and another instance
                // takes it. Releasing on the way out would delete that instance's lease.
                _clock.Advance(TimeSpan.FromSeconds(31));
                await store.SetIfAbsentAsync(
                    "svc:cache:lease:k",
                    new byte[] { 1 },
                    TimeSpan.FromSeconds(30),
                    cancellationToken: TestContext.Current.CancellationToken
                );
                return 7;
            },
            TimeSpan.FromMinutes(1),
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.NotNull(
            await store.GetAsync("svc:cache:lease:k", TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task AFullInvalidationQueue_ReportsTheDropRatherThanSwallowingIt()
    {
        using var metrics = new CacheMetricRecorder("drops");
        var logger = new RecordingLogger<TieredCache>();
        var store = new InMemoryDistributedCacheStore(_clock);
        var options = Options("drops");
        options.Invalidation.MaxPending = 1;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var cache = new TieredCache(
            options,
            store,
            _serializer,
            timeProvider: _clock,
            logger: logger
        );

        // The first message is taken by the applying loop, which then stops inside it: the queue
        // holds the second and has to drop the third. DropWrite reports that write as a success,
        // which is exactly what used to make the loss invisible.
        await store.PublishAsync(
            new CacheInvalidation(new BlockingKeys(entered, release), []),
            TestContext.Current.CancellationToken
        );
        await entered.Task.WaitAsync(
            TimeSpan.FromSeconds(10),
            TestContext.Current.CancellationToken
        );
        await store.PublishAsync(
            new CacheInvalidation(["queued"], []),
            TestContext.Current.CancellationToken
        );
        await store.PublishAsync(
            new CacheInvalidation(["dropped"], []),
            TestContext.Current.CancellationToken
        );
        release.SetResult();

        Assert.Equal(1, logger.Entries.Count(entry => entry.Event.Id == 1005));
        Assert.Contains(("hostloom.cache.invalidations", "dropped"), metrics.Directions);
    }

    [Fact]
    public async Task DegradedWarning_IsRateLimitedPerKey()
    {
        var logger = new RecordingLogger<TieredCache>();
        var faulting = new FaultingCacheStore(new InMemoryDistributedCacheStore(_clock));
        faulting.FailAll(CacheFailureKind.Unavailable);
        await using var cache = new TieredCache(
            Options(),
            faulting,
            _serializer,
            timeProvider: _clock,
            logger: logger
        );

        await cache.GetOrCreateAsync(
            "k",
            _ => ValueTask.FromResult(1),
            TimeSpan.Zero,
            TestContext.Current.CancellationToken
        );
        await cache.GetOrCreateAsync(
            "k",
            _ => ValueTask.FromResult(1),
            TimeSpan.Zero,
            TestContext.Current.CancellationToken
        );
        var afterTwoCalls = logger.Entries.Count(entry => entry.Event.Id == 1001);
        _clock.Advance(TimeSpan.FromMinutes(1));
        await cache.GetOrCreateAsync(
            "k",
            _ => ValueTask.FromResult(1),
            TimeSpan.Zero,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(1, afterTwoCalls);
        Assert.Equal(2, logger.Entries.Count(entry => entry.Event.Id == 1001));
        Assert.All(logger.Entries, entry => Assert.Equal(LogLevel.Warning, entry.Level));
    }

    [Fact]
    public async Task UnreadablePayload_IsAMissLoggedAtErrorAndOverwritten()
    {
        var logger = new RecordingLogger<TieredCache>();
        var store = new InMemoryDistributedCacheStore(_clock);
        await using var cache = new TieredCache(
            Options(),
            store,
            _serializer,
            timeProvider: _clock,
            logger: logger
        );
        await store.SetAsync(
            "svc:cache:data:k",
            new byte[] { 0x10, (byte)'?' },
            TimeSpan.FromMinutes(1),
            cancellationToken: TestContext.Current.CancellationToken
        );

        var value = await cache.GetOrCreateAsync(
            "k",
            _ => ValueTask.FromResult(new CachingTestPayload("fresh", 1)),
            TimeSpan.FromMinutes(1),
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Equal("fresh", value!.Text);
        var entry = Assert.Single(logger.Entries);
        Assert.Equal(1002, entry.Event.Id);
        Assert.Equal(LogLevel.Error, entry.Level);
        await using var other = new TieredCache(
            Options(),
            store,
            _serializer,
            timeProvider: _clock
        );
        Assert.Equal(
            "fresh",
            (
                await other.GetAsync<CachingTestPayload>("k", TestContext.Current.CancellationToken)
            )!.Text
        );
    }

    [Fact]
    public async Task OtherFormatVersion_IsASilentMiss()
    {
        var logger = new RecordingLogger<TieredCache>();
        var store = new InMemoryDistributedCacheStore(_clock);
        await using var cache = new TieredCache(
            Options(),
            store,
            _serializer,
            timeProvider: _clock,
            logger: logger
        );
        await store.SetAsync(
            "svc:cache:data:k",
            new byte[] { 0x20, (byte)'1' },
            TimeSpan.FromMinutes(1),
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.False(
            (await cache.TryGetAsync<int>("k", TestContext.Current.CancellationToken)).Found
        );
        Assert.Empty(logger.Entries);
    }

    [Fact]
    public async Task Describe_ReportsTheCompositionWithOptionKeys()
    {
        var store = new InMemoryDistributedCacheStore(_clock);
        await using var cache = new TieredCache(
            Options(),
            store,
            _serializer,
            timeProvider: _clock
        );

        var description = CachingProbe.Describe(cache);

        Assert.Equal("svc", description.Namespace);
        Assert.Equal(nameof(InMemoryDistributedCacheStore), description.Store);
        Assert.True(description.L1Enabled);
        Assert.Equal(nameof(SystemTextJsonCacheValueSerializer), description.Serializer);
        Assert.StartsWith("channel", description.Invalidation, StringComparison.Ordinal);
        Assert.Contains(
            description.Lines,
            line => line.Contains("Caching:L1:Enabled", StringComparison.Ordinal)
        );
        Assert.Contains(
            description.Lines,
            line => line.Contains("Caching:Stampede:LeaseDuration", StringComparison.Ordinal)
        );
        Assert.Contains(
            description.Lines,
            line => line.Contains("Caching:Warmup:BlocksReadiness", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Describe_InMemoryOnly_ReportsInMemoryStoreAndNoInvalidation()
    {
        await using var cache = new TieredCache(Options(), timeProvider: _clock);

        var description = CachingProbe.Describe(cache);

        Assert.Equal("InMemory", description.Store);
        Assert.Null(description.Serializer);
        Assert.Equal("none", description.Invalidation);
    }

    [Fact]
    public async Task DisposeAsync_StopsTheCacheAndRejectsFurtherCalls()
    {
        var cache = new TieredCache(
            Options(),
            new InMemoryDistributedCacheStore(_clock),
            _serializer,
            timeProvider: _clock
        );
        await cache.DisposeAsync();
        await cache.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await cache.GetAsync<int>("k", TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task LocalExpiration_ShorterThanExpiration_RefreshesFromL2()
    {
        var store = new InMemoryDistributedCacheStore(_clock);
        await using var cache = new TieredCache(
            Options(),
            store,
            _serializer,
            timeProvider: _clock
        );
        await cache.SetAsync(
            "k",
            1,
            new CacheEntryOptions(TimeSpan.FromMinutes(10))
            {
                LocalExpiration = TimeSpan.FromSeconds(5),
            },
            TestContext.Current.CancellationToken
        );

        _clock.Advance(TimeSpan.FromSeconds(6));

        Assert.Equal(
            CacheTier.L2,
            (await cache.TryGetAsync<int>("k", TestContext.Current.CancellationToken)).Tier
        );
    }

    [Fact]
    public async Task LocalExpiration_LongerThanExpiration_IsRejected()
    {
        await using var cache = new TieredCache(Options(), timeProvider: _clock);
        var options = new CacheEntryOptions(TimeSpan.FromSeconds(1))
        {
            LocalExpiration = TimeSpan.FromSeconds(2),
        };

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await cache.SetAsync("k", 1, options, TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task Cancellation_DuringTheFactory_PropagatesAndReleasesTheGuard()
    {
        await using var cache = new TieredCache(Options(), timeProvider: _clock);
        using var cts = new CancellationTokenSource();
        var pending = cache.GetOrCreateAsync<int>(
            "k",
            async token =>
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                return 1;
            },
            TimeSpan.FromMinutes(1),
            cts.Token
        );
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pending);
        Assert.Equal(
            2,
            await cache.GetOrCreateAsync(
                "k",
                _ => ValueTask.FromResult(2),
                TimeSpan.FromMinutes(1),
                TestContext.Current.CancellationToken
            )
        );
    }

    /// <summary>
    /// Keys whose enumeration stops the applying loop, so a test can fill the queue behind it
    /// instead of racing it.
    /// </summary>
    private sealed class BlockingKeys(TaskCompletionSource entered, TaskCompletionSource release)
        : IReadOnlyCollection<string>
    {
        public int Count => 1;

        public IEnumerator<string> GetEnumerator()
        {
            entered.TrySetResult();
            release.Task.Wait();
            yield return "blocked";
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private sealed class CacheMetricRecorder : IDisposable
    {
        private readonly string _namespace;
        private readonly MeterListener _listener = new();
        private readonly Lock _gate = new();
        private readonly List<(
            string Name,
            double Value,
            string? Outcome,
            string? Direction
        )> _measurements = [];

        public CacheMetricRecorder(string ns)
        {
            _namespace = ns;
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == CachingDiagnostics.MeterName)
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<double>(
                (instrument, value, tags, _) => Record(instrument, value, tags)
            );
            _listener.SetMeasurementEventCallback<long>(
                (instrument, value, tags, _) => Record(instrument, value, tags)
            );
            _listener.Start();
        }

        public IReadOnlyList<(string Name, string? Outcome)> Outcomes
        {
            get
            {
                lock (_gate)
                {
                    return _measurements.Select(m => (m.Name, m.Outcome)).ToList();
                }
            }
        }

        public IReadOnlyList<(string Name, string? Direction)> Directions
        {
            get
            {
                lock (_gate)
                {
                    return _measurements.Select(m => (m.Name, m.Direction)).ToList();
                }
            }
        }

        public List<double> Values(string name)
        {
            lock (_gate)
            {
                return _measurements.Where(m => m.Name == name).Select(m => m.Value).ToList();
            }
        }

        public void Dispose() => _listener.Dispose();

        private void Record(
            Instrument instrument,
            double value,
            ReadOnlySpan<KeyValuePair<string, object?>> tags
        )
        {
            string? ns = null;
            string? outcome = null;
            string? direction = null;
            foreach (var tag in tags)
            {
                if (tag.Key == "hostloom.cache.namespace")
                {
                    ns = tag.Value as string;
                }
                else if (tag.Key == "hostloom.cache.outcome")
                {
                    outcome = tag.Value as string;
                }
                else if (tag.Key == "hostloom.cache.direction")
                {
                    direction = tag.Value as string;
                }
            }

            if (ns != _namespace)
            {
                return;
            }

            lock (_gate)
            {
                _measurements.Add((instrument.Name, value, outcome, direction));
            }
        }
    }
}
