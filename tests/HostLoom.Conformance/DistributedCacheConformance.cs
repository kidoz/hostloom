using System.Buffers;
using HostLoom.Caching;
using HostLoom.Caching.Testing;
using Microsoft.Extensions.Caching.Distributed;
using Xunit;

namespace HostLoom.Conformance;

/// <summary>The collaborators one opted-in Microsoft distributed-cache adapter scenario needs.</summary>
public sealed class DistributedCacheConformanceFixture
{
    public required IDistributedCache Cache { get; init; }

    public required IBufferDistributedCache BufferedCache { get; init; }

    public required FaultingCacheStore Faults { get; init; }

    public required ConformanceClock Clock { get; init; }

    public required string Namespace { get; init; }

    public IDistributedCacheStore Store => Faults.Inner;

    public string EnvelopedKey(string key) => $"{Namespace}:cache:external-v2:{key}";

    public string LegacyKey(string key) => $"{Namespace}:cache:external:{key}";
}

/// <summary>Adapter-only sliding and migration scenarios shared by all distributed stores.</summary>
public static class DistributedCacheConformance
{
    public static IReadOnlyDictionary<
        string,
        Func<DistributedCacheConformanceFixture, Task>
    > Scenarios { get; } =
        new Dictionary<string, Func<DistributedCacheConformanceFixture, Task>>(
            StringComparer.Ordinal
        )
        {
            [nameof(ReadsAndExplicitRefresh_RenewSlidingEntries)] =
                ReadsAndExplicitRefresh_RenewSlidingEntries,
            [nameof(Inactivity_Expires)] = Inactivity_Expires,
            [nameof(AbsoluteCap_IsNotExtendedByReads)] = AbsoluteCap_IsNotExtendedByReads,
            [nameof(DeletedKey_IsNotResurrected)] = DeletedKey_IsNotResurrected,
            [nameof(ReplacementUnderChangedPolicy_IsNotRefreshed)] =
                ReplacementUnderChangedPolicy_IsNotRefreshed,
            [nameof(ReplacementUnderIdenticalPolicy_IsNotRefreshed)] =
                ReplacementUnderIdenticalPolicy_IsNotRefreshed,
            [nameof(ExpirationAfterRead_IsAMiss)] = ExpirationAfterRead_IsAMiss,
            [nameof(RefreshFailure_ReturnsTheCompletedRead)] =
                RefreshFailure_ReturnsTheCompletedRead,
            [nameof(CallerCancellation_Propagates)] = CallerCancellation_Propagates,
            [nameof(LegacyEntries_ReadWithoutRenewalAndRemovalCoversBothDomains)] =
                LegacyEntries_ReadWithoutRenewalAndRemovalCoversBothDomains,
            [nameof(EnvelopedAndLegacyPrefixes_AreDisjoint)] =
                EnvelopedAndLegacyPrefixes_AreDisjoint,
            [nameof(SuccessfulWrite_RetiresTheLegacyRepresentation)] =
                SuccessfulWrite_RetiresTheLegacyRepresentation,
            [nameof(FailedLegacyRetirement_DoesNotPublishTheReplacement)] =
                FailedLegacyRetirement_DoesNotPublishTheReplacement,
            [nameof(MalformedEnvelope_IsAMissWithoutLegacyFallback)] =
                MalformedEnvelope_IsAMissWithoutLegacyFallback,
        };

    public static async Task ReadsAndExplicitRefresh_RenewSlidingEntries(
        DistributedCacheConformanceFixture fixture
    )
    {
        var options = Sliding(TimeSpan.FromSeconds(4));
        await fixture.Cache.SetAsync("catalog:read", [1], options);
        await fixture.Clock.AdvanceAsync(TimeSpan.FromSeconds(2));

        Assert.Equal([1], await fixture.Cache.GetAsync("catalog:read"));
        await fixture.Clock.AdvanceAsync(TimeSpan.FromSeconds(2));
        var destination = new ArrayBufferWriter<byte>();
        Assert.True(await fixture.BufferedCache.TryGetAsync("catalog:read", destination));
        Assert.Equal([1], destination.WrittenSpan.ToArray());

        await fixture.Clock.AdvanceAsync(TimeSpan.FromSeconds(2));
        await fixture.Cache.RefreshAsync("catalog:read");
        await fixture.Clock.AdvanceAsync(TimeSpan.FromSeconds(2));
        Assert.Equal([1], await fixture.Cache.GetAsync("catalog:read"));
        await fixture.Clock.AdvanceAsync(TimeSpan.FromSeconds(4));
        Assert.Null(await fixture.Cache.GetAsync("catalog:read"));
    }

    public static async Task Inactivity_Expires(DistributedCacheConformanceFixture fixture)
    {
        await fixture.Cache.SetAsync("catalog:idle", [2], Sliding(TimeSpan.FromSeconds(1)));
        await fixture.Clock.AdvanceAsync(TimeSpan.FromMilliseconds(1200));
        Assert.Null(await fixture.Cache.GetAsync("catalog:idle"));
    }

    public static async Task AbsoluteCap_IsNotExtendedByReads(
        DistributedCacheConformanceFixture fixture
    )
    {
        var options = new DistributedCacheEntryOptions
        {
            SlidingExpiration = TimeSpan.FromSeconds(2),
            AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(4),
        };
        await fixture.Cache.SetAsync("catalog:cap", [3], options);
        await fixture.Clock.AdvanceAsync(TimeSpan.FromSeconds(1));
        Assert.Equal([3], await fixture.Cache.GetAsync("catalog:cap"));
        await fixture.Clock.AdvanceAsync(TimeSpan.FromSeconds(3));
        Assert.Null(await fixture.Cache.GetAsync("catalog:cap"));
    }

    public static async Task DeletedKey_IsNotResurrected(DistributedCacheConformanceFixture fixture)
    {
        var key = fixture.EnvelopedKey("catalog:deleted");
        await fixture.Cache.SetAsync("catalog:deleted", [4], Sliding(TimeSpan.FromMinutes(1)));
        fixture.Faults.BeforeRefresh = async token =>
        {
            fixture.Faults.BeforeRefresh = null;
            await fixture.Store.RemoveAsync([key], token);
        };

        Assert.Equal([4], await fixture.Cache.GetAsync("catalog:deleted"));
        Assert.Null(await fixture.Store.GetAsync(key));
    }

    public static async Task ReplacementUnderChangedPolicy_IsNotRefreshed(
        DistributedCacheConformanceFixture fixture
    ) =>
        await Replacement_IsNotRefreshed(
            fixture,
            "catalog:changed",
            [5],
            [6],
            new DistributedCacheEntryOptions
            {
                SlidingExpiration = TimeSpan.FromSeconds(30),
                AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(2),
            }
        );

    public static async Task ReplacementUnderIdenticalPolicy_IsNotRefreshed(
        DistributedCacheConformanceFixture fixture
    )
    {
        var options = Sliding(TimeSpan.FromMinutes(1));
        await Replacement_IsNotRefreshed(fixture, "catalog:identical", [7], [7], options, options);
    }

    public static async Task ExpirationAfterRead_IsAMiss(DistributedCacheConformanceFixture fixture)
    {
        await fixture.Cache.SetAsync(
            "catalog:expired",
            [8],
            new DistributedCacheEntryOptions
            {
                SlidingExpiration = TimeSpan.FromSeconds(10),
                AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(2),
            }
        );
        fixture.Faults.BeforeRefresh = async _ =>
        {
            fixture.Faults.BeforeRefresh = null;
            await fixture.Clock.AdvanceAsync(TimeSpan.FromSeconds(2));
        };

        Assert.Null(await fixture.Cache.GetAsync("catalog:expired"));
    }

    public static async Task RefreshFailure_ReturnsTheCompletedRead(
        DistributedCacheConformanceFixture fixture
    )
    {
        await fixture.Cache.SetAsync("catalog:degraded", [9], Sliding(TimeSpan.FromMinutes(1)));
        fixture.Faults.FailOperation(CacheFailureKind.Unavailable, "refresh");

        Assert.Equal([9], await fixture.Cache.GetAsync("catalog:degraded"));
        fixture.Faults.FailOperation(CacheFailureKind.Timeout, "refresh");
        await fixture.Cache.RefreshAsync("catalog:degraded");
    }

    public static async Task CallerCancellation_Propagates(
        DistributedCacheConformanceFixture fixture
    )
    {
        await fixture.Cache.SetAsync("catalog:cancelled", [10], Sliding(TimeSpan.FromMinutes(1)));
        using var cancelled = new CancellationTokenSource();
        fixture.Faults.BeforeRefresh = async token =>
        {
            fixture.Faults.BeforeRefresh = null;
            await cancelled.CancelAsync();
            token.ThrowIfCancellationRequested();
        };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            fixture.Cache.GetAsync("catalog:cancelled", cancelled.Token)
        );
    }

    public static async Task LegacyEntries_ReadWithoutRenewalAndRemovalCoversBothDomains(
        DistributedCacheConformanceFixture fixture
    )
    {
        var legacyKey = fixture.LegacyKey("catalog:legacy");
        await fixture.Store.SetAsync(legacyKey, new byte[] { 11 }, TimeSpan.FromMinutes(1));
        var before = (await fixture.Store.GetAsync(legacyKey))!.Value;

        Assert.Equal([11], await fixture.Cache.GetAsync("catalog:legacy"));
        await fixture.Cache.RefreshAsync("catalog:legacy");
        var after = (await fixture.Store.GetAsync(legacyKey))!.Value;
        Assert.True(after.RemainingTimeToLive <= before.RemainingTimeToLive);

        await fixture.Cache.SetAsync("catalog:legacy", [12], Sliding(TimeSpan.FromMinutes(1)));
        await fixture.Cache.RemoveAsync("catalog:legacy");
        Assert.Null(await fixture.Store.GetAsync(legacyKey));
        Assert.Null(await fixture.Store.GetAsync(fixture.EnvelopedKey("catalog:legacy")));
    }

    public static async Task EnvelopedAndLegacyPrefixes_AreDisjoint(
        DistributedCacheConformanceFixture fixture
    )
    {
        await fixture.Cache.SetAsync("product", [14], Sliding(TimeSpan.FromMinutes(1)));

        Assert.Null(await fixture.Cache.GetAsync("v2:product"));
        await fixture.Cache.RemoveAsync("v2:product");
        Assert.Equal([14], await fixture.Cache.GetAsync("product"));
    }

    public static async Task SuccessfulWrite_RetiresTheLegacyRepresentation(
        DistributedCacheConformanceFixture fixture
    )
    {
        var key = "catalog:replace-legacy";
        var legacyKey = fixture.LegacyKey(key);
        await fixture.Store.SetAsync(legacyKey, new byte[] { 15 }, TimeSpan.FromMinutes(10));
        await fixture.Cache.SetAsync(key, [16], Sliding(TimeSpan.FromSeconds(1)));

        Assert.Null(await fixture.Store.GetAsync(legacyKey));
        await fixture.Clock.AdvanceAsync(TimeSpan.FromMilliseconds(1200));
        Assert.Null(await fixture.Cache.GetAsync(key));
    }

    public static async Task FailedLegacyRetirement_DoesNotPublishTheReplacement(
        DistributedCacheConformanceFixture fixture
    )
    {
        var key = "catalog:failed-retirement";
        await fixture.Store.SetAsync(
            fixture.LegacyKey(key),
            new byte[] { 17 },
            TimeSpan.FromMinutes(10)
        );
        fixture.Faults.FailOperation(CacheFailureKind.Unavailable, "remove");

        await fixture.Cache.SetAsync(key, [18], Sliding(TimeSpan.FromSeconds(1)));

        Assert.Null(await fixture.Store.GetAsync(fixture.EnvelopedKey(key)));
        Assert.Equal([17], await fixture.Cache.GetAsync(key));
        await fixture.Cache.SetAsync(key, [18], Sliding(TimeSpan.FromSeconds(1)));
        await fixture.Clock.AdvanceAsync(TimeSpan.FromMilliseconds(1200));
        Assert.Null(await fixture.Cache.GetAsync(key));
    }

    public static async Task MalformedEnvelope_IsAMissWithoutLegacyFallback(
        DistributedCacheConformanceFixture fixture
    )
    {
        var key = "catalog:malformed";
        await fixture.Store.SetAsync(
            fixture.EnvelopedKey(key),
            new byte[] { 1, 2, 3 },
            TimeSpan.FromMinutes(1)
        );
        await fixture.Store.SetAsync(
            fixture.LegacyKey(key),
            new byte[] { 13 },
            TimeSpan.FromMinutes(1)
        );

        Assert.Null(await fixture.Cache.GetAsync(key));
        var destination = new ArrayBufferWriter<byte>();
        Assert.False(await fixture.BufferedCache.TryGetAsync(key, destination));
        Assert.Equal(0, destination.WrittenCount);
    }

    private static DistributedCacheEntryOptions Sliding(TimeSpan window) =>
        new() { SlidingExpiration = window };

    private static async Task Replacement_IsNotRefreshed(
        DistributedCacheConformanceFixture fixture,
        string key,
        byte[] originalValue,
        byte[] replacementValue,
        DistributedCacheEntryOptions replacementOptions,
        DistributedCacheEntryOptions? originalOptions = null
    )
    {
        originalOptions ??= Sliding(TimeSpan.FromMinutes(1));
        var storedKey = fixture.EnvelopedKey(key);
        await fixture.Cache.SetAsync(key, originalValue, originalOptions);
        var original = (await fixture.Store.GetAsync(storedKey))!.Value;
        CacheStoreEntry replacement = default;
        fixture.Faults.BeforeRefresh = async token =>
        {
            fixture.Faults.BeforeRefresh = null;
            await fixture.Cache.SetAsync(key, replacementValue, replacementOptions, token);
            replacement = (await fixture.Store.GetAsync(storedKey, token))!.Value;
        };

        Assert.Equal(originalValue, await fixture.Cache.GetAsync(key));
        var current = (await fixture.Store.GetAsync(storedKey))!.Value;
        Assert.NotEqual(original.Payload.ToArray(), replacement.Payload.ToArray());
        Assert.Equal(replacement.Payload.ToArray(), current.Payload.ToArray());
        Assert.True(current.RemainingTimeToLive <= replacement.RemainingTimeToLive);
    }
}
