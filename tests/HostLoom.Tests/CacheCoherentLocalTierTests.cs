using HostLoom.Caching;
using HostLoom.Caching.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// The coherent in-process tier's race rules, driven synchronously: capture as an operation
/// would before its awaited work, apply an invalidation as if it arrived during that work, then
/// try the insert the operation would make afterwards.
/// </summary>
public sealed class CacheCoherentLocalTierTests
{
    private static readonly TimeSpan Life = TimeSpan.FromMinutes(5);
    private readonly TestClock _clock = new();

    [Fact]
    public void A_fill_nothing_overlapped_is_inserted()
    {
        using var tier = Tier();
        var captured = tier.Capture("orders:1", ["catalog"]);

        Assert.True(tier.IsCurrent(captured));
        Assert.True(tier.TryCommit(captured, "orders:1", "v1", Life, ["catalog"], null, null));
        Assert.True(tier.TryGet<string>("orders:1", out var value));
        Assert.Equal("v1", value);
    }

    [Fact]
    public void An_invalidation_of_the_same_key_keeps_the_fill_out()
    {
        using var tier = Tier();
        var captured = tier.Capture("orders:1", null);

        tier.Apply(new CacheInvalidation(["orders:1"], []));

        Assert.False(tier.IsCurrent(captured));
        Assert.False(tier.TryCommit(captured, "orders:1", "v1", Life, null, null, null));
        Assert.False(tier.TryCommitNull(captured, "orders:1", Life, null, null));
        Assert.Equal(0, tier.Count);
        // A capture taken after the invalidation is current again.
        var fresh = tier.Capture("orders:1", null);
        Assert.True(tier.TryCommit(fresh, "orders:1", "v2", Life, null, null, null));
        Assert.Equal(1, tier.Count);
    }

    [Fact]
    public void An_invalidation_of_a_key_on_the_same_stripe_keeps_the_fill_out_and_one_elsewhere_does_not()
    {
        using var tier = Tier();
        var sharing = KeyOnStripe("orders:1", same: true);
        var apart = KeyOnStripe("orders:1", same: false);
        var captured = tier.Capture("orders:1", null);

        tier.Apply(new CacheInvalidation([apart], []));
        Assert.True(tier.IsCurrent(captured));

        // Stripes are conservative: another key sharing the stripe counts as an overlap.
        tier.Apply(new CacheInvalidation([sharing], []));
        Assert.False(tier.TryCommit(captured, "orders:1", "v1", Life, null, null, null));
        Assert.Equal(0, tier.Count);
    }

    [Fact]
    public void An_invalidation_of_a_declared_tag_keeps_the_fill_out()
    {
        using var tier = Tier();
        string[] tags = ["catalog", "customers"];
        var captured = tier.Capture("orders:1", tags);

        tier.Apply(new CacheInvalidation([], ["customers"]));

        Assert.False(tier.TryCommit(captured, "orders:1", "v1", Life, tags, null, null));
        Assert.False(tier.TryCommitNull(captured, "orders:1", Life, tags, null));
        Assert.Equal(0, tier.Count);
    }

    [Fact]
    public void An_invalidation_of_an_unrelated_tag_lets_a_tagged_fill_in()
    {
        using var tier = Tier();
        var unrelated = TagOnOtherStripe("catalog");
        var captured = tier.Capture("orders:1", ["catalog"]);

        tier.Apply(new CacheInvalidation([], [unrelated]));

        Assert.True(tier.TryCommit(captured, "orders:1", "v1", Life, ["catalog"], null, null));
        Assert.True(tier.TryGet<string>("orders:1", out _));
    }

    [Fact]
    public void A_tag_invalidation_lets_an_untagged_fill_in()
    {
        using var tier = Tier();
        var captured = tier.Capture("orders:1", null);

        tier.Apply(new CacheInvalidation([], ["catalog"]));

        Assert.True(tier.TryCommit(captured, "orders:1", "v1", Life, null, null, null));
        Assert.True(tier.TryGet<string>("orders:1", out _));
    }

    [Fact]
    public void A_flush_clears_the_tier_and_keeps_every_kind_of_overlapped_insert_out()
    {
        using var tier = Tier();
        var before = tier.Capture("invoices:1", null);
        Assert.True(tier.TryCommit(before, "invoices:1", "v0", Life, null, null, null));
        var untagged = tier.Capture("orders:1", null);
        var tagged = tier.Capture("orders:2", ["catalog"]);
        var read = tier.CaptureRead("orders:3");
        var bulk = tier.CaptureBulk();
        var readStart = _clock.GetTimestamp();

        tier.Apply(CacheInvalidation.Flush);

        Assert.Equal(0, tier.Count);
        Assert.False(tier.TryCommit(untagged, "orders:1", "v1", Life, null, null, null));
        Assert.False(tier.TryCommit(tagged, "orders:2", "v2", Life, ["catalog"], null, null));
        Assert.False(tier.TryPromote(read, "orders:3", null, "v3", Life, 1, null, readStart));
        Assert.False(tier.TryCommitBatch<string>(bulk, [new("orders:4", "v4")], Life));
        Assert.Equal(0, tier.Count);
    }

    [Fact]
    public void After_any_tag_invalidation_a_read_promotes_an_untagged_payload_but_not_a_tagged_one()
    {
        using var tier = Tier();
        var unrelated = TagOnOtherStripe("catalog");
        var read = tier.CaptureRead("orders:1");
        var readStart = _clock.GetTimestamp();

        // The read did not know its tags when it started, so any tag message counts.
        tier.Apply(new CacheInvalidation([], [unrelated]));

        Assert.False(
            tier.TryPromote(read, "orders:1", ["catalog"], "v1", Life, 1, null, readStart)
        );
        Assert.False(tier.TryPromoteNull(read, "orders:1", ["catalog"], Life, null, readStart));
        Assert.Equal(0, tier.Count);
        Assert.True(tier.TryPromote(read, "orders:1", null, "v1", Life, 1, null, readStart));
        Assert.True(tier.TryGet<string>("orders:1", out var value));
        Assert.Equal("v1", value);
    }

    [Fact]
    public void An_invalidation_of_the_read_key_keeps_its_promotion_out()
    {
        using var tier = Tier();
        var read = tier.CaptureRead("orders:1");
        var readStart = _clock.GetTimestamp();

        tier.Apply(new CacheInvalidation(["orders:1"], []));

        Assert.False(tier.TryPromote(read, "orders:1", null, "v1", Life, 1, null, readStart));
        Assert.False(tier.TryPromoteNull(read, "orders:1", null, Life, null, readStart));
        Assert.Equal(0, tier.Count);
    }

    [Fact]
    public void A_promotion_does_not_replace_an_entry_written_after_the_read_began()
    {
        using var tier = Tier();
        var read = tier.CaptureRead("orders:1");
        var readStart = _clock.GetTimestamp();
        _clock.Advance(TimeSpan.FromMilliseconds(1));
        var write = tier.Capture("orders:1", null);
        Assert.True(tier.TryCommit(write, "orders:1", "v2", Life, null, null, null));

        // Nothing invalidated the key, so the check passes; the newer entry still stays.
        Assert.True(tier.TryPromote(read, "orders:1", null, "v1", Life, 1, null, readStart));
        Assert.True(tier.TryGet<string>("orders:1", out var value));
        Assert.Equal("v2", value);
    }

    [Fact]
    public void Any_invalidation_keeps_a_bulk_read_and_a_warmup_batch_out()
    {
        using var tier = Tier();
        var bulk = tier.CaptureBulk();
        var readStart = _clock.GetTimestamp();

        // Bulk operations compare the whole generation, so even a key on another stripe counts.
        tier.Apply(new CacheInvalidation([KeyOnStripe("orders:1", same: false)], []));

        Assert.False(tier.TryPromote(bulk, "orders:1", null, "v1", Life, 1, null, readStart));
        Assert.False(tier.TryPromoteNull(bulk, "orders:1", ["catalog"], Life, null, readStart));
        Assert.False(tier.TryCommitBatch<string>(bulk, [new("orders:1", "v1")], Life));
        Assert.Equal(0, tier.Count);

        var fresh = tier.CaptureBulk();
        Assert.True(
            tier.TryCommitBatch<string>(fresh, [new("orders:1", "v1"), new("orders:2", "v2")], Life)
        );
        Assert.True(
            tier.TryPromote(fresh, "orders:3", ["catalog"], "v3", Life, 1, null, readStart)
        );
        Assert.Equal(3, tier.Count);
    }

    [Fact]
    public void Applying_an_invalidation_evicts_the_keys_it_names_and_the_entries_carrying_its_tags()
    {
        using var tier = Tier();
        Commit(tier, "orders:1", ["catalog"]);
        Commit(tier, "orders:2", ["customers"]);
        Commit(tier, "orders:3", null);

        tier.Apply(new CacheInvalidation(["orders:3"], ["catalog"]));

        Assert.False(tier.TryGet<string>("orders:1", out _));
        Assert.True(tier.TryGet<string>("orders:2", out _));
        Assert.False(tier.TryGet<string>("orders:3", out _));
        Assert.Equal(1, tier.Count);
    }

    [Fact]
    public void A_disabled_tier_holds_nothing_but_still_reports_an_overlapped_write()
    {
        using var tier = Tier(enabled: false);
        var captured = tier.Capture("orders:1", null);

        Assert.False(tier.Enabled);
        Assert.True(tier.TryCommit(captured, "orders:1", "v1", Life, null, null, null));
        Assert.False(tier.TryGet<string>("orders:1", out _));
        Assert.Equal(0, tier.Count);

        tier.Apply(new CacheInvalidation(["orders:1"], []));
        Assert.False(tier.IsCurrent(captured));
    }

    [Fact]
    public void Stripes_are_stable_and_within_range()
    {
        foreach (var name in new[] { "orders:1", "catalog", "", new string('x', 500) })
        {
            Assert.InRange(CoherentLocalTier.StripeOf(name), 0, 1023);
            Assert.InRange(CoherentLocalTier.TagStripeOf(name), 0, 1023);
            Assert.Equal(CoherentLocalTier.StripeOf(name), TieredCache.StripeOf(name));
            Assert.Equal(CoherentLocalTier.TagStripeOf(name), TieredCache.TagStripeOf(name));
        }
    }

    private CoherentLocalTier Tier(bool enabled = true) =>
        new(new CacheL1Options { Enabled = enabled }, _clock, NullLogger.Instance);

    private static void Commit(CoherentLocalTier tier, string key, string[]? tags)
    {
        var captured = tier.Capture(key, tags);
        Assert.True(tier.TryCommit(captured, key, key, Life, tags, null, null));
    }

    private static string KeyOnStripe(string key, bool same)
    {
        var stripe = CoherentLocalTier.StripeOf(key);
        return Enumerable
            .Range(0, 1_000_000)
            .Select(i => "other:" + i)
            .First(candidate =>
                candidate != key && (CoherentLocalTier.StripeOf(candidate) == stripe) == same
            );
    }

    private static string TagOnOtherStripe(string tag)
    {
        var stripe = CoherentLocalTier.TagStripeOf(tag);
        return Enumerable
            .Range(0, 100_000)
            .Select(i => "other-" + i)
            .First(candidate => CoherentLocalTier.TagStripeOf(candidate) != stripe);
    }
}
