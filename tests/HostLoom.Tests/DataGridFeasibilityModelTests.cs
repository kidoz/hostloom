using HostLoom.DataGrid;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridFeasibilityModelTests
{
    [Theory]
    [InlineData(1, 1)]
    [InlineData(32, 16)]
    [InlineData(128, 64)]
    public void PlannedAxesStayWithinPublicBoundsAndP0TimesAreExplicit(int batch, int concurrency)
    {
        var spec = new DataGridFeasibilityCase(100000, batch, concurrency, 17, 60, 300);
        spec.Validate();
        Assert.True(spec.IsP0);
        Assert.False((spec with { KeyCount = 256, WarmupSeconds = 1, SampleSeconds = 5 }).IsP0);
        Assert.Throws<ArgumentException>(() => (spec with { KeyCount = 100001 }).Validate());
        Assert.Throws<ArgumentException>(() => (spec with { KeyCount = 255 }).Validate());
        Assert.Throws<ArgumentException>(() => (spec with { BatchSize = 129 }).Validate());
        Assert.Throws<ArgumentException>(() => (spec with { Concurrency = 65 }).Validate());
        Assert.Throws<ArgumentException>(() => (spec with { SampleSeconds = 301 }).Validate());
    }

    [Fact]
    public void EntirePlannedKeyspaceHasDistinctExactly32ByteKeys()
    {
        var keys = Enumerable.Range(0, 100000).Select(DataGridFeasibilityCase.Key).ToHashSet();
        Assert.Equal(100000, keys.Count);
        Assert.All(keys, key => Assert.Equal(32, key.ByteCount));
    }

    [Fact]
    public void FixedSizeValuesDetectPayloadDamageWithoutARequestSideReference()
    {
        var value = DataGridFeasibilityCase.Value(17, 3, 9, 1234);
        Assert.Equal(1024, value.Length);
        DataGridFeasibilityCase.ValidateValue(value);
        value[100] ^= 1;
        Assert.Throws<InvalidDataException>(() => DataGridFeasibilityCase.ValidateValue(value));
    }

    [Fact]
    public void BatchRetriesRetainIdentityDigestExpiryAndIndependentInputBytes()
    {
        var request = DataGridFeasibilityCase.Put(
            1,
            42,
            Enumerable.Range(0, 128).ToArray(),
            17,
            3,
            true
        );
        var bytes = request.Mutation!.Encode();
        var digest = request.Mutation.ComputeDigest();
        Assert.Equal(DataGridExpiryKind.Relative, request.Mutation.ExpiryKind);
        Assert.Equal(TimeSpan.FromSeconds(60).Ticks, request.Mutation.ExpiryTicks);
        Assert.Equal(128, request.Mutation.ItemCount);
        request.Mutation.GetItem(0).GetValue()[0] ^= 1;
        Assert.Equal(bytes, request.Mutation.Encode());
        Assert.Equal(digest, request.Mutation.ComputeDigest());
        Assert.NotEqual(
            digest,
            DataGridFeasibilityCase
                .Put(1, 43, Enumerable.Range(0, 128).ToArray(), 17, 3, true)
                .Mutation!.ComputeDigest()
        );
    }

    [Fact]
    public void PercentilesUseIndividualNearestRankAndNeverInventEmptySamples()
    {
        Assert.Null(DataGridFeasibilityCase.Percentile([], .99));
        Assert.Equal(5, DataGridFeasibilityCase.Percentile([1, 2, 3, 4, 5], .95));
        Assert.Equal(3, DataGridFeasibilityCase.Percentile([5, 2, 4, 1, 3], .5));
    }
}
