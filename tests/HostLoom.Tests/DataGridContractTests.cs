using System.Text;
using HostLoom.DataGrid;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridContractTests
{
    private static readonly Guid Incarnation = new("00112233-4455-6677-8899-aabbccddeeff");

    [Fact]
    public void KeyOwnsInputAndOutputBuffers()
    {
        var input = Encoding.UTF8.GetBytes("catalog:eu");
        var key = new DataGridKey(input);
        var lookup = new Dictionary<DataGridKey, string> { [key] = "books" };
        input[0] = 0;
        var output = key.ToArray();
        output[0] = 0;

        Assert.Equal("books", lookup[DataGridKey.FromUtf8("catalog:eu")]);
        Assert.Equal("catalog:eu"u8.ToArray(), key.ToArray());
        var copy = new byte[key.ByteCount];
        key.CopyTo(copy);
        Assert.Equal("catalog:eu"u8.ToArray(), copy);
    }

    [Fact]
    public void TextEncodingIsStrictAndDoesNotNormalize()
    {
        Assert.Throws<EncoderFallbackException>(() => DataGridKey.FromUtf8("\ud800"));
        Assert.NotEqual(DataGridKey.FromUtf8("é"), DataGridKey.FromUtf8("e\u0301"));
        Assert.Equal(new DataGridKey([0xc3, 0xa9]), DataGridKey.FromUtf8("é"));
    }

    [Fact]
    public void KeysEnforceEncodedByteLimitAndAllowOpaqueBytes()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new DataGridKey([]));
        Assert.Throws<ArgumentOutOfRangeException>(() => new DataGridKey(new byte[1025]));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            DataGridKey.FromUtf8(new string('é', 513))
        );
        Assert.Throws<ArgumentOutOfRangeException>(() => DataGridKey.FromUtf8(string.Empty));
        Assert.Throws<ArgumentNullException>(() => DataGridKey.FromUtf8(null!));
        Assert.Equal(1024, DataGridKey.FromUtf8(new string('é', 512)).ByteCount);
        Assert.Equal(new byte[] { 0, 255 }, new DataGridKey([0, 255]).ToArray());
    }

    [Fact]
    public void VersionEncodingHasStableByteOrderAndRoundTrips()
    {
        var version = new DataGridEntryVersion(Incarnation, 0x0102030405060708, 0x01020304);
        var encoded = new byte[DataGridEntryVersion.EncodedByteCount];
        version.WriteTo(encoded);
        Assert.Equal(
            "00112233445566778899AABBCCDDEEFF010203040506070801020304",
            Convert.ToHexString(encoded)
        );
        Assert.Equal(version, DataGridEntryVersion.ReadFrom(encoded));
    }

    [Fact]
    public void VersionDistinguishesRecreationBatchItemsAndIncarnations()
    {
        var original = new DataGridEntryVersion(Incarnation, 1);
        Assert.NotEqual(original, new DataGridEntryVersion(Incarnation, 3));
        Assert.NotEqual(original, new DataGridEntryVersion(Incarnation, 1, 1));
        Assert.NotEqual(
            original,
            new DataGridEntryVersion(new Guid("10112233-4455-6677-8899-aabbccddeeff"), 1)
        );
        Assert.True(default(DataGridEntryVersion).IsEmpty);
        Assert.False(original.IsEmpty);
        Assert.Throws<InvalidOperationException>(() =>
            default(DataGridEntryVersion).WriteTo(new byte[28])
        );
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(-1, 0)]
    [InlineData(1, -1)]
    public void InvalidPositionsAreRejected(long index, int item)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DataGridEntryVersion(Incarnation, index, item)
        );
    }

    [Fact]
    public void InvalidVersionEncodingIsRejected()
    {
        var version = new DataGridEntryVersion(Incarnation, long.MaxValue, int.MaxValue);
        Assert.Throws<ArgumentException>(() => new DataGridEntryVersion(Guid.Empty, 1));
        Assert.Throws<ArgumentException>(() => version.WriteTo(new byte[29]));
        Assert.Throws<ArgumentException>(() => DataGridEntryVersion.ReadFrom(new byte[27]));
        Assert.Throws<ArgumentException>(() => DataGridEntryVersion.ReadFrom(new byte[28]));
        var encoded = new byte[28];
        version.WriteTo(encoded);
        Assert.Equal(version, DataGridEntryVersion.ReadFrom(encoded));
        encoded[16] = 0xff;
        Assert.Throws<ArgumentOutOfRangeException>(() => DataGridEntryVersion.ReadFrom(encoded));
    }
}
