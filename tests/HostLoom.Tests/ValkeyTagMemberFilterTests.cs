using System.Text;
using HostLoom.Valkey;
using HostLoom.Valkey.Internal;
using ValkeyDotNet;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// The Valkey adapter applies the same two guards as Redis: tag-index members are unlinked only
/// under the namespace's cache-data prefix, and invalidation items are validated as cache keys.
/// </summary>
public sealed class ValkeyTagMemberFilterTests
{
    [Theory]
    [InlineData("svc:catalog", "svc:cache:tag:catalog")]
    [InlineData("svc:cache:lease:catalog", "svc:cache:tag:catalog")]
    [InlineData("other:cache:data:catalog", "svc:cache:tag:catalog")]
    [InlineData("svc:cache:data:", "svc:cache:tag:catalog")]
    [InlineData("svc:cache:data:catalog", "svc:tag")]
    [InlineData("svc:cache:data:catalog", "svc:cache:tag:")]
    public async Task Tagged_writes_reject_unremovable_keys_before_backend_io(
        string key,
        string tag
    )
    {
        await using var connection = new ValkeyConnection(new ValkeyOptions());
        var store = new ValkeyCacheStore(connection);
        var token = TestContext.Current.CancellationToken;
        // Include a valid first tag to ensure the whole input is checked before dispatch.
        string[] tags = ["svc:cache:tag:valid", tag];
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.SetAsync(key, "value"u8.ToArray(), TimeSpan.FromMinutes(1), tags, token).AsTask()
        );
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store
                .SetIfAbsentAsync(key, "value"u8.ToArray(), TimeSpan.FromMinutes(1), tags, token)
                .AsTask()
        );
        Assert.Equal(ValkeyConnectionState.NeverConnected, connection.State);
    }

    [Theory]
    [InlineData("svc:custom-index")]
    [InlineData("nocolon")]
    [InlineData(":cache:tag:catalog")]
    [InlineData("svc:cache:tag:")]
    public void Invalid_tag_domains_are_rejected(string tag) =>
        Assert.Throws<ArgumentException>(() => ValkeyCacheStore.DataPrefixFor(tag));

    [Theory]
    [InlineData("svc:cache:tag:catalog", "svc:cache:data:")]
    [InlineData("svc:cache:tag:catalog:eu", "svc:cache:data:")]
    public void DataPrefixFor_DerivesTheNamespaceDataPrefixFromTheTagKey(
        string tagKey,
        string expected
    ) => Assert.Equal(expected, ValkeyCacheStore.DataPrefixFor(tagKey));

    [Fact]
    public void Partition_SeparatesMembersOutsideTheDataPrefix()
    {
        ReadOnlyMemory<byte>[] members =
        [
            Bytes("svc:cache:data:catalog:eu"),
            Bytes("svc:lock:catalog:refresh"),
            Bytes("svc:cache:data:catalog:us"),
            Bytes("other:cache:data:catalog:eu"),
            Bytes("svc:cache:lease:catalog:eu"),
        ];

        var (accepted, rejected) = ValkeyCacheStore.Partition(members, "svc:cache:data:"u8);

        Assert.Equal(
            ["svc:cache:data:catalog:eu", "svc:cache:data:catalog:us"],
            accepted.Select(Text)
        );
        Assert.Equal(
            [
                "svc:lock:catalog:refresh",
                "other:cache:data:catalog:eu",
                "svc:cache:lease:catalog:eu",
            ],
            rejected.Select(Text)
        );
    }

    [Theory]
    [InlineData("[1,[\"catalog eu\"],[]]")]
    [InlineData("[1,[\"catalog:eu\"],[\"\"]]")]
    [InlineData("[1,[\"catalog:eu\"],[\"a\\tb\"]]")]
    [InlineData("[1,[\"catalog:eu\",\"x\\u0007\"],[]]")]
    public void Codec_DropsTheWholeMessageWhenAnyItemIsInvalid(string payload) =>
        Assert.Null(ValkeyInvalidationCodec.Decode(Bytes(payload)));

    [Fact]
    public void Codec_HonoursTheConfiguredKeyLength()
    {
        var key = new string('k', 513);
        Assert.Null(ValkeyInvalidationCodec.Decode(Bytes($"[1,[\"{key}\"],[]]")));
        Assert.NotNull(ValkeyInvalidationCodec.Decode(Bytes($"[1,[\"{key}\"],[]]"), 1024));
        Assert.Null(ValkeyInvalidationCodec.Decode(Bytes("[1,[\"catalog:eu\"],[]]"), 5));
        var decoded = ValkeyInvalidationCodec.Decode(Bytes("[1,[\"catalog:eu\"],[\"catalog\"]]"));
        Assert.NotNull(decoded);
        Assert.Equal(["catalog:eu"], decoded.Keys);
        Assert.Equal(["catalog"], decoded.Tags);
    }

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static string Text(ReadOnlyMemory<byte> bytes) => Encoding.UTF8.GetString(bytes.Span);
}
