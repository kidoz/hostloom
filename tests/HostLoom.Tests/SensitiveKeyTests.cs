using System.Security.Cryptography;
using System.Text;
using HostLoom.Caching;
using HostLoom.Locking;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// The unkeyed hash suits high-entropy inputs; a low-entropy secret such as a PIN needs the
/// keyed form, whose output cannot be tested against a guess without the key.
/// </summary>
public sealed class SensitiveKeyTests
{
    private static readonly byte[] Pepper = "catalog-service-pepper"u8.ToArray();

    [Fact]
    public void Key_helpers_reject_unpaired_surrogates_and_preserve_supplementary_scalars()
    {
        foreach (var malformed in new[] { "catalog\ud800", "\udc00catalog" })
        {
            Assert.Throws<EncoderFallbackException>(() => CacheKey.FromSensitive(malformed));
            Assert.Throws<EncoderFallbackException>(() =>
                CacheKey.FromSensitive(malformed, Pepper)
            );
            Assert.Throws<EncoderFallbackException>(() => LockKey.FromSensitive(malformed));
            Assert.Throws<EncoderFallbackException>(() => LockKey.FromSensitive(malformed, Pepper));
            Assert.Throws<EncoderFallbackException>(() =>
                RateLimiting.RateLimitKey.FromSensitive(malformed)
            );
            Assert.Throws<EncoderFallbackException>(() =>
                RateLimiting.RateLimitKey.FromSensitive(malformed, Pepper)
            );
            Assert.False(CacheKey.IsValid(malformed, 512));
            Assert.Throws<ArgumentException>(() => LockKey.Validate(malformed));
        }
        const string valid = "catalog\U0001f4da";
        var bytes = Encoding.UTF8.GetBytes(valid);
        var expected = Convert.ToHexStringLower(SHA256.HashData(bytes))[..32];
        var keyed = Convert.ToHexStringLower(HMACSHA256.HashData(Pepper, bytes))[..32];
        Assert.True(CacheKey.IsValid(valid, 512));
        LockKey.Validate(valid);
        Assert.Equal(expected, CacheKey.FromSensitive(valid));
        Assert.Equal(expected, LockKey.FromSensitive(valid));
        Assert.Equal(expected, RateLimiting.RateLimitKey.FromSensitive(valid));
        Assert.Equal(keyed, CacheKey.FromSensitive(valid, Pepper));
        Assert.Equal(keyed, LockKey.FromSensitive(valid, Pepper));
        Assert.Equal(keyed, RateLimiting.RateLimitKey.FromSensitive(valid, Pepper));
    }

    [Fact]
    public void KeyedHash_DiffersFromTheUnkeyedHashAndFromOtherKeys()
    {
        var unkeyed = CacheKey.FromSensitive("1234");
        var keyed = CacheKey.FromSensitive("1234", Pepper);
        var otherKey = CacheKey.FromSensitive("1234", "other-pepper"u8);

        Assert.Equal(32, keyed.Length);
        Assert.Matches("^[0-9a-f]{32}$", keyed);
        Assert.NotEqual(unkeyed, keyed);
        Assert.NotEqual(keyed, otherKey);
        Assert.Equal(keyed, CacheKey.FromSensitive("1234", Pepper));
        Assert.NotEqual(keyed, CacheKey.FromSensitive("1235", Pepper));

        // Both kernels derive the same key, so a cache entry and a lock can share one secret.
        Assert.Equal(keyed, LockKey.FromSensitive("1234", Pepper));
        Assert.Equal(unkeyed, LockKey.FromSensitive("1234"));
    }

    [Fact]
    public void KeyedHash_IsTruncatedHmacSha256OverUtf8()
    {
        var expected = Convert.ToHexStringLower(
            HMACSHA256.HashData(Pepper, Encoding.UTF8.GetBytes("1234"))
        )[..32];

        Assert.Equal(expected, CacheKey.FromSensitive("1234", Pepper));
        Assert.Equal(expected, LockKey.FromSensitive("1234", Pepper));
    }

    [Fact]
    public void KeyedHash_RejectsAnEmptyKeyAndANullValue()
    {
        Assert.Throws<ArgumentException>(() =>
            CacheKey.FromSensitive("1234", ReadOnlySpan<byte>.Empty)
        );
        Assert.Throws<ArgumentException>(() =>
            LockKey.FromSensitive("1234", ReadOnlySpan<byte>.Empty)
        );
        Assert.Throws<ArgumentNullException>(() => CacheKey.FromSensitive(null!, Pepper));
        Assert.Throws<ArgumentNullException>(() => LockKey.FromSensitive(null!, Pepper));
    }

    [Theory]
    [InlineData("catalog:eu", 512, true)]
    [InlineData("", 512, false)]
    [InlineData(null, 512, false)]
    [InlineData("catalog eu", 512, false)]
    [InlineData("catalog\u0007eu", 512, false)]
    [InlineData("catalog:eu", 5, false)]
    public void IsValid_MirrorsValidateWithoutThrowing(string? key, int maxLength, bool expected)
    {
        Assert.Equal(expected, CacheKey.IsValid(key, maxLength));
        if (expected)
        {
            CacheKey.Validate(key!, maxLength);
        }
        else if (key is not null)
        {
            Assert.Throws<ArgumentException>(() => CacheKey.Validate(key, maxLength));
        }
    }
}
