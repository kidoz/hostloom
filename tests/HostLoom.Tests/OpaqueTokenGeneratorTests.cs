using System.Buffers.Text;
using HostLoom.Generators;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// An <see cref="OpaqueTokenGenerator"/> fills a fresh buffer per call and encodes it as
/// unpadded Base64Url or hexadecimal, so a 32-byte token is exactly 43 characters with no '='
/// padding and round-trips back to the same 32 random bytes.
/// </summary>
public sealed class OpaqueTokenGeneratorTests
{
    [Fact]
    public void A_32_byte_token_is_43_unpadded_base64url_characters()
    {
        var generator = new OpaqueTokenGenerator();

        var token = generator.GenerateToken(32);

        Assert.Equal(43, token.Length);
        Assert.DoesNotContain('=', token);
        var decoded = Base64Url.DecodeFromChars(token);
        Assert.Equal(32, decoded.Length);
    }

    [Fact]
    public void An_8_byte_hex_nonce_is_16_lowercase_characters()
    {
        var generator = new OpaqueTokenGenerator();

        var nonce = generator.GenerateHexNonce(8);

        Assert.Equal(16, nonce.Length);
        Assert.True(nonce.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f'));
    }

    [Fact]
    public void The_uppercase_hex_variant_uses_uppercase_characters()
    {
        var generator = new OpaqueTokenGenerator();

        var nonce = generator.GenerateHexNonce(8, uppercase: true);

        Assert.Equal(16, nonce.Length);
        Assert.True(nonce.All(c => c is >= '0' and <= '9' or >= 'A' and <= 'F'));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(1025)]
    public void Invalid_byte_lengths_are_rejected(int byteLength)
    {
        var generator = new OpaqueTokenGenerator();

        Assert.Throws<ArgumentOutOfRangeException>(() => generator.GenerateToken(byteLength));
        Assert.Throws<ArgumentOutOfRangeException>(() => generator.GenerateHexNonce(byteLength));
    }

    [Fact]
    public void Successive_tokens_differ()
    {
        var generator = new OpaqueTokenGenerator();

        Assert.NotEqual(generator.GenerateToken(32), generator.GenerateToken(32));
        Assert.NotEqual(generator.GenerateHexNonce(8), generator.GenerateHexNonce(8));
    }
}
