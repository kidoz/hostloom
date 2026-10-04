using System.Buffers.Text;

namespace HostLoom.Generators;

/// <summary>
/// Generates opaque tokens and nonces from independent random bytes: every call fills a fresh
/// buffer, so two calls never share state. These are opaque secrets or identifiers — never log
/// generated values, and never use them where an ordered identifier is expected: tokens are not
/// ordered.
/// </summary>
public sealed class OpaqueTokenGenerator
{
    private readonly IRandomSource _random;

    /// <summary>
    /// Creates a generator that draws from <paramref name="random"/>, or from
    /// <see cref="SecureRandomSource.Instance"/> when <paramref name="random"/> is null.
    /// </summary>
    public OpaqueTokenGenerator(IRandomSource? random = null)
    {
        _random = random ?? SecureRandomSource.Instance;
    }

    /// <summary>
    /// Returns <paramref name="byteLength"/> random bytes encoded with
    /// <see cref="Base64Url.EncodeToString(ReadOnlySpan{byte})"/>: unpadded Base64Url, so 32
    /// bytes produce exactly 43 characters.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="byteLength"/> is outside [1, 1024].
    /// </exception>
    public string GenerateToken(int byteLength)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(byteLength, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(byteLength, 1024);

        var bytes = new byte[byteLength];
        _random.Fill(bytes);
        return Base64Url.EncodeToString(bytes);
    }

    /// <summary>
    /// Returns <paramref name="byteLength"/> random bytes encoded as hexadecimal, lowercase by
    /// default, so 8 bytes produce exactly 16 characters.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="byteLength"/> is outside [1, 1024].
    /// </exception>
    public string GenerateHexNonce(int byteLength, bool uppercase = false)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(byteLength, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(byteLength, 1024);

        var bytes = new byte[byteLength];
        _random.Fill(bytes);
        return uppercase ? Convert.ToHexString(bytes) : Convert.ToHexStringLower(bytes);
    }
}
