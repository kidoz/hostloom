using System.Security.Cryptography;
using System.Text;

namespace HostLoom.RateLimiting;

/// <summary>Validation and safe partition-key derivation for rate limiters.</summary>
public static class RateLimitKey
{
    /// <summary>Hashes a high-entropy credential so the secret does not reach the store or telemetry.</summary>
    public static string FromSensitive(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(Encoding.UTF8.GetBytes(value), hash);
        return Convert.ToHexStringLower(hash[..16]);
    }

    /// <summary>Hashes a low-entropy secret with HMAC-SHA256 under a shared application key.</summary>
    public static string FromSensitive(string value, ReadOnlySpan<byte> key)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (key.IsEmpty)
        {
            throw new ArgumentException("The HMAC key must not be empty.", nameof(key));
        }

        Span<byte> hash = stackalloc byte[HMACSHA256.HashSizeInBytes];
        HMACSHA256.HashData(key, Encoding.UTF8.GetBytes(value), hash);
        return Convert.ToHexStringLower(hash[..16]);
    }

    /// <summary>Rejects an empty, overlong, whitespace-containing, or control-character key.</summary>
    public static void Validate(string key, int maxKeyLength = 512)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        if (key.Length > maxKeyLength)
        {
            throw new ArgumentException(
                $"Rate-limit key is {key.Length} characters long; the maximum is {maxKeyLength}.",
                nameof(key)
            );
        }

        if (!HasOnlyAllowedCharacters(key))
        {
            throw new ArgumentException(
                "A rate-limit key must not contain whitespace or control characters.",
                nameof(key)
            );
        }
    }

    /// <summary>Whether <paramref name="value"/> matches <c>[a-z0-9-]+</c>.</summary>
    public static bool IsValidNamespace(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return false;
        }

        foreach (var character in value)
        {
            if (
                !(
                    char.IsAsciiLetterLower(character)
                    || char.IsAsciiDigit(character)
                    || character == '-'
                )
            )
            {
                return false;
            }
        }

        return true;
    }

    private static bool HasOnlyAllowedCharacters(string key)
    {
        foreach (var character in key)
        {
            if (char.IsWhiteSpace(character) || char.IsControl(character))
            {
                return false;
            }
        }

        return true;
    }
}
