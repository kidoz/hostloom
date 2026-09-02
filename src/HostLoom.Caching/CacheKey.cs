using System.Security.Cryptography;
using System.Text;

namespace HostLoom.Caching;

/// <summary>Helpers for building and validating cache keys.</summary>
public static class CacheKey
{
    /// <summary>
    /// Hashes a credential-bearing value (SHA-256, first 32 lowercase hex characters) so the
    /// secret itself never reaches the store, a log line, or a span.
    /// </summary>
    public static string FromSensitive(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(Encoding.UTF8.GetBytes(value), hash);
        return Convert.ToHexStringLower(hash[..16]);
    }

    /// <summary>
    /// Appends a per-call-site schema version to <paramref name="key"/>, so one consumer bumps
    /// its payload format without touching <see cref="CachingOptions.PayloadVersion"/>. The
    /// versioned key is an ordinary key: pass the same value to remove it.
    /// </summary>
    public static string Versioned(string key, string version)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentException.ThrowIfNullOrWhiteSpace(version);
        return string.Concat(key, ":v", version);
    }

    /// <summary>
    /// Rejects an empty key, one containing whitespace or control characters, or one longer than
    /// <paramref name="maxLength"/>. <c>:</c> is the conventional separator and is allowed.
    /// </summary>
    /// <exception cref="ArgumentException">The key is invalid.</exception>
    public static void Validate(string key, int maxLength, string parameterName = "key")
    {
        ArgumentNullException.ThrowIfNull(key, parameterName);
        if (key.Length == 0)
        {
            throw new ArgumentException("A cache key must not be empty.", parameterName);
        }

        if (key.Length > maxLength)
        {
            throw new ArgumentException(
                $"A cache key must not exceed {maxLength} characters; this one has {key.Length}.",
                parameterName
            );
        }

        foreach (var character in key)
        {
            if (char.IsWhiteSpace(character) || char.IsControl(character))
            {
                throw new ArgumentException(
                    "A cache key must not contain whitespace or control characters.",
                    parameterName
                );
            }
        }
    }
}
