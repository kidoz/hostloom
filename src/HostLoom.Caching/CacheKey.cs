using System.Security.Cryptography;
using System.Text;

namespace HostLoom.Caching;

/// <summary>Helpers for building and validating cache keys.</summary>
public static class CacheKey
{
    /// <summary>
    /// Hashes a high-entropy credential such as a bearer token, refresh token, or session id
    /// (SHA-256, first 32 lowercase hex characters) so the secret itself never reaches the store,
    /// a log line, or a span. The hash is unkeyed: anyone who can read the key can confirm a
    /// guess of the input, so a password, PIN, or other low-entropy secret must go through
    /// <see cref="FromSensitive(string, ReadOnlySpan{byte})"/> instead.
    /// </summary>
    public static string FromSensitive(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(Encoding.UTF8.GetBytes(value), hash);
        return Convert.ToHexStringLower(hash[..16]);
    }

    /// <summary>
    /// Hashes a low-entropy secret such as a password, PIN, or one-time code with HMAC-SHA256
    /// under <paramref name="key"/>, truncated and encoded like <see cref="FromSensitive(string)"/>
    /// (first 32 lowercase hex characters). Without the key the stored value cannot be tested
    /// against a guess. Keep the key outside the cache, in configuration or a secret store, and
    /// share it across the instances of one service; rotating it makes every derived key a miss.
    /// </summary>
    /// <exception cref="ArgumentException"><paramref name="key"/> is empty.</exception>
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

        if (!HasOnlyAllowedCharacters(key))
        {
            throw new ArgumentException(
                "A cache key must not contain whitespace or control characters.",
                parameterName
            );
        }
    }

    /// <summary>
    /// Whether <see cref="Validate"/> would accept <paramref name="key"/>: not null or empty, at
    /// most <paramref name="maxLength"/> characters, and free of whitespace and control
    /// characters. Meant for input that arrives over the wire, where an exception per bad item
    /// would be the wrong cost.
    /// </summary>
    public static bool IsValid(string? key, int maxLength) =>
        key is { Length: > 0 } && key.Length <= maxLength && HasOnlyAllowedCharacters(key);

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
