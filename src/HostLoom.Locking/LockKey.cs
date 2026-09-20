using System.Security.Cryptography;
using System.Text;

namespace HostLoom.Locking;

/// <summary>Key hygiene shared by every lock: validation and the one sanctioned way to key on a secret.</summary>
public static class LockKey
{
    /// <summary>
    /// Hashes a high-entropy credential such as a bearer token, refresh token, session id, or
    /// API key (SHA-256, first 32 lowercase hex characters) so it never reaches the provider, a
    /// log line, or a span. The hash is unkeyed: anyone who can read the key can confirm a guess
    /// of the input, so a password, PIN, or other low-entropy secret must go through
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
    /// against a guess. Keep the key outside the provider, in configuration or a secret store,
    /// and share it across the instances of one service, or two instances will not contend for
    /// the same lock.
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
    /// Rejects a key that is empty, longer than <paramref name="maxKeyLength"/>, or contains
    /// whitespace or control characters. Keys are otherwise opaque; <c>:</c> is the conventional
    /// separator.
    /// </summary>
    /// <exception cref="ArgumentException">The key is not acceptable.</exception>
    public static void Validate(string key, int maxKeyLength = 512)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        if (key.Length > maxKeyLength)
        {
            throw new ArgumentException(
                $"Lock key is {key.Length} characters long; the maximum is {maxKeyLength} (Locking:MaxKeyLength).",
                nameof(key)
            );
        }

        foreach (var character in key)
        {
            if (char.IsWhiteSpace(character) || char.IsControl(character))
            {
                throw new ArgumentException(
                    "Lock keys must not contain whitespace or control characters.",
                    nameof(key)
                );
            }
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
}
