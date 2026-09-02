using System.Security.Cryptography;
using System.Text;

namespace HostLoom.Locking;

/// <summary>Key hygiene shared by every lock: validation and the one sanctioned way to key on a secret.</summary>
public static class LockKey
{
    /// <summary>
    /// Hashes <paramref name="value"/> (SHA-256, first 32 lowercase hex characters) so a
    /// credential never reaches the provider, a log line, or a span. Use it for keys built from
    /// tokens, secrets, passwords, or API keys.
    /// </summary>
    public static string FromSensitive(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(Encoding.UTF8.GetBytes(value), hash);
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
