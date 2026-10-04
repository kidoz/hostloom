using System.Security.Cryptography;

namespace HostLoom.Generators;

/// <summary>
/// Cryptographically secure <see cref="IRandomSource"/> backed by
/// <see cref="RandomNumberGenerator"/>. The BCL already performs unbiased rejection sampling
/// for <c>GetInt32</c>; <see cref="NextInt64"/> applies the same unbiased rejection sampling
/// over 64 bits, so every range is exact with no modulo bias.
/// </summary>
public sealed class SecureRandomSource : IRandomSource
{
    private SecureRandomSource() { }

    /// <summary>The process-wide shared instance. Thread-safe.</summary>
    public static SecureRandomSource Instance { get; } = new();

    /// <inheritdoc />
    public void Fill(Span<byte> destination)
    {
        RandomNumberGenerator.Fill(destination);
    }

    /// <inheritdoc />
    public int NextInt32(int minInclusive, int maxExclusive)
    {
        return RandomNumberGenerator.GetInt32(minInclusive, maxExclusive);
    }

    /// <inheritdoc />
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="minInclusive"/> is greater than <paramref name="maxExclusive"/>.
    /// Equal bounds return <paramref name="minInclusive"/> without drawing, matching
    /// <see cref="Random.NextInt64(long, long)"/>. <see cref="NextInt32"/> still throws.
    /// </exception>
    public long NextInt64(long minInclusive, long maxExclusive)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(minInclusive, maxExclusive);
        if (minInclusive == maxExclusive)
        {
            return minInclusive;
        }

        // The largest acceptable offset above minInclusive; spans the full long range without overflow.
        var range = (ulong)maxExclusive - (ulong)minInclusive - 1UL;
        var mask = range;
        mask |= mask >> 1;
        mask |= mask >> 2;
        mask |= mask >> 4;
        mask |= mask >> 8;
        mask |= mask >> 16;
        mask |= mask >> 32;

        Span<byte> buffer = stackalloc byte[sizeof(ulong)];
        ulong offset;
        do
        {
            RandomNumberGenerator.Fill(buffer);
            offset = BitConverter.ToUInt64(buffer) & mask;
        } while (offset > range);

        return (long)((ulong)minInclusive + offset);
    }
}
