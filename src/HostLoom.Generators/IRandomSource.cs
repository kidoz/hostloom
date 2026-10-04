namespace HostLoom.Generators;

/// <summary>
/// Entropy seam for every generator in this package. The production default,
/// <see cref="SecureRandomSource.Instance"/>, is backed by a cryptographic random number
/// generator; implementations must produce statistically unbiased values across the whole
/// requested range. This interface is the explicit, documented test seam: substitute a
/// deterministic implementation (the fake ships in HostLoom.Generators.Testing) to force
/// characters, bounds, and collisions in tests. Production code must not silently switch to
/// seeded pseudorandomness.
/// </summary>
public interface IRandomSource
{
    /// <summary>Fills <paramref name="destination"/> with independent random bytes.</summary>
    void Fill(Span<byte> destination);

    /// <summary>
    /// Returns an unbiased random integer in [<paramref name="minInclusive"/>,
    /// <paramref name="maxExclusive"/>).
    /// </summary>
    int NextInt32(int minInclusive, int maxExclusive);

    /// <summary>
    /// Returns an unbiased random integer in [<paramref name="minInclusive"/>,
    /// <paramref name="maxExclusive"/>).
    /// </summary>
    long NextInt64(long minInclusive, long maxExclusive);
}
