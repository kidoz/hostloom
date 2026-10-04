namespace HostLoom.Generators.Testing;

/// <summary>
/// A deterministic <see cref="IRandomSource"/> over <see cref="Random"/>: two instances created
/// with the same seed produce the same sequence, which makes a test repeatable across runs.
/// <para>
/// The values are unbiased enough for shape and membership tests, but they are NOT
/// cryptographically unbiased and MUST NOT appear in production code: this source exists so
/// tests can pin outcomes. Production code draws from
/// <see cref="SecureRandomSource.Instance"/>.
/// </para>
/// </summary>
public sealed class SeededRandomSource : IRandomSource
{
    private readonly Random _random;

    /// <summary>
    /// Creates a source whose sequence is fully determined by <paramref name="seed"/>. The
    /// instance is not thread-safe; create one per thread or per generator under test.
    /// </summary>
    public SeededRandomSource(int seed)
    {
        _random = new Random(seed);
    }

    /// <inheritdoc />
#pragma warning disable CA5394 // Insecure by design: this is a deterministic test fake, never production entropy.
    public void Fill(Span<byte> destination)
    {
        _random.NextBytes(destination);
    }

    /// <inheritdoc />
    public int NextInt32(int minInclusive, int maxExclusive)
    {
        return _random.Next(minInclusive, maxExclusive);
    }

    /// <inheritdoc />
    public long NextInt64(long minInclusive, long maxExclusive)
    {
        return _random.NextInt64(minInclusive, maxExclusive);
    }
#pragma warning restore CA5394
}
