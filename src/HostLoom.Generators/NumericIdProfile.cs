namespace HostLoom.Generators;

/// <summary>
/// Immutable profile for public numeric identifiers drawn from
/// [<see cref="MinInclusive"/>, <see cref="MaxExclusive"/>). Unlike
/// <see cref="NumericCodeProfile"/>, values are never zero-padded; pick
/// <see cref="MinInclusive"/> = 10^(d−1) so identifiers never start with a zero. For example, a
/// ten-digit identifier is <c>new NumericIdProfile(1_000_000_000L, 10_000_000_000L)</c>.
/// </summary>
public sealed class NumericIdProfile
{
    /// <summary>
    /// Creates a profile whose drawn values lie in
    /// [<paramref name="minInclusive"/>, <paramref name="maxExclusive"/>).
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="minInclusive"/> is less than 1, or not less than
    /// <paramref name="maxExclusive"/>.
    /// </exception>
    public NumericIdProfile(long minInclusive, long maxExclusive)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(minInclusive, 1L);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(minInclusive, maxExclusive);

        MinInclusive = minInclusive;
        MaxExclusive = maxExclusive;
    }

    /// <summary>The inclusive lower bound of the drawn value; at least 1, so no leading zeros.</summary>
    public long MinInclusive { get; }

    /// <summary>The exclusive upper bound of the drawn value.</summary>
    public long MaxExclusive { get; }
}
