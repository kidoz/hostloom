namespace HostLoom.Generators;

/// <summary>
/// Immutable profile for fixed-width numeric codes such as confirmation codes: a value drawn
/// from [0, <see cref="MaxExclusive"/>) and zero-padded to exactly <see cref="Digits"/>
/// characters. Leading zeros are part of the contract, so <c>00042</c> is a valid five-digit
/// code.
/// </summary>
public sealed class NumericCodeProfile
{
    /// <summary>
    /// Creates a profile with <paramref name="digits"/> in [1, 18]. The effective
    /// <paramref name="maxExclusive"/> defaults to 10^digits (computed with checked arithmetic),
    /// covering every <paramref name="digits"/>-digit value including the final one; a supplied
    /// value must lie in (0, 10^digits].
    /// </summary>
    /// <remarks>
    /// A legacy adapter reproduces an upper bound that excludes the final value, for example
    /// <c>new NumericCodeProfile(5, maxExclusive: 99999)</c> yields <c>00000</c>–<c>99998</c>.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="digits"/> is outside [1, 18], or <paramref name="maxExclusive"/> is
    /// outside (0, 10^digits].
    /// </exception>
    public NumericCodeProfile(int digits, long? maxExclusive = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(digits, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(digits, 18);

        var rangeExclusive = checked(Pow10(digits));

        if (maxExclusive is { } supplied)
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(supplied, 0L);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(supplied, rangeExclusive);
            MaxExclusive = supplied;
        }
        else
        {
            MaxExclusive = rangeExclusive;
        }

        Digits = digits;
    }

    /// <summary>The exact number of characters in every generated code, zero-padded.</summary>
    public int Digits { get; }

    /// <summary>
    /// The exclusive upper bound of the drawn value. Defaults to 10^<see cref="Digits"/>;
    /// supply a smaller bound to reproduce a legacy range that excludes the final value.
    /// </summary>
    public long MaxExclusive { get; }

    private static long Pow10(int digits)
    {
        var result = 1L;
        for (var index = 0; index < digits; index++)
        {
            result = checked(result * 10L);
        }

        return result;
    }
}
