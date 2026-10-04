using System.Globalization;

namespace HostLoom.Generators;

/// <summary>
/// Generates fixed-width numeric codes and numeric identifiers. All formatting uses the
/// invariant culture only; the current culture never affects output. There is no uniqueness
/// promise: two calls can return the same value, and durable uniqueness stays an atomic
/// constraint in the owning service. For one-time passwords, expiry, attempt limits, and
/// delivery remain caller responsibilities.
/// </summary>
public sealed class NumericCodeGenerator
{
    private readonly IRandomSource _random;

    /// <summary>
    /// Creates a generator that draws from <paramref name="random"/>, or from
    /// <see cref="SecureRandomSource.Instance"/> when <paramref name="random"/> is null.
    /// </summary>
    public NumericCodeGenerator(IRandomSource? random = null)
    {
        _random = random ?? SecureRandomSource.Instance;
    }

    /// <summary>
    /// Draws a value from [0, <see cref="NumericCodeProfile.MaxExclusive"/>) and formats it
    /// zero-padded to exactly <see cref="NumericCodeProfile.Digits"/> characters with the
    /// invariant culture.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="profile"/> is null.</exception>
    public string Generate(NumericCodeProfile profile)
    {
        ArgumentNullException.ThrowIfNull(profile);

        var value = _random.NextInt64(0, profile.MaxExclusive);
        return value.ToString(
            "D" + profile.Digits.ToString(CultureInfo.InvariantCulture),
            CultureInfo.InvariantCulture
        );
    }

    /// <summary>
    /// Draws a value from
    /// [<see cref="NumericIdProfile.MinInclusive"/>, <see cref="NumericIdProfile.MaxExclusive"/>)
    /// and formats it with the invariant culture, without padding.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="profile"/> is null.</exception>
    public string Generate(NumericIdProfile profile)
    {
        ArgumentNullException.ThrowIfNull(profile);

        var value = _random.NextInt64(profile.MinInclusive, profile.MaxExclusive);
        return value.ToString(CultureInfo.InvariantCulture);
    }
}
