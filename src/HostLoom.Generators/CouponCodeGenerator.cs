namespace HostLoom.Generators;

/// <summary>
/// Generates and validates human-readable coupon codes under a <see cref="CouponCodeProfile"/>.
/// <para>
/// Check digit algorithm, part of the versioned compatibility contract: for part index
/// <c>p</c> (0-based) with random characters <c>c0…c(k−1)</c> whose alphabet indices are
/// <c>a(c)</c>, the check index is
/// <c>(p + Σ (i+1)·a(ci)) mod alphabet.Length</c> and the check character is
/// <c>alphabet[checkIndex]</c>, appended as the part's last character. The term <c>p</c> makes
/// the check position-aware: the same characters fail validation when moved to a different part
/// position.
/// </para>
/// <para>
/// Check digits detect transcription typos before a code is submitted; they are not
/// authentication and add no entropy. Codes issued by an older generator or library are never
/// regenerated or rewritten by this generator.
/// </para>
/// </summary>
public sealed class CouponCodeGenerator
{
    private readonly IRandomSource _random;

    /// <summary>
    /// Creates a generator that draws from <paramref name="random"/>, or from
    /// <see cref="SecureRandomSource.Instance"/> when <paramref name="random"/> is null.
    /// </summary>
    public CouponCodeGenerator(IRandomSource? random = null)
    {
        _random = random ?? SecureRandomSource.Instance;
    }

    /// <summary>
    /// Generates a code of <see cref="CouponCodeProfile.PartCount"/> parts of
    /// <see cref="CouponCodeProfile.PartLength"/> characters each, joined by
    /// <see cref="CouponCodeProfile.Separator"/>.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="profile"/> is null.</exception>
    public string Generate(CouponCodeProfile profile)
    {
        ArgumentNullException.ThrowIfNull(profile);

        var totalLength = checked(profile.PartCount * profile.PartLength + (profile.PartCount - 1));
        var state = (Profile: profile, Random: _random);
        return string.Create(
            totalLength,
            state,
            static (destination, state) =>
            {
                var profile = state.Profile;
                var characters = profile.Alphabet.Characters;
                var randomPerPart = profile.IncludeCheckDigit
                    ? profile.PartLength - 1
                    : profile.PartLength;
                var position = 0;
                for (var part = 0; part < profile.PartCount; part++)
                {
                    if (part > 0)
                    {
                        destination[position++] = profile.Separator;
                    }

                    var checkSum = part;
                    for (var index = 0; index < randomPerPart; index++)
                    {
                        var alphabetIndex = state.Random.NextInt32(0, characters.Length);
                        destination[position++] = characters[alphabetIndex];
                        checkSum += (index + 1) * alphabetIndex;
                    }

                    if (profile.IncludeCheckDigit)
                    {
                        destination[position++] = characters[checkSum % characters.Length];
                    }
                }
            }
        );
    }

    /// <summary>
    /// Trims and uppercases <paramref name="code"/> with the invariant culture. Normalization
    /// is not validation: it makes a typed-in code comparable but accepts any characters.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="code"/> is null.</exception>
    public static string Normalize(string code)
    {
        ArgumentNullException.ThrowIfNull(code);

        return code.Trim().ToUpperInvariant();
    }

    /// <summary>
    /// Validates <paramref name="code"/> against <paramref name="profile"/>: normalizes it,
    /// splits on the separator, and verifies the part count, the exact length of every part,
    /// alphabet membership of every character, and every check digit when the profile enables
    /// them. On success, <paramref name="normalized"/> receives the normalized code; on any
    /// mismatch the result is <see langword="false"/> and <paramref name="normalized"/> is null.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="profile"/> is null.</exception>
#pragma warning disable CA1822 // Instance member by design: validation is part of the generator surface, not a static utility.
    public bool TryValidate(string? code, CouponCodeProfile profile, out string? normalized)
#pragma warning restore CA1822
    {
        ArgumentNullException.ThrowIfNull(profile);

        normalized = null;
        if (string.IsNullOrEmpty(code))
        {
            return false;
        }

        var candidate = Normalize(code);
        var parts = candidate.Split(profile.Separator);
        if (parts.Length != profile.PartCount)
        {
            return false;
        }

        var characters = profile.Alphabet.Characters;
        for (var part = 0; part < parts.Length; part++)
        {
            var value = parts[part];
            if (value.Length != profile.PartLength)
            {
                return false;
            }

            var randomPerPart = profile.IncludeCheckDigit
                ? profile.PartLength - 1
                : profile.PartLength;
            var checkSum = part;
            for (var index = 0; index < value.Length; index++)
            {
                var alphabetIndex = characters.IndexOf(value[index], StringComparison.Ordinal);
                if (alphabetIndex < 0)
                {
                    return false;
                }

                if (index < randomPerPart)
                {
                    checkSum += (index + 1) * alphabetIndex;
                }
                else if (characters[checkSum % characters.Length] != value[index])
                {
                    return false;
                }
            }
        }

        normalized = candidate;
        return true;
    }
}
