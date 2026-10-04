namespace HostLoom.Generators;

/// <summary>
/// Immutable, thread-safe settings for <see cref="RandomStringGenerator"/>: one alphabet and an
/// exact output length. Profiles carry no state and can be shared across generators and threads.
/// </summary>
public sealed class RandomStringProfile
{
    /// <summary>
    /// Creates a profile that produces strings of exactly <paramref name="length"/> characters
    /// drawn from <paramref name="alphabet"/>.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="alphabet"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="length"/> is outside [1, 4096].
    /// </exception>
    public RandomStringProfile(RandomAlphabet alphabet, int length)
    {
        ArgumentNullException.ThrowIfNull(alphabet);
        ArgumentOutOfRangeException.ThrowIfLessThan(length, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(length, 4096);

        Alphabet = alphabet;
        Length = length;
    }

    /// <summary>The alphabet each character is drawn from.</summary>
    public RandomAlphabet Alphabet { get; }

    /// <summary>The exact number of characters every generated string contains.</summary>
    public int Length { get; }
}
