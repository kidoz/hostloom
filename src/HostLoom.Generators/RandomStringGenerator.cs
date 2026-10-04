namespace HostLoom.Generators;

/// <summary>
/// Generates exact-length random strings under a <see cref="RandomStringProfile"/>. Each
/// character is selected independently by <see cref="IRandomSource.NextInt32"/> over the whole
/// alphabet, so selection is unbiased. Thread-safe when the supplied
/// <see cref="IRandomSource"/> is thread-safe; <see cref="SecureRandomSource.Instance"/> is.
/// </summary>
public sealed class RandomStringGenerator
{
    private readonly IRandomSource _random;

    /// <summary>
    /// Creates a generator that draws from <paramref name="random"/>, or from
    /// <see cref="SecureRandomSource.Instance"/> when <paramref name="random"/> is null.
    /// </summary>
    public RandomStringGenerator(IRandomSource? random = null)
    {
        _random = random ?? SecureRandomSource.Instance;
    }

    /// <summary>
    /// Returns a new string of exactly <see cref="RandomStringProfile.Length"/> characters, each
    /// drawn from the profile alphabet with equal weight.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="profile"/> is null.</exception>
    public string Generate(RandomStringProfile profile)
    {
        ArgumentNullException.ThrowIfNull(profile);

        var state = (Characters: profile.Alphabet.Characters, Random: _random);
        return string.Create(
            profile.Length,
            state,
            static (destination, state) =>
            {
                for (var index = 0; index < destination.Length; index++)
                {
                    destination[index] = state.Characters[
                        state.Random.NextInt32(0, state.Characters.Length)
                    ];
                }
            }
        );
    }

    /// <summary>
    /// Writes a generated string into <paramref name="destination"/> instead of allocating one.
    /// Returns <see langword="false"/> without writing anything when
    /// <paramref name="destination"/> is shorter than <see cref="RandomStringProfile.Length"/>;
    /// excess capacity is left untouched.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="profile"/> is null.</exception>
    public bool TryGenerate(RandomStringProfile profile, Span<char> destination)
    {
        ArgumentNullException.ThrowIfNull(profile);

        if (destination.Length < profile.Length)
        {
            return false;
        }

        var characters = profile.Alphabet.Characters;
        for (var index = 0; index < profile.Length; index++)
        {
            destination[index] = characters[_random.NextInt32(0, characters.Length)];
        }

        return true;
    }
}
