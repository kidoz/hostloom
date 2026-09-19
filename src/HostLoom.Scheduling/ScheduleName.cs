namespace HostLoom.Scheduling;

/// <summary>Name hygiene shared by every schedule.</summary>
public static class ScheduleName
{
    /// <summary>The longest schedule name accepted.</summary>
    public const int MaxLength = 128;

    /// <summary>
    /// Rejects a name that is empty, longer than <see cref="MaxLength"/>, or contains whitespace
    /// or control characters. Names are otherwise opaque; <c>:</c> and <c>-</c> are the
    /// conventional separators, and a name becomes the suffix of a guard key.
    /// </summary>
    /// <exception cref="ArgumentException">The name is not acceptable.</exception>
    public static void Validate(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        if (name.Length > MaxLength)
        {
            throw new ArgumentException(
                $"Schedule name is {name.Length} characters long; the maximum is {MaxLength}.",
                nameof(name)
            );
        }

        foreach (var character in name)
        {
            if (char.IsWhiteSpace(character) || char.IsControl(character))
            {
                throw new ArgumentException(
                    "Schedule names must not contain whitespace or control characters.",
                    nameof(name)
                );
            }
        }
    }
}
