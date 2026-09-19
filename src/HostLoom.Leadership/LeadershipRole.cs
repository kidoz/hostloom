namespace HostLoom.Leadership;

/// <summary>Role-name hygiene shared by every elector.</summary>
public static class LeadershipRole
{
    /// <summary>The longest role name accepted.</summary>
    public const int MaxLength = 128;

    /// <summary>The prefix every role's lock key carries: <c>leader:{role}</c>.</summary>
    public const string KeyPrefix = "leader:";

    /// <summary>
    /// Rejects a role that is empty, longer than <see cref="MaxLength"/>, or contains whitespace
    /// or control characters. A role becomes the suffix of the lock key.
    /// </summary>
    /// <exception cref="ArgumentException">The role is not acceptable.</exception>
    public static void Validate(string role)
    {
        ArgumentException.ThrowIfNullOrEmpty(role);
        if (role.Length > MaxLength)
        {
            throw new ArgumentException(
                $"Leadership role is {role.Length} characters long; the maximum is {MaxLength}.",
                nameof(role)
            );
        }

        foreach (var character in role)
        {
            if (char.IsWhiteSpace(character) || char.IsControl(character))
            {
                throw new ArgumentException(
                    "Leadership roles must not contain whitespace or control characters.",
                    nameof(role)
                );
            }
        }
    }

    /// <summary>The lock key for <paramref name="role"/>, before the lock's own namespace prefix.</summary>
    public static string KeyFor(string role)
    {
        Validate(role);
        return KeyPrefix + role;
    }
}
