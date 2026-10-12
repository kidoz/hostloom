namespace HostLoom.NodeLeasing;

/// <summary>Pool-name hygiene shared by every elector.</summary>
public static class NodeLeasingPool
{
    /// <summary>The longest pool name accepted.</summary>
    public const int MaxLength = 128;

    /// <summary>The prefix every pool's lock keys carry: <c>nodeid:{pool}:{id}</c>.</summary>
    public const string KeyPrefix = "nodeid:";

    /// <summary>
    /// Rejects a pool that is empty, longer than <see cref="MaxLength"/>, or outside
    /// <c>[a-z0-9-]+</c>, the same shape as <c>Locking:Namespace</c>, so provider keys stay
    /// unambiguous across services. A pool becomes the middle segment of the lock key.
    /// </summary>
    /// <exception cref="ArgumentException">The pool is not acceptable.</exception>
    public static void Validate(string pool)
    {
        ArgumentException.ThrowIfNullOrEmpty(pool);
        if (pool.Length > MaxLength)
        {
            throw new ArgumentException(
                $"Node-leasing pool is {pool.Length} characters long; the maximum is {MaxLength}.",
                nameof(pool)
            );
        }

        foreach (var character in pool)
        {
            if (
                !(
                    char.IsAsciiLetterLower(character)
                    || char.IsAsciiDigit(character)
                    || character == '-'
                )
            )
            {
                throw new ArgumentException(
                    "Node-leasing pools must match [a-z0-9-]+ so provider keys stay unambiguous across services.",
                    nameof(pool)
                );
            }
        }
    }

    /// <summary>
    /// The lock key for <paramref name="id"/> in <paramref name="pool"/>, before the lock's own
    /// namespace prefix.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The ID is negative.</exception>
    public static string KeyFor(string pool, int id)
    {
        Validate(pool);
        ArgumentOutOfRangeException.ThrowIfNegative(id);
        return $"{KeyPrefix}{pool}:{id}";
    }
}
