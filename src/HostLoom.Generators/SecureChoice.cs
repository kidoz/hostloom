namespace HostLoom.Generators;

/// <summary>
/// Selects one element from an indexed collection using the secure, unbiased
/// <see cref="IRandomSource"/>. Caller mutation of the collection during the call is
/// unsupported.
/// </summary>
public static class SecureChoice
{
    /// <summary>
    /// Returns one element of <paramref name="items"/>, selected by
    /// <see cref="IRandomSource.NextInt32"/> over [0, <c>items.Count</c>), so every element has
    /// equal weight.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <exception cref="ArgumentNullException"><paramref name="items"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="items"/> is empty.</exception>
    public static T Choose<T>(IReadOnlyList<T> items, IRandomSource? random = null)
    {
        ArgumentNullException.ThrowIfNull(items);
        if (items.Count == 0)
        {
            throw new ArgumentException(
                "The collection must contain at least one element.",
                nameof(items)
            );
        }

        var source = random ?? SecureRandomSource.Instance;
        return items[source.NextInt32(0, items.Count)];
    }
}
