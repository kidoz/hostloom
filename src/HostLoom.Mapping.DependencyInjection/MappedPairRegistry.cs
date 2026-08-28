using System.Collections.ObjectModel;

namespace HostLoom.Mapping.DependencyInjection;

/// <summary>One registered source and destination pair.</summary>
public readonly record struct MappedTypePair(Type Source, Type Destination);

/// <summary>
/// The source and destination pairs registered with <c>AddHostLoomMapping</c>, so a service can
/// assert its expectations at startup and a missing pair can name what was registered instead.
/// </summary>
/// <remarks>
/// The registry is filled during registration and read afterwards. Registration is single-threaded
/// by construction — it happens while composing the container, before anything resolves — so the
/// registry takes no lock, and <see cref="Pairs"/> is a live view rather than a snapshot: reading
/// it mid-registration shows only what has been added so far.
/// </remarks>
public sealed class MappedPairRegistry
{
    private readonly List<MappedTypePair> _pairs = [];
    private readonly ReadOnlyCollection<MappedTypePair> _view;

    /// <summary>Creates an empty registry.</summary>
    public MappedPairRegistry() => _view = _pairs.AsReadOnly();

    /// <summary>Every registered pair, in registration order.</summary>
    /// <remarks>
    /// A wrapper rather than the backing list, so a caller cannot downcast it and add a pair the
    /// container will never resolve. It stays a live view of registration, which is the point:
    /// reading it mid-registration shows what has been added so far.
    /// </remarks>
    public IReadOnlyList<MappedTypePair> Pairs => _view;

    /// <summary>The destinations registered for one source type.</summary>
    public IReadOnlyList<Type> DestinationsFor(Type source)
    {
        ArgumentNullException.ThrowIfNull(source);

        List<Type>? destinations = null;
        foreach (MappedTypePair pair in _pairs)
        {
            if (pair.Source == source)
            {
                destinations ??= [];
                destinations.Add(pair.Destination);
            }
        }

        return destinations ?? (IReadOnlyList<Type>)[];
    }

    /// <summary>Reports whether one pair is registered.</summary>
    public bool Contains(Type source, Type destination)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(destination);

        foreach (MappedTypePair pair in _pairs)
        {
            if (pair.Source == source && pair.Destination == destination)
            {
                return true;
            }
        }

        return false;
    }

    internal void Record(Type source, Type destination) =>
        _pairs.Add(new MappedTypePair(source, destination));
}
