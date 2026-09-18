using System.Collections.ObjectModel;

namespace HostLoom.Mapping.DependencyInjection;

/// <summary>One registered source and destination pair.</summary>
public readonly record struct MappedTypePair(Type Source, Type Destination);

/// <summary>
/// The source and destination pairs registered with <c>AddHostLoomMapping</c>, so a service can
/// assert its expectations at startup and a missing pair can name what was registered instead.
/// </summary>
/// <remarks>
/// <para>
/// The registry is filled during registration and read afterwards. Registration is single-threaded
/// by construction — it happens while composing the container, before anything resolves — so the
/// registry takes no lock, and <see cref="Pairs"/> is a live view rather than a snapshot: reading
/// it mid-registration shows only what has been added so far.
/// </para>
/// <para>
/// Creation maps and update maps are listed separately. They are distinct services for the same
/// pair, and only creation maps are reachable through the <see cref="IMapper"/> dispatcher, so a
/// "registered to map to" hint in <see cref="MappingNotFoundException"/> must not suggest an
/// update map the dispatcher cannot resolve.
/// </para>
/// </remarks>
public sealed class MappedPairRegistry
{
    private readonly List<MappedTypePair> _pairs = [];
    private readonly List<MappedTypePair> _updatePairs = [];
    private readonly ReadOnlyCollection<MappedTypePair> _view;
    private readonly ReadOnlyCollection<MappedTypePair> _updateView;

    /// <summary>Creates an empty registry.</summary>
    public MappedPairRegistry()
    {
        _view = _pairs.AsReadOnly();
        _updateView = _updatePairs.AsReadOnly();
    }

    /// <summary>Every registered creation pair, in registration order.</summary>
    /// <remarks>
    /// A wrapper rather than the backing list, so a caller cannot downcast it and add a pair the
    /// container will never resolve. It stays a live view of registration, which is the point:
    /// reading it mid-registration shows what has been added so far.
    /// </remarks>
    public IReadOnlyList<MappedTypePair> Pairs => _view;

    /// <summary>
    /// Every registered update pair — an <see cref="IUpdateMapper{TSource, TDestination}"/> — in
    /// registration order.
    /// </summary>
    public IReadOnlyList<MappedTypePair> UpdatePairs => _updateView;

    /// <summary>The destinations a creation map is registered for from one source type.</summary>
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

    /// <summary>Reports whether a creation map is registered for one pair.</summary>
    public bool Contains(Type source, Type destination)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(destination);
        return Contains(_pairs, source, destination);
    }

    /// <summary>Reports whether an update map is registered for one pair.</summary>
    public bool ContainsUpdate(Type source, Type destination)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(destination);
        return Contains(_updatePairs, source, destination);
    }

    internal void Record(Type source, Type destination) =>
        _pairs.Add(new MappedTypePair(source, destination));

    internal void RecordUpdate(Type source, Type destination) =>
        _updatePairs.Add(new MappedTypePair(source, destination));

    private static bool Contains(List<MappedTypePair> pairs, Type source, Type destination)
    {
        foreach (MappedTypePair pair in pairs)
        {
            if (pair.Source == source && pair.Destination == destination)
            {
                return true;
            }
        }

        return false;
    }
}
