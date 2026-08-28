using HostLoom.Mapping;

namespace HostLoom.Mapping.Testing;

/// <summary>
/// Composes an <see cref="IMapper"/> dispatcher from explicit maps, with no container.
/// </summary>
/// <remarks>
/// The core package deliberately has no dependency-injection dependency, which leaves a unit test
/// that wants the dispatcher building an <c>IServiceCollection</c> to get one. This builds the same
/// contract directly. Substituting the dispatcher is the other option and a worse one: it needs one
/// substitute per pair, and each returns whatever it was told to rather than what a map would.
/// </remarks>
public sealed class TestMapperBuilder
{
    private readonly Dictionary<(Type Source, Type Destination), object> _maps = [];

    /// <summary>Adds a map instance for its pair.</summary>
    /// <exception cref="InvalidOperationException">The pair is already added.</exception>
    public TestMapperBuilder Add<TSource, TDestination>(IMapper<TSource, TDestination> mapper)
        where TSource : notnull
        where TDestination : notnull
    {
        ArgumentNullException.ThrowIfNull(mapper);
        return Add<TSource, TDestination>((object)mapper);
    }

    /// <summary>
    /// Adds a map written inline, for a test that cares what the map produces rather than which
    /// class produced it.
    /// </summary>
    public TestMapperBuilder Add<TSource, TDestination>(Func<TSource, TDestination> map)
        where TSource : notnull
        where TDestination : notnull
    {
        ArgumentNullException.ThrowIfNull(map);
        return Add<TSource, TDestination>((object)new DelegateMapper<TSource, TDestination>(map));
    }

    /// <summary>Builds a dispatcher over the maps added so far.</summary>
    public IMapper Build() => new CompositeMapper(new Dictionary<(Type, Type), object>(_maps));

    private TestMapperBuilder Add<TSource, TDestination>(object mapper)
        where TSource : notnull
        where TDestination : notnull
    {
        (Type, Type) key = (typeof(TSource), typeof(TDestination));
        if (!_maps.TryAdd(key, mapper))
        {
            throw new InvalidOperationException(
                $"A mapping from '{typeof(TSource).FullName}' to '{typeof(TDestination).FullName}' "
                    + "is already added. The registered builder rejects duplicates the same way, so "
                    + "accepting one here would let a test pass against a container that cannot be "
                    + "composed."
            );
        }

        return this;
    }

    private sealed class DelegateMapper<TSource, TDestination>(Func<TSource, TDestination> map)
        : IMapper<TSource, TDestination>
        where TSource : notnull
        where TDestination : notnull
    {
        public TDestination Map(TSource source) => map(source);
    }

    private sealed class CompositeMapper(Dictionary<(Type Source, Type Destination), object> maps)
        : IMapper
    {
        public TDestination Map<TSource, TDestination>(TSource source)
            where TSource : notnull
            where TDestination : notnull
        {
            ArgumentNullException.ThrowIfNull(source);

            if (
                !maps.TryGetValue((typeof(TSource), typeof(TDestination)), out object? mapper)
                || mapper is not IMapper<TSource, TDestination> typed
            )
            {
                throw new MappingNotFoundException(
                    typeof(TSource),
                    typeof(TDestination),
                    DestinationsFor(typeof(TSource))
                );
            }

            return typed.Map(source);
        }

        private List<Type> DestinationsFor(Type source)
        {
            List<Type> destinations = [];
            foreach ((Type Source, Type Destination) key in maps.Keys)
            {
                if (key.Source == source)
                {
                    destinations.Add(key.Destination);
                }
            }

            return destinations;
        }
    }
}
