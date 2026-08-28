namespace HostLoom.Mapping;

/// <summary>Ergonomic extensions over the mapping contracts.</summary>
/// <remarks>
/// The null policy is carried by the method name rather than by configuration. <c>MapMany</c> and
/// <see cref="IMapper{TSource, TDestination}.Map"/> reject null; <c>MapManyOrEmpty</c> and
/// <c>MapOrNull</c> accept it and say so at the call site. A convention mapper decides this once,
/// globally and invisibly; here every place that depends on null tolerance is greppable.
/// </remarks>
public static class MapperExtensions
{
    /// <summary>
    /// Captures a source so callers can write <c>mapper.From(source).To&lt;Destination&gt;()</c>
    /// without repeating the inferred source type.
    /// </summary>
    public static MappingSource<TSource> From<TSource>(this IMapper mapper, TSource source)
        where TSource : notnull
    {
        ArgumentNullException.ThrowIfNull(mapper);
        ArgumentNullException.ThrowIfNull(source);
        return new MappingSource<TSource>(mapper, source);
    }

    /// <summary>Maps every element of <paramref name="source"/>, which must not be null.</summary>
    /// <remarks>
    /// Elements are not null-checked individually. <typeparamref name="TSource"/> is constrained
    /// <c>notnull</c>, so a null element is a contract violation the map class sees directly
    /// rather than an outcome this method silently substitutes for.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
    public static IReadOnlyList<TDestination> MapMany<TSource, TDestination>(
        this IMapper<TSource, TDestination> mapper,
        IEnumerable<TSource> source
    )
        where TSource : notnull
        where TDestination : notnull
    {
        ArgumentNullException.ThrowIfNull(mapper);
        ArgumentNullException.ThrowIfNull(source);
        return MapSequence(mapper, source);
    }

    /// <summary>
    /// Maps every element, treating a null <paramref name="source"/> as an empty sequence.
    /// </summary>
    /// <remarks>
    /// This is the shape AutoMapper produces by default (<c>AllowNullCollections = false</c>), so
    /// it is the behaviour-preserving choice when migrating a collection map. Prefer
    /// <see cref="MapMany"/> in new code, where a null collection is usually a defect rather than
    /// an empty result.
    /// </remarks>
    public static IReadOnlyList<TDestination> MapManyOrEmpty<TSource, TDestination>(
        this IMapper<TSource, TDestination> mapper,
        IEnumerable<TSource>? source
    )
        where TSource : notnull
        where TDestination : notnull
    {
        ArgumentNullException.ThrowIfNull(mapper);
        return source is null ? [] : MapSequence(mapper, source);
    }

    /// <summary>
    /// Maps lazily, one element per enumeration step, for scans too large to materialize.
    /// </summary>
    /// <remarks>
    /// Deferred execution over a map class that holds scoped dependencies is a trap: enumerate the
    /// result before the scope that resolved the mapper ends, or the dependencies are already
    /// disposed. Arguments are validated eagerly, but the mapping itself is not. Prefer
    /// <see cref="MapMany"/> unless the size of the sequence is the reason not to.
    /// </remarks>
    public static IEnumerable<TDestination> MapManyDeferred<TSource, TDestination>(
        this IMapper<TSource, TDestination> mapper,
        IEnumerable<TSource> source
    )
        where TSource : notnull
        where TDestination : notnull
    {
        ArgumentNullException.ThrowIfNull(mapper);
        ArgumentNullException.ThrowIfNull(source);
        return Iterate(mapper, source);

        // A local iterator keeps the guards above eager; an iterator body alone would defer them
        // to the first MoveNext, reporting a null argument far from the call that passed it.
        static IEnumerable<TDestination> Iterate(
            IMapper<TSource, TDestination> mapper,
            IEnumerable<TSource> source
        )
        {
            foreach (var item in source)
            {
                yield return mapper.Map(item);
            }
        }
    }

    /// <summary>Maps one value, returning null when <paramref name="source"/> is null.</summary>
    /// <remarks>
    /// The closed contract requires a non-null source, which is the right default. This overload
    /// is the explicit opt-in for the AutoMapper behaviour being migrated away from, where a null
    /// scalar mapped to null.
    /// </remarks>
    public static TDestination? MapOrNull<TSource, TDestination>(
        this IMapper<TSource, TDestination> mapper,
        TSource? source
    )
        where TSource : class
        where TDestination : class
    {
        ArgumentNullException.ThrowIfNull(mapper);
        return source is null ? null : mapper.Map(source);
    }

    private static IReadOnlyList<TDestination> MapSequence<TSource, TDestination>(
        IMapper<TSource, TDestination> mapper,
        IEnumerable<TSource> source
    )
        where TSource : notnull
        where TDestination : notnull
    {
        // Arrays, List<T>, and IReadOnlyList<T> contract members all land here, which covers the
        // sources a map is realistically handed. Indexing fills an exact-size result with no
        // enumerator and no growth, so this costs what the hand-written loop it replaces cost.
        if (source is IReadOnlyList<TSource> indexable)
        {
            if (indexable.Count == 0)
            {
                return [];
            }

            var mapped = new TDestination[indexable.Count];
            for (var index = 0; index < mapped.Length; index++)
            {
                mapped[index] = mapper.Map(indexable[index]);
            }

            return mapped;
        }

        var results = source.TryGetNonEnumeratedCount(out var count)
            ? new List<TDestination>(count)
            : [];
        foreach (var item in source)
        {
            results.Add(mapper.Map(item));
        }

        return results;
    }
}
