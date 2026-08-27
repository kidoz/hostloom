namespace HostLoom.Mapping;

/// <summary>
/// Dispatches a mapping through its compile-time source and destination types.
/// </summary>
/// <remarks>
/// Prefer taking <see cref="IMapper{TSource, TDestination}"/> when a component uses one
/// known mapping. This facade is intended for orchestration code that coordinates several maps.
/// </remarks>
public interface IMapper
{
    /// <summary>Maps <paramref name="source"/> to <typeparamref name="TDestination"/>.</summary>
    /// <exception cref="MappingNotFoundException">
    /// No mapping is registered for the source and destination pair.
    /// </exception>
    TDestination Map<TSource, TDestination>(TSource source)
        where TSource : notnull
        where TDestination : notnull;
}

/// <summary>
/// Implements one explicit mapping from <typeparamref name="TSource"/> to
/// <typeparamref name="TDestination"/>.
/// </summary>
/// <remarks>
/// Implementations should normally be deterministic and side-effect free. Dependencies needed
/// to calculate a value can be supplied through constructor injection; database and network I/O
/// belongs in an application service before or after mapping.
/// </remarks>
public interface IMapper<in TSource, out TDestination>
    where TSource : notnull
    where TDestination : notnull
{
    /// <summary>Maps one non-null source value.</summary>
    TDestination Map(TSource source);
}
