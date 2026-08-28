namespace HostLoom.Mapping;

/// <summary>
/// A short-lived, allocation-free mapping request that keeps the source type inferred while the
/// destination is selected with <see cref="To{TDestination}"/>.
/// </summary>
public readonly record struct MappingSource<TSource>
    where TSource : notnull
{
    private readonly IMapper _mapper;
    private readonly TSource _source;

    internal MappingSource(IMapper mapper, TSource source)
    {
        _mapper = mapper;
        _source = source;
    }

    /// <summary>Maps the captured source to <typeparamref name="TDestination"/>.</summary>
    /// <exception cref="InvalidOperationException">
    /// The value was never obtained from <see cref="MapperExtensions.From"/> and so carries no
    /// mapper — the uninitialized value of any struct is always reachable.
    /// </exception>
    public TDestination To<TDestination>()
        where TDestination : notnull =>
        _mapper is null
            ? throw new InvalidOperationException(
                $"This {nameof(MappingSource<TSource>)} carries no mapper. Obtain one from "
                    + $"{nameof(MapperExtensions.From)} rather than using the default value."
            )
            : _mapper.Map<TSource, TDestination>(_source);
}
