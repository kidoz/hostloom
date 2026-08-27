using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Mapping.DependencyInjection;

internal sealed class ServiceProviderMapper(IServiceProvider services) : IMapper
{
    public TDestination Map<TSource, TDestination>(TSource source)
        where TSource : notnull
        where TDestination : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        var mapper = services.GetService<IMapper<TSource, TDestination>>();
        return mapper is null
            ? throw new MappingNotFoundException(typeof(TSource), typeof(TDestination))
            : mapper.Map(source);
    }
}
