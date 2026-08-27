using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Mapping.DependencyInjection;

/// <summary>Registers explicit source/destination mappings with the .NET service container.</summary>
public sealed class MappingBuilder
{
    internal MappingBuilder(IServiceCollection services) => Services = services;

    /// <summary>The service collection receiving mapping registrations.</summary>
    public IServiceCollection Services { get; }

    /// <summary>
    /// Registers <typeparamref name="TMapper"/> for one type pair. The default transient lifetime
    /// is safe for map classes that take scoped services through constructor injection. As with
    /// other implementation types in the built-in container, the map class must have a public
    /// constructor.
    /// </summary>
    public MappingBuilder Add<
        TSource,
        TDestination,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TMapper
    >(ServiceLifetime lifetime = ServiceLifetime.Transient)
        where TSource : notnull
        where TDestination : notnull
        where TMapper : class, IMapper<TSource, TDestination>
    {
        EnsureNotRegistered<TSource, TDestination>();
        Services.Add(
            new ServiceDescriptor(typeof(IMapper<TSource, TDestination>), typeof(TMapper), lifetime)
        );
        return this;
    }

    /// <summary>
    /// Registers a prebuilt mapping instance as a singleton. This overload is intended for pure,
    /// stateless maps that have no scoped dependencies.
    /// </summary>
    public MappingBuilder Add<TSource, TDestination>(IMapper<TSource, TDestination> mapper)
        where TSource : notnull
        where TDestination : notnull
    {
        ArgumentNullException.ThrowIfNull(mapper);
        EnsureNotRegistered<TSource, TDestination>();
        Services.AddSingleton(mapper);
        return this;
    }

    private void EnsureNotRegistered<TSource, TDestination>()
        where TSource : notnull
        where TDestination : notnull
    {
        var serviceType = typeof(IMapper<TSource, TDestination>);
        var registered = false;
        for (var i = 0; i < Services.Count; i++)
        {
            // Keyed descriptors are skipped on purpose: the dispatcher resolves this pair
            // unkeyed, so a keyed registration can never satisfy it. Counting one as a duplicate
            // would leave the pair unregisterable here and unresolvable at run time.
            if (Services[i].IsKeyedService is false && Services[i].ServiceType == serviceType)
            {
                registered = true;
                break;
            }
        }

        if (registered)
        {
            throw new InvalidOperationException(
                $"A mapping from '{typeof(TSource).FullName}' to '{typeof(TDestination).FullName}' "
                    + "is already registered. Use a distinct destination type for a different semantic view."
            );
        }
    }
}
