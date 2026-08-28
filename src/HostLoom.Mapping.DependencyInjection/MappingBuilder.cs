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
    /// Registers <typeparamref name="TMapper"/> for the single type pair it already declares,
    /// inferred from the one closed <see cref="IMapper{TSource, TDestination}"/> it implements.
    /// The lifetime rules match the explicit overload: transient by default, so a map class taking
    /// scoped services through constructor injection stays safe.
    /// </summary>
    /// <remarks>
    /// The pair is read from the map class's own interface, so a registration does not restate a
    /// type triple the class has already declared. Inference is metadata-only and happens once per
    /// registration; the map dispatch path stays free of reflection. Use
    /// <see cref="Add{TSource, TDestination, TMapper}(ServiceLifetime)"/> for a map class that
    /// implements more than one pair, or to close an open generic map explicitly.
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// <typeparamref name="TMapper"/> implements no closed <see cref="IMapper{TSource, TDestination}"/>,
    /// implements more than one, or its pair is already registered.
    /// </exception>
    public MappingBuilder Add<
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicConstructors
                | DynamicallyAccessedMemberTypes.Interfaces
        )]
            TMapper
    >(ServiceLifetime lifetime = ServiceLifetime.Transient)
        where TMapper : class
    {
        // Read from a per-map-class static rather than walking the interfaces again: registering
        // the same map in a second composition root, or in a test that builds a container per
        // case, then costs a field read and no allocation.
        var serviceType =
            MappedPair<TMapper>.ServiceType
            ?? throw new InvalidOperationException(MappedPair<TMapper>.Diagnostic);
        EnsureNotRegistered(serviceType);
        Services.Add(new ServiceDescriptor(serviceType, typeof(TMapper), lifetime));
        return this;
    }

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
        EnsureNotRegistered(typeof(IMapper<TSource, TDestination>));
        Services.Add(
            new ServiceDescriptor(typeof(IMapper<TSource, TDestination>), typeof(TMapper), lifetime)
        );
        return this;
    }

    /// <summary>
    /// Registers one pair through a factory, which is how a generic map class is closed.
    /// </summary>
    /// <remarks>
    /// The container cannot register a generic map as an open generic: it requires the open
    /// service type and open implementation type to have equal arity, and a map generic in more
    /// than its source and destination — <c>Mapper&lt;TEntity, TModel, TTranslation&gt;</c>
    /// implementing <c>IMapper&lt;TEntity, TModel&gt;</c> — does not. Closing it at the call site
    /// instead keeps every type argument visible to the compiler, so this stays free of
    /// <see cref="Type.MakeGenericType"/> and the trimming and Native AOT analyzers stay clean.
    /// Each registration is still one closed descriptor, so the registered pairs remain
    /// enumerable. Call it from a generic helper to produce many pairs from one map class.
    /// </remarks>
    /// <example>
    /// <code>
    /// static void AddEntityMap&lt;TEntity, TModel, TTranslation&gt;(MappingBuilder mapping)
    ///     where TEntity : notnull where TModel : notnull =&gt;
    ///     mapping
    ///         .Add&lt;TEntity, TModel&gt;(_ =&gt; new EntityMapper&lt;TEntity, TModel, TTranslation&gt;())
    ///         .Add&lt;TModel, TEntity&gt;(_ =&gt; new ModelMapper&lt;TEntity, TModel, TTranslation&gt;());
    /// </code>
    /// </example>
    public MappingBuilder Add<TSource, TDestination>(
        Func<IServiceProvider, IMapper<TSource, TDestination>> factory,
        ServiceLifetime lifetime = ServiceLifetime.Transient
    )
        where TSource : notnull
        where TDestination : notnull
    {
        ArgumentNullException.ThrowIfNull(factory);
        EnsureNotRegistered(typeof(IMapper<TSource, TDestination>));
        Services.Add(
            new ServiceDescriptor(
                typeof(IMapper<TSource, TDestination>),
                // A factory returning null would otherwise surface as MappingNotFoundException
                // from the dispatcher, which would report the pair as unregistered when it is
                // registered and the factory is the thing at fault.
                provider =>
                    factory(provider)
                    ?? throw new InvalidOperationException(
                        $"The factory registered for the mapping from '{typeof(TSource).FullName}' "
                            + $"to '{typeof(TDestination).FullName}' returned null."
                    ),
                lifetime
            )
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
        EnsureNotRegistered(typeof(IMapper<TSource, TDestination>));
        Services.AddSingleton(mapper);
        return this;
    }

    /// <summary>
    /// The pair one map class declares, resolved once per closed <typeparamref name="TMapper"/>
    /// and then read from a static field.
    /// </summary>
    /// <remarks>
    /// A failure is stored rather than thrown. Throwing from a static constructor would reach the
    /// caller as <see cref="TypeInitializationException"/> wrapping the real diagnostic, which is
    /// a worse message than the uncached code produced — and it would be cached too, so only the
    /// first attempt would report anything useful.
    /// </remarks>
    private static class MappedPair<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)] TMapper
    >
    {
        /// <summary>The closed service type, or null when the pair could not be inferred.</summary>
        public static readonly Type? ServiceType;

        /// <summary>Why inference failed, when <see cref="ServiceType"/> is null.</summary>
        public static readonly string? Diagnostic;

        static MappedPair() => (ServiceType, Diagnostic) = Resolve(typeof(TMapper));

        /// <summary>
        /// <see cref="Type.GetInterfaces"/> already returns closed interface types, so the service
        /// type is read straight out of metadata rather than composed with reflection.
        /// </summary>
        private static (Type? ServiceType, string? Diagnostic) Resolve(
            [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)] Type mapperType
        )
        {
            Type? mapped = null;
            List<Type>? ambiguous = null;

            foreach (var candidate in mapperType.GetInterfaces())
            {
                if (
                    candidate.IsGenericType is false
                    || candidate.GetGenericTypeDefinition() != typeof(IMapper<,>)
                )
                {
                    continue;
                }

                if (mapped is null)
                {
                    mapped = candidate;
                    continue;
                }

                ambiguous ??= [mapped];
                ambiguous.Add(candidate);
            }

            if (ambiguous is not null)
            {
                var pairs = string.Join(
                    ", ",
                    ambiguous.Select(pair =>
                        $"'{pair.GenericTypeArguments[0].FullName}' to '{pair.GenericTypeArguments[1].FullName}'"
                    )
                );
                return (
                    null,
                    $"'{mapperType.FullName}' implements more than one mapping ({pairs}), so the "
                        + "pair cannot be inferred. Register it with "
                        + "Add<TSource, TDestination, TMapper> once per pair to choose each one "
                        + "explicitly."
                );
            }

            return mapped is null
                ? (
                    null,
                    $"'{mapperType.FullName}' does not implement IMapper<TSource, TDestination>, "
                        + "so there is no pair to infer. A map class implements the closed "
                        + "interface for the pair it maps."
                )
                : (mapped, null);
        }
    }

    private void EnsureNotRegistered(Type serviceType)
    {
        var registered = false;
        foreach (var service in Services)
        {
            // Keyed descriptors are skipped on purpose: the dispatcher resolves this pair
            // unkeyed, so a keyed registration can never satisfy it. Counting one as a duplicate
            // would leave the pair unregisterable here and unresolvable at run time.
            if (service.IsKeyedService is false && service.ServiceType == serviceType)
            {
                registered = true;
                break;
            }
        }

        if (registered)
        {
            throw new InvalidOperationException(
                $"A mapping from '{serviceType.GenericTypeArguments[0].FullName}' to "
                    + $"'{serviceType.GenericTypeArguments[1].FullName}' is already registered. "
                    + "Use a distinct destination type for a different semantic view."
            );
        }
    }
}
