using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Mapping.DependencyInjection;

/// <summary>Registers explicit source/destination mappings with the .NET service container.</summary>
public sealed class MappingBuilder
{
    private readonly MappedPairRegistry _registry;
    private readonly ServiceLifetime _dispatcherLifetime;

    internal MappingBuilder(
        IServiceCollection services,
        MappedPairRegistry registry,
        ServiceLifetime dispatcherLifetime
    )
    {
        Services = services;
        _registry = registry;
        _dispatcherLifetime = dispatcherLifetime;
    }

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
    /// implements more than one pair, or to close an open generic map explicitly. Only creation
    /// maps are read here: a class that also implements
    /// <see cref="IUpdateMapper{TSource, TDestination}"/> registers that contract through
    /// <see cref="AddUpdate{TMapper}(ServiceLifetime)"/>, so each call adds exactly one service.
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
            CreationPair<TMapper>.ServiceType
            ?? throw new InvalidOperationException(CreationPair<TMapper>.Diagnostic);
        EnsureNotRegistered(serviceType, MapKind.Creation);
        EnsureLifetimeMatchesDispatcher(lifetime, typeof(TMapper).Name);
        Services.Add(new ServiceDescriptor(serviceType, typeof(TMapper), lifetime));
        _registry.Record(CreationPair<TMapper>.Source!, CreationPair<TMapper>.Destination!);
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
        EnsureNotRegistered(typeof(IMapper<TSource, TDestination>), MapKind.Creation);
        EnsureLifetimeMatchesDispatcher(lifetime, typeof(TMapper).Name);
        Services.Add(
            new ServiceDescriptor(typeof(IMapper<TSource, TDestination>), typeof(TMapper), lifetime)
        );
        _registry.Record(typeof(TSource), typeof(TDestination));
        return this;
    }

    /// <summary>
    /// Registers one pair through a factory, which is how a generic map class is closed.
    /// </summary>
    /// <remarks>
    /// Reserve this for construction the container cannot perform — a map needing a value rather
    /// than a service. A factory body is opaque to <c>ValidateOnBuild</c>: whatever it resolves is
    /// unchecked until the map is first resolved, which turns a startup failure into a first-use
    /// one. When the container can build the map, including a closed generic one, prefer
    /// <see cref="Add{TSource, TDestination, TMapper}(ServiceLifetime)"/> —
    /// <c>Add&lt;TEntity, TModel, EntityMapper&lt;TEntity, TModel, TTranslation&gt;&gt;()</c> from a
    /// generic helper closes the same map and keeps startup validation over it.
    /// Each registration is still one closed descriptor, so the registered pairs remain
    /// enumerable.
    /// </remarks>
    public MappingBuilder Add<TSource, TDestination>(
        Func<IServiceProvider, IMapper<TSource, TDestination>> factory,
        ServiceLifetime lifetime = ServiceLifetime.Transient
    )
        where TSource : notnull
        where TDestination : notnull
    {
        ArgumentNullException.ThrowIfNull(factory);
        EnsureNotRegistered(typeof(IMapper<TSource, TDestination>), MapKind.Creation);
        EnsureLifetimeMatchesDispatcher(
            lifetime,
            $"the factory for '{typeof(TSource).Name}' to '{typeof(TDestination).Name}'"
        );
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
        _registry.Record(typeof(TSource), typeof(TDestination));
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
        EnsureNotRegistered(typeof(IMapper<TSource, TDestination>), MapKind.Creation);
        Services.AddSingleton(mapper);
        _registry.Record(typeof(TSource), typeof(TDestination));
        return this;
    }

    /// <summary>
    /// Registers <typeparamref name="TMapper"/> as the update map for the single pair it declares,
    /// inferred from the one closed <see cref="IUpdateMapper{TSource, TDestination}"/> it
    /// implements. Transient by default, like a creation map.
    /// </summary>
    /// <remarks>
    /// An update map is a distinct service from the creation map for the same pair, so both can be
    /// registered and each is injected under its own closed interface. Only update maps are read
    /// here; a class that also implements <see cref="IMapper{TSource, TDestination}"/> registers
    /// that contract through <see cref="Add{TMapper}(ServiceLifetime)"/>. The non-generic
    /// <see cref="IMapper"/> dispatcher never resolves an update map, so the singleton-dispatcher
    /// lifetime rule does not apply to one: choose the lifetime its own dependency graph allows.
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// <typeparamref name="TMapper"/> implements no closed
    /// <see cref="IUpdateMapper{TSource, TDestination}"/>, implements more than one, or its pair
    /// already has an update map.
    /// </exception>
    public MappingBuilder AddUpdate<
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicConstructors
                | DynamicallyAccessedMemberTypes.Interfaces
        )]
            TMapper
    >(ServiceLifetime lifetime = ServiceLifetime.Transient)
        where TMapper : class
    {
        var serviceType =
            UpdatePair<TMapper>.ServiceType
            ?? throw new InvalidOperationException(UpdatePair<TMapper>.Diagnostic);
        EnsureNotRegistered(serviceType, MapKind.Update);
        Services.Add(new ServiceDescriptor(serviceType, typeof(TMapper), lifetime));
        _registry.RecordUpdate(UpdatePair<TMapper>.Source!, UpdatePair<TMapper>.Destination!);
        return this;
    }

    /// <summary>
    /// Registers <typeparamref name="TMapper"/> as the update map for one explicit pair. Use it
    /// for a class implementing several update pairs, or to close an open generic update map.
    /// </summary>
    public MappingBuilder AddUpdate<
        TSource,
        TDestination,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TMapper
    >(ServiceLifetime lifetime = ServiceLifetime.Transient)
        where TSource : notnull
        where TDestination : class
        where TMapper : class, IUpdateMapper<TSource, TDestination>
    {
        EnsureNotRegistered(typeof(IUpdateMapper<TSource, TDestination>), MapKind.Update);
        Services.Add(
            new ServiceDescriptor(
                typeof(IUpdateMapper<TSource, TDestination>),
                typeof(TMapper),
                lifetime
            )
        );
        _registry.RecordUpdate(typeof(TSource), typeof(TDestination));
        return this;
    }

    /// <summary>
    /// Registers one update pair through a factory. As with the creation overload, reserve this
    /// for construction the container cannot perform; a factory body is opaque to
    /// <c>ValidateOnBuild</c>.
    /// </summary>
    public MappingBuilder AddUpdate<TSource, TDestination>(
        Func<IServiceProvider, IUpdateMapper<TSource, TDestination>> factory,
        ServiceLifetime lifetime = ServiceLifetime.Transient
    )
        where TSource : notnull
        where TDestination : class
    {
        ArgumentNullException.ThrowIfNull(factory);
        EnsureNotRegistered(typeof(IUpdateMapper<TSource, TDestination>), MapKind.Update);
        Services.Add(
            new ServiceDescriptor(
                typeof(IUpdateMapper<TSource, TDestination>),
                provider =>
                    factory(provider)
                    ?? throw new InvalidOperationException(
                        $"The factory registered for the update mapping from "
                            + $"'{typeof(TSource).FullName}' to '{typeof(TDestination).FullName}' "
                            + "returned null."
                    ),
                lifetime
            )
        );
        _registry.RecordUpdate(typeof(TSource), typeof(TDestination));
        return this;
    }

    /// <summary>
    /// Registers a prebuilt update map instance as a singleton, for pure, stateless maps with no
    /// scoped dependencies.
    /// </summary>
    public MappingBuilder AddUpdate<TSource, TDestination>(
        IUpdateMapper<TSource, TDestination> mapper
    )
        where TSource : notnull
        where TDestination : class
    {
        ArgumentNullException.ThrowIfNull(mapper);
        EnsureNotRegistered(typeof(IUpdateMapper<TSource, TDestination>), MapKind.Update);
        Services.AddSingleton(mapper);
        _registry.RecordUpdate(typeof(TSource), typeof(TDestination));
        return this;
    }

    private enum MapKind
    {
        Creation,
        Update,
    }

    /// <summary>One inference result: the closed pair and its parts, or why there is none.</summary>
    /// <remarks>
    /// A failure is stored rather than thrown. Throwing from a static constructor would reach the
    /// caller as <see cref="TypeInitializationException"/> wrapping the real diagnostic, which is
    /// a worse message than the uncached code produced — and it would be cached too, so only the
    /// first attempt would report anything useful.
    /// </remarks>
    private readonly record struct InferredPair(
        Type? ServiceType,
        Type? Source,
        Type? Destination,
        string? Diagnostic
    )
    {
        public static InferredPair From(Type serviceType)
        {
            // Read once here, so registration never touches GenericTypeArguments again.
            Type[] arguments = serviceType.GenericTypeArguments;
            return new InferredPair(serviceType, arguments[0], arguments[1], null);
        }

        public static InferredPair Failed(string diagnostic) => new(null, null, null, diagnostic);

        /// <summary>
        /// <see cref="Type.GetInterfaces"/> already returns closed interface types, so the service
        /// type is read straight out of metadata rather than composed with reflection.
        /// </summary>
        public static InferredPair Resolve(
            [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)] Type mapperType,
            Type openContract,
            string contractName,
            string explicitOverload
        )
        {
            Type? mapped = null;
            List<Type>? ambiguous = null;

            foreach (var candidate in mapperType.GetInterfaces())
            {
                if (
                    candidate.IsGenericType is false
                    || candidate.GetGenericTypeDefinition() != openContract
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
                return Failed(
                    $"'{mapperType.FullName}' implements more than one {contractName} ({pairs}), "
                        + $"so the pair cannot be inferred. Register it with {explicitOverload} "
                        + "once per pair to choose each one explicitly."
                );
            }

            return mapped is null
                ? Failed(
                    $"'{mapperType.FullName}' does not implement {contractName}, so there is no "
                        + "pair to infer. A map class implements the closed interface for the pair "
                        + "it maps."
                )
                : From(mapped);
        }
    }

    /// <summary>
    /// The creation pair one map class declares, resolved once per closed
    /// <typeparamref name="TMapper"/> and then read from a static field.
    /// </summary>
    private static class CreationPair<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)] TMapper
    >
    {
        private static readonly InferredPair Value = InferredPair.Resolve(
            typeof(TMapper),
            typeof(IMapper<,>),
            "IMapper<TSource, TDestination>",
            "Add<TSource, TDestination, TMapper>"
        );

        /// <summary>The closed service type, or null when the pair could not be inferred.</summary>
        public static Type? ServiceType => Value.ServiceType;

        /// <summary>The inferred source type, read once with the pair.</summary>
        /// <remarks>
        /// Held here rather than read back from <see cref="Type.GenericTypeArguments"/>, which
        /// allocates a fresh array on every access — twice per registration, on the path whose
        /// whole point is that inferring a pair costs nothing over restating it.
        /// </remarks>
        public static Type? Source => Value.Source;

        /// <summary>The inferred destination type, read once with the pair.</summary>
        public static Type? Destination => Value.Destination;

        /// <summary>Why inference failed, when <see cref="ServiceType"/> is null.</summary>
        public static string? Diagnostic => Value.Diagnostic;
    }

    /// <summary>
    /// The update pair one map class declares, cached separately from its creation pair so a
    /// class implementing both is read once per contract and each registration adds one service.
    /// </summary>
    private static class UpdatePair<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)] TMapper
    >
    {
        private static readonly InferredPair Value = InferredPair.Resolve(
            typeof(TMapper),
            typeof(IUpdateMapper<,>),
            "IUpdateMapper<TSource, TDestination>",
            "AddUpdate<TSource, TDestination, TMapper>"
        );

        public static Type? ServiceType => Value.ServiceType;

        public static Type? Source => Value.Source;

        public static Type? Destination => Value.Destination;

        public static string? Diagnostic => Value.Diagnostic;
    }

    /// <summary>
    /// Rejects a map that would outlive its own resolution when the dispatcher is a singleton.
    /// </summary>
    /// <remarks>
    /// A singleton dispatcher resolves each pair from the root provider, which never goes out of
    /// scope. Two consequences follow and neither is visible at the call site: a disposable map, or
    /// any disposable in its graph, is retained by the root container for the life of the process
    /// rather than released per unit of work; and a scoped dependency reached through a transient
    /// map is captured, which scope validation reports in Development and silently permits where it
    /// is disabled. Requiring every map to be a singleton removes both — the instance is created
    /// once and retained once, deliberately. Update maps are exempt: the dispatcher never resolves
    /// one, so nothing promotes them through the root provider.
    /// </remarks>
    private void EnsureLifetimeMatchesDispatcher(ServiceLifetime lifetime, string what)
    {
        if (_dispatcherLifetime != ServiceLifetime.Singleton)
        {
            return;
        }

        if (lifetime != ServiceLifetime.Singleton)
        {
            throw new InvalidOperationException(
                $"The mapping dispatcher is registered as a singleton, so {what} must be "
                    + $"registered with {nameof(ServiceLifetime)}.{nameof(ServiceLifetime.Singleton)} "
                    + $"rather than {nameof(ServiceLifetime)}.{lifetime}. A singleton dispatcher "
                    + "resolves from the root provider, which retains every disposable it creates "
                    + "for the life of the process and captures any scoped dependency reached "
                    + "through the map. Keep the dispatcher scoped, or make every map a singleton."
            );
        }
    }

    private void EnsureNotRegistered(Type serviceType, MapKind kind)
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
            var what = kind is MapKind.Update ? "An update mapping" : "A mapping";
            throw new InvalidOperationException(
                $"{what} from '{serviceType.GenericTypeArguments[0].FullName}' to "
                    + $"'{serviceType.GenericTypeArguments[1].FullName}' is already registered. "
                    + "Use a distinct destination type for a different semantic view."
            );
        }
    }
}
