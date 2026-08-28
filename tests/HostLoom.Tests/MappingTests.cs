using HostLoom.Mapping;
using HostLoom.Mapping.DependencyInjection;
using HostLoom.Tests.MappingInference.Maps;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

// Note for future edits: this file deliberately does NOT import
// HostLoom.Tests.MappingInference.Contracts. Adding that using would silently void
// A_registration_never_names_the_contract_types, which exists to prove it is unnecessary.

namespace HostLoom.Tests;

public sealed class MappingTests
{
    [Fact]
    public void Closed_mapper_uses_constructor_injection_and_maps_directly()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new NamePolicy("Customer: "));
        services.AddHostLoomMapping(mapping =>
            mapping.Add<Customer, CustomerDto, CustomerMapper>()
        );
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );
        using var scope = provider.CreateScope();

        var mapper = scope.ServiceProvider.GetRequiredService<IMapper<Customer, CustomerDto>>();
        var result = mapper.Map(new Customer("Ada"));

        Assert.Equal("Customer: Ada", result.DisplayName);
    }

    [Fact]
    public void Dispatcher_and_source_wrapper_preserve_compile_time_types()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new NamePolicy(string.Empty));
        services.AddHostLoomMapping(mapping =>
            mapping.Add<Customer, CustomerDto, CustomerMapper>()
        );
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var mapper = scope.ServiceProvider.GetRequiredService<IMapper>();

        var result = mapper.From(new Customer("Grace")).To<CustomerDto>();

        Assert.Equal("Grace", result.DisplayName);
    }

    [Fact]
    public void A_map_can_compose_another_closed_map()
    {
        var services = new ServiceCollection();
        services.AddHostLoomMapping(mapping =>
            mapping
                .Add<Address, AddressDto, AddressMapper>()
                .Add<CustomerWithAddress, CustomerWithAddressDto, CustomerWithAddressMapper>()
        );
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );
        using var scope = provider.CreateScope();

        var mapper = scope.ServiceProvider.GetRequiredService<
            IMapper<CustomerWithAddress, CustomerWithAddressDto>
        >();
        var result = mapper.Map(new CustomerWithAddress("Lin", new Address("Paris", "75001")));

        Assert.Equal("Lin", result.Name);
        Assert.Equal(new AddressDto("Paris", "75001"), result.Address);
    }

    [Fact]
    public void Dispatcher_uses_the_current_dependency_injection_scope()
    {
        var services = new ServiceCollection();
        services.AddScoped<MappingStamp>();
        services.AddHostLoomMapping(mapping =>
            mapping.Add<Customer, StampedCustomerDto, StampedCustomerMapper>()
        );
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );

        Guid firstStamp;
        using (var scope = provider.CreateScope())
        {
            var mapper = scope.ServiceProvider.GetRequiredService<IMapper>();
            firstStamp = mapper.From(new Customer("first")).To<StampedCustomerDto>().Stamp;
            var sameScope = mapper.From(new Customer("again")).To<StampedCustomerDto>().Stamp;
            Assert.Equal(firstStamp, sameScope);
        }

        using var secondScope = provider.CreateScope();
        var secondMapper = secondScope.ServiceProvider.GetRequiredService<IMapper>();
        var secondStamp = secondMapper.From(new Customer("second")).To<StampedCustomerDto>().Stamp;

        Assert.NotEqual(firstStamp, secondStamp);
    }

    [Fact]
    public void Map_implementations_are_transient_by_default()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new NamePolicy(string.Empty));
        services.AddHostLoomMapping(mapping =>
            mapping.Add<Customer, CustomerDto, CustomerMapper>()
        );
        using var provider = services.BuildServiceProvider();

        var first = provider.GetRequiredService<IMapper<Customer, CustomerDto>>();
        var second = provider.GetRequiredService<IMapper<Customer, CustomerDto>>();

        Assert.NotSame(first, second);
    }

    [Fact]
    public void A_prebuilt_map_is_registered_as_a_singleton()
    {
        var instance = new CustomerMapper(new NamePolicy("prebuilt: "));
        var services = new ServiceCollection();
        services.AddHostLoomMapping(mapping => mapping.Add<Customer, CustomerDto>(instance));
        using var provider = services.BuildServiceProvider();

        var first = provider.GetRequiredService<IMapper<Customer, CustomerDto>>();
        var second = provider.GetRequiredService<IMapper<Customer, CustomerDto>>();

        Assert.Same(instance, first);
        Assert.Same(first, second);
        Assert.Equal("prebuilt: Edsger", first.Map(new Customer("Edsger")).DisplayName);
    }

    [Fact]
    public void Duplicate_pairs_fail_during_registration()
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services.AddHostLoomMapping(mapping =>
                mapping
                    .Add<Customer, CustomerDto, CustomerMapper>()
                    .Add<Customer, CustomerDto, AlternateCustomerMapper>()
            )
        );

        Assert.Contains(typeof(Customer).FullName!, exception.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(CustomerDto).FullName!, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Missing_pairs_report_both_requested_types()
    {
        var services = new ServiceCollection();
        services.AddHostLoomMapping();
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var mapper = scope.ServiceProvider.GetRequiredService<IMapper>();

        var exception = Assert.Throws<MappingNotFoundException>(() =>
            mapper.Map<Customer, CustomerDto>(new Customer("missing"))
        );

        Assert.Equal(typeof(Customer), exception.SourceType);
        Assert.Equal(typeof(CustomerDto), exception.DestinationType);
    }

    [Fact]
    public void Dispatcher_rejects_a_null_source_before_invoking_the_map()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new NamePolicy(string.Empty));
        services.AddHostLoomMapping(mapping =>
            mapping.Add<Customer, CustomerDto, CustomerMapper>()
        );
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var mapper = scope.ServiceProvider.GetRequiredService<IMapper>();
        Customer source = null!;

        Assert.Throws<ArgumentNullException>(() => mapper.Map<Customer, CustomerDto>(source));
    }

    [Fact]
    public void A_keyed_registration_does_not_block_the_unkeyed_pair()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new NamePolicy(string.Empty));
        services.AddKeyedSingleton<IMapper<Customer, CustomerDto>>(
            "legacy",
            new AlternateCustomerMapper()
        );

        // A keyed descriptor can never satisfy the unkeyed resolve the dispatcher performs, so
        // counting it as a duplicate would make the pair both unregisterable and unresolvable.
        services.AddHostLoomMapping(mapping =>
            mapping.Add<Customer, CustomerDto, CustomerMapper>()
        );

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var mapper = scope.ServiceProvider.GetRequiredService<IMapper>();
        Assert.Equal("Ada", mapper.From(new Customer("Ada")).To<CustomerDto>().DisplayName);
    }

    [Fact]
    public void A_default_mapping_source_reports_that_it_carries_no_mapper()
    {
        // A struct always has an accessible parameterless constructor, so the uninitialized value
        // is reachable through a field, an array element, or a plain `default`.
        var uninitialized = default(MappingSource<Customer>);

        var exception = Assert.Throws<InvalidOperationException>(() =>
            uninitialized.To<CustomerDto>()
        );
        Assert.Contains(nameof(MapperExtensions.From), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_dispatcher_is_scoped_and_cannot_be_resolved_from_the_root_provider()
    {
        var services = new ServiceCollection();
        services.AddHostLoomMapping();
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateScopes = true }
        );

        // The generic host enables ValidateScopes in Development, so root resolution is a
        // Development-only startup failure that Production would not reproduce.
        Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<IMapper>());
    }

    [Fact]
    public void The_dispatcher_cannot_be_captured_by_a_singleton()
    {
        var services = new ServiceCollection();
        services.AddHostLoomMapping();
        services.AddSingleton<SingletonNeedingMapper>();

        // Scoped dispatcher into a singleton is a captive dependency; ValidateOnBuild is what
        // turns it into a startup failure rather than a stale mapper held for the process.
        Assert.Throws<AggregateException>(() =>
            services.BuildServiceProvider(
                new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
            )
        );
    }

    [Fact]
    public void A_map_can_be_registered_with_an_explicit_lifetime()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new NamePolicy(string.Empty));
        services.AddHostLoomMapping(mapping =>
            mapping.Add<Customer, CustomerDto, CustomerMapper>(ServiceLifetime.Singleton)
        );
        using var provider = services.BuildServiceProvider();

        Assert.Same(
            provider.GetRequiredService<IMapper<Customer, CustomerDto>>(),
            provider.GetRequiredService<IMapper<Customer, CustomerDto>>()
        );
    }

    [Fact]
    public void A_map_registers_from_the_pair_its_interface_already_declares()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new NamePolicy("Customer: "));
        services.AddHostLoomMapping(mapping => mapping.Add<CustomerMapper>());
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );
        using var scope = provider.CreateScope();

        // The inferred registration is the same closed service type the explicit triple produces.
        var mapper = scope.ServiceProvider.GetRequiredService<IMapper<Customer, CustomerDto>>();
        Assert.Equal("Customer: Ada", mapper.Map(new Customer("Ada")).DisplayName);
        Assert.Equal(
            "Customer: Ada",
            scope
                .ServiceProvider.GetRequiredService<IMapper>()
                .From(new Customer("Ada"))
                .To<CustomerDto>()
                .DisplayName
        );
    }

    [Fact]
    public void An_inferred_map_composes_another_map_and_honours_an_explicit_lifetime()
    {
        var services = new ServiceCollection();
        services.AddHostLoomMapping(mapping =>
            mapping.Add<AddressMapper>(ServiceLifetime.Singleton).Add<CustomerWithAddressMapper>()
        );
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );

        var result = provider
            .GetRequiredService<IMapper<CustomerWithAddress, CustomerWithAddressDto>>()
            .Map(new CustomerWithAddress("Lin", new Address("Paris", "75001")));

        Assert.Equal("Lin", result.Name);
        Assert.Equal(new AddressDto("Paris", "75001"), result.Address);
        Assert.Same(
            provider.GetRequiredService<IMapper<Address, AddressDto>>(),
            provider.GetRequiredService<IMapper<Address, AddressDto>>()
        );
    }

    [Fact]
    public void A_type_that_implements_no_map_cannot_have_a_pair_inferred()
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services.AddHostLoomMapping(mapping => mapping.Add<NotAMapper>())
        );

        Assert.Contains(typeof(NotAMapper).FullName!, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_map_implementing_several_pairs_names_them_and_points_at_the_explicit_overload()
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services.AddHostLoomMapping(mapping => mapping.Add<DualMapper>())
        );

        // Both pairs are named so the caller can see which registrations to write.
        Assert.Contains(typeof(Customer).FullName!, exception.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(CustomerDto).FullName!, exception.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(Address).FullName!, exception.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(AddressDto).FullName!, exception.Message, StringComparison.Ordinal);
        Assert.Contains(
            "Add<TSource, TDestination, TMapper>",
            exception.Message,
            StringComparison.Ordinal
        );
    }

    [Fact]
    public void A_registration_never_names_the_contract_types()
    {
        var services = new ServiceCollection();

        // Only the map class is named. The source and destination live in a namespace this file
        // does not import, so this compiling at all is the property under test: a registration
        // restates nothing the map class has already declared on its interface.
        services.AddHostLoomMapping(mapping => mapping.Add<RegistrationRequestMapper>());

        var descriptor = Assert.Single(
            services,
            candidate => candidate.ImplementationType == typeof(RegistrationRequestMapper)
        );
        Assert.Equal(typeof(IMapper<,>), descriptor.ServiceType!.GetGenericTypeDefinition());
        Assert.Equal(ServiceLifetime.Transient, descriptor.Lifetime);
    }

    [Fact]
    public void Duplicate_detection_spans_the_inferred_and_explicit_overloads()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new NamePolicy(string.Empty));

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services.AddHostLoomMapping(mapping =>
                mapping.Add<Customer, CustomerDto, CustomerMapper>().Add<AlternateCustomerMapper>()
            )
        );

        Assert.Contains(typeof(Customer).FullName!, exception.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(CustomerDto).FullName!, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void One_generic_map_class_registers_many_pairs_through_a_factory()
    {
        var services = new ServiceCollection();
        services.AddHostLoomMapping(mapping =>
        {
            // The FSA shape: one generic map class, two directions per call, closed at the call
            // site so the compiler still sees every type argument.
            AddEntityMap<ProductEntity, ProductModel, ProductTranslation>(
                mapping,
                entity => new ProductModel(entity.Name),
                model => new ProductEntity(model.Name)
            );
            AddEntityMap<VendorEntity, VendorModel, VendorTranslation>(
                mapping,
                entity => new VendorModel(entity.Name),
                model => new VendorEntity(model.Name)
            );
        });
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );

        // Four closed pairs from one map class and two calls, both directions resolvable.
        var forward = provider.GetRequiredService<IMapper<ProductEntity, ProductModel>>();
        var reverse = provider.GetRequiredService<IMapper<VendorModel, VendorEntity>>();
        Assert.Equal("notebook", forward.Map(new ProductEntity("notebook")).Name);
        Assert.Equal("acme", reverse.Map(new VendorModel("acme")).Name);

        // The third type argument really closed the map; it is the parameter that makes an
        // open-generic registration impossible.
        Assert.Equal(
            typeof(ProductTranslation),
            (
                (GenericEntityMapper<ProductEntity, ProductModel, ProductTranslation>)forward
            ).TranslationType
        );
    }

    [Fact]
    public void An_open_generic_map_class_cannot_be_registered_as_an_open_generic()
    {
        IServiceCollection services = new ServiceCollection();
        services.Add(
            new ServiceDescriptor(
                typeof(IMapper<,>),
                typeof(GenericEntityMapper<,,>),
                ServiceLifetime.Transient
            )
        );

        // This is why the factory overload exists rather than an open-generic registration: the
        // container requires equal arity, and a map generic in more than its pair never has it.
        // If a future container relaxes this, that is worth knowing here first.
        var exception = Assert.Throws<ArgumentException>(() => services.BuildServiceProvider());
        Assert.Contains("arity", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void A_factory_map_receives_the_provider_and_honours_its_lifetime()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new NamePolicy("factory: "));
        services.AddHostLoomMapping(mapping =>
            mapping.Add<Customer, CustomerDto>(
                provider => new CustomerMapper(provider.GetRequiredService<NamePolicy>()),
                ServiceLifetime.Singleton
            )
        );
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );

        var mapper = provider.GetRequiredService<IMapper<Customer, CustomerDto>>();
        Assert.Equal("factory: Ada", mapper.Map(new Customer("Ada")).DisplayName);
        Assert.Same(mapper, provider.GetRequiredService<IMapper<Customer, CustomerDto>>());
    }

    [Fact]
    public void A_factory_returning_null_is_reported_as_the_factory_rather_than_a_missing_pair()
    {
        var services = new ServiceCollection();
        services.AddHostLoomMapping(mapping => mapping.Add<Customer, CustomerDto>(_ => null!));
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        // MappingNotFoundException here would blame the registration for something the factory did.
        var exception = Assert.Throws<InvalidOperationException>(() =>
            scope
                .ServiceProvider.GetRequiredService<IMapper>()
                .Map<Customer, CustomerDto>(new Customer("Ada"))
        );

        Assert.IsNotType<MappingNotFoundException>(exception);
        Assert.Contains("returned null", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Duplicate_detection_spans_the_factory_overload()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new NamePolicy(string.Empty));

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services.AddHostLoomMapping(mapping =>
                mapping
                    .Add<AlternateCustomerMapper>()
                    .Add<Customer, CustomerDto>(_ => new AlternateCustomerMapper())
            )
        );

        Assert.Contains(typeof(Customer).FullName!, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_null_factory_is_rejected()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentNullException>(() =>
            services.AddHostLoomMapping(mapping =>
                mapping.Add<Customer, CustomerDto>(
                    (Func<IServiceProvider, IMapper<Customer, CustomerDto>>)null!
                )
            )
        );
    }

    private static void AddEntityMap<TEntity, TModel, TTranslation>(
        MappingBuilder mapping,
        Func<TEntity, TModel> forward,
        Func<TModel, TEntity> reverse
    )
        where TEntity : notnull
        where TModel : notnull =>
        mapping
            .Add<TEntity, TModel>(_ => new GenericEntityMapper<TEntity, TModel, TTranslation>(
                forward
            ))
            .Add<TModel, TEntity>(_ => new GenericEntityMapper<TModel, TEntity, TTranslation>(
                reverse
            ));

    [Fact]
    public void The_builder_overload_registers_the_dispatcher_once_across_repeated_calls()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new NamePolicy(string.Empty));

        services.AddHostLoomMapping().Add<Customer, CustomerDto, CustomerMapper>();
        services.AddHostLoomMapping().Add<Address, AddressDto, AddressMapper>();

        Assert.Single(services, descriptor => descriptor.ServiceType == typeof(IMapper));
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );
        using var scope = provider.CreateScope();
        var mapper = scope.ServiceProvider.GetRequiredService<IMapper>();
        Assert.Equal("Ada", mapper.From(new Customer("Ada")).To<CustomerDto>().DisplayName);
        Assert.Equal("Paris", mapper.From(new Address("Paris", "75001")).To<AddressDto>().City);
    }

    [Fact]
    public void Null_arguments_are_rejected()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentNullException>(() =>
            services.AddHostLoomMapping((Action<MappingBuilder>)null!)
        );
        Assert.Throws<ArgumentNullException>(() =>
            services.AddHostLoomMapping(mapping =>
                mapping.Add<Customer, CustomerDto>((IMapper<Customer, CustomerDto>)null!)
            )
        );
        Assert.Throws<ArgumentNullException>(() =>
            ((IServiceCollection)null!).AddHostLoomMapping(_ => { })
        );
    }

    [Fact]
    public void The_missing_pair_message_names_both_types_and_how_to_register_one()
    {
        var exception = new MappingNotFoundException(typeof(Customer), typeof(CustomerDto));

        Assert.Contains(typeof(Customer).FullName!, exception.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(CustomerDto).FullName!, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_sequence_maps_every_element_in_order()
    {
        var mapper = new AlternateCustomerMapper();
        Customer[] indexable = [new("ada"), new("grace"), new("lin")];

        // Indexable and non-indexable sources take different paths inside MapSequence; the
        // Where keeps the second one from reporting a count, which is the unsized case.
        var fromIndexable = mapper.MapMany(indexable);
        var fromEnumerable = mapper.MapMany(indexable.Where(_ => true));

        Assert.Equal(["ADA", "GRACE", "LIN"], fromIndexable.Select(dto => dto.DisplayName));
        Assert.Equal(["ADA", "GRACE", "LIN"], fromEnumerable.Select(dto => dto.DisplayName));
        Assert.Empty(mapper.MapMany(Array.Empty<Customer>()));
    }

    [Fact]
    public void A_null_sequence_is_rejected_by_MapMany_and_emptied_by_MapManyOrEmpty()
    {
        var mapper = new AlternateCustomerMapper();

        Assert.Throws<ArgumentNullException>(() => mapper.MapMany(null!));

        // AutoMapper's AllowNullCollections=false default, opted into by name rather than by
        // configuration, so every migrated call site that relies on it stays greppable.
        var coerced = mapper.MapManyOrEmpty(null);
        Assert.NotNull(coerced);
        Assert.Empty(coerced);
        Assert.Equal(
            ["ADA"],
            mapper.MapManyOrEmpty([new Customer("ada")]).Select(d => d.DisplayName)
        );
    }

    [Fact]
    public void A_deferred_sequence_validates_eagerly_but_maps_lazily()
    {
        var mapper = new AlternateCustomerMapper();
        var pulled = 0;

        IEnumerable<Customer> Source()
        {
            foreach (var name in (string[])["ada", "grace", "lin"])
            {
                pulled++;
                yield return new Customer(name);
            }
        }

        // Never enumerated, so this only throws if the guard runs at call time. An iterator body
        // without the local-function split would defer the guard and pass this silently.
        Assert.Throws<ArgumentNullException>(() => mapper.MapManyDeferred(null!));

        var deferred = mapper.MapManyDeferred(Source());
        Assert.Equal(0, pulled);
        Assert.Equal("ADA", deferred.First().DisplayName);
        Assert.Equal(1, pulled);
    }

    [Fact]
    public void A_null_value_is_rejected_by_Map_and_returned_as_null_by_MapOrNull()
    {
        var mapper = new AlternateCustomerMapper();

        Assert.Null(mapper.MapOrNull(null));
        Assert.Equal("ADA", mapper.MapOrNull(new Customer("ada"))!.DisplayName);
    }

    [Fact]
    public void The_sequence_and_null_extensions_reject_a_null_mapper()
    {
        IMapper<Customer, CustomerDto> mapper = null!;

        Assert.Throws<ArgumentNullException>(() => mapper.MapMany([]));
        Assert.Throws<ArgumentNullException>(() => mapper.MapManyOrEmpty(null));
        Assert.Throws<ArgumentNullException>(() => mapper.MapManyDeferred([]));
        Assert.Throws<ArgumentNullException>(() => mapper.MapOrNull(new Customer("ada")));
    }

    [Fact]
    public void The_registered_pairs_can_be_asserted_at_startup()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new NamePolicy(string.Empty));
        services.AddHostLoomMapping(mapping => mapping.Add<CustomerMapper>().Add<AddressMapper>());

        MappedPairRegistry registry = services.GetMappedPairs();

        Assert.True(registry.Contains(typeof(Customer), typeof(CustomerDto)));
        Assert.True(registry.Contains(typeof(Address), typeof(AddressDto)));
        Assert.False(registry.Contains(typeof(Address), typeof(CustomerDto)));
        Assert.Equal(2, registry.Pairs.Count);
    }

    [Fact]
    public void The_pair_registry_spans_every_registration_overload_and_repeated_calls()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new NamePolicy(string.Empty));

        services.AddHostLoomMapping().Add<CustomerMapper>();
        services
            .AddHostLoomMapping()
            .Add<Address, AddressDto, AddressMapper>()
            .Add<CustomerWithAddress, CustomerWithAddressDto>(_ => new CustomerWithAddressMapper(
                new AddressMapper()
            ))
            .Add<Customer, StampedCustomerDto>(new StampedCustomerMapper(new MappingStamp()));

        // One registry across repeated AddHostLoomMapping calls, as the dispatcher is.
        Assert.Equal(4, services.GetMappedPairs().Pairs.Count);
    }

    [Fact]
    public void A_missing_pair_names_what_the_source_is_registered_to_map_to()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new NamePolicy(string.Empty));
        services.AddHostLoomMapping(mapping => mapping.Add<CustomerMapper>());
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var mapper = scope.ServiceProvider.GetRequiredService<IMapper>();

        var exception = Assert.Throws<MappingNotFoundException>(() =>
            mapper.Map<Customer, StampedCustomerDto>(new Customer("Ada"))
        );

        // The near miss is the diagnosis: Customer maps somewhere, just not there.
        Assert.Equal([typeof(CustomerDto)], exception.RegisteredDestinations);
        Assert.Contains(typeof(CustomerDto).FullName!, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_dispatcher_can_be_registered_as_a_singleton()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new NamePolicy(string.Empty));
        services.AddHostLoomMapping(
            mapping => mapping.Add<CustomerMapper>(ServiceLifetime.Singleton),
            ServiceLifetime.Singleton
        );
        services.AddSingleton<SingletonNeedingMapper>();

        // The captive-dependency failure the scoped default produces is gone, so this composes and
        // resolves from the root provider even with scope validation on.
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );

        var consumer = provider.GetRequiredService<SingletonNeedingMapper>();
        Assert.Equal(
            "Ada",
            consumer.Mapper.From(new Customer("Ada")).To<CustomerDto>().DisplayName
        );
    }

    [Fact]
    public void A_singleton_dispatcher_rejects_a_map_it_would_resolve_from_the_root()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new NamePolicy(string.Empty));

        // Transient is the default, and under a singleton dispatcher it is exactly the unsound
        // combination: the root provider would create a map per call and — when anything in the
        // graph is disposable — hold every one of them until the process ends.
        var exception = Assert.Throws<InvalidOperationException>(() =>
            services.AddHostLoomMapping(
                mapping => mapping.Add<CustomerMapper>(),
                ServiceLifetime.Singleton
            )
        );

        Assert.Contains("Singleton", exception.Message, StringComparison.Ordinal);
        Assert.Contains("root provider", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_scoped_dispatcher_releases_a_disposable_map_with_its_scope()
    {
        var services = new ServiceCollection();
        services.AddHostLoomMapping(mapping => mapping.Add<DisposableCustomerMapper>());
        using var provider = services.BuildServiceProvider();
        DisposableCustomerMapper.Reset();

        using (var scope = provider.CreateScope())
        {
            var mapper = scope.ServiceProvider.GetRequiredService<IMapper>();
            for (var index = 0; index < 50; index++)
            {
                mapper.Map<Customer, CustomerDto>(new Customer("Ada"));
            }

            // Every dispatch constructs a transient map, and the container holds each disposable
            // one until the scope ends. That retention is the reason the dispatcher is scoped.
            Assert.Equal(50, DisposableCustomerMapper.Created);
            Assert.Equal(0, DisposableCustomerMapper.Disposed);
        }

        Assert.Equal(50, DisposableCustomerMapper.Disposed);
    }

    [Fact]
    public void The_explicit_overload_closes_a_constructed_generic_map()
    {
        var services = new ServiceCollection();
        services.AddHostLoomMapping(mapping =>
            AddEntityMap<Customer, CustomerDto, NamePolicy>(mapping)
        );

        // The container constructs it, so ValidateOnBuild covers its dependencies — which a
        // factory registration cannot offer, because its body is opaque until first resolve.
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );

        Assert.NotNull(provider.GetRequiredService<IMapper<Customer, CustomerDto>>());
    }

    private static void AddEntityMap<TEntity, TModel, TTranslation>(MappingBuilder mapping)
        where TEntity : notnull
        where TModel : notnull =>
        mapping.Add<TEntity, TModel, GenericMapper<TEntity, TModel, TTranslation>>();

    public sealed class SingletonNeedingMapper(IMapper mapper)
    {
        public IMapper Mapper { get; } = mapper;
    }

    public sealed record Customer(string Name);

    public sealed record CustomerDto(string DisplayName);

    public sealed record Address(string City, string PostalCode);

    public sealed record AddressDto(string City, string PostalCode);

    public sealed record CustomerWithAddress(string Name, Address Address);

    public sealed record CustomerWithAddressDto(string Name, AddressDto Address);

    public sealed record StampedCustomerDto(string Name, Guid Stamp);

    public sealed record NamePolicy(string Prefix);

    public sealed class MappingStamp
    {
        public Guid Value { get; } = Guid.NewGuid();
    }

    public sealed class CustomerMapper(NamePolicy policy) : IMapper<Customer, CustomerDto>
    {
        public CustomerDto Map(Customer source) => new(policy.Prefix + source.Name);
    }

    public sealed class AlternateCustomerMapper : IMapper<Customer, CustomerDto>
    {
        public CustomerDto Map(Customer source) => new(source.Name.ToUpperInvariant());
    }

    public sealed class AddressMapper : IMapper<Address, AddressDto>
    {
        public AddressDto Map(Address source) => new(source.City, source.PostalCode);
    }

    public sealed class CustomerWithAddressMapper(IMapper<Address, AddressDto> addressMapper)
        : IMapper<CustomerWithAddress, CustomerWithAddressDto>
    {
        public CustomerWithAddressDto Map(CustomerWithAddress source) =>
            new(source.Name, addressMapper.Map(source.Address));
    }

    public sealed class StampedCustomerMapper(MappingStamp stamp)
        : IMapper<Customer, StampedCustomerDto>
    {
        public StampedCustomerDto Map(Customer source) => new(source.Name, stamp.Value);
    }

    /// <summary>A plain class, so inference has no pair to read.</summary>
    public sealed class NotAMapper;

    /// <summary>Counts construction and disposal, so retention is observable rather than argued.</summary>
    public sealed class DisposableCustomerMapper : IMapper<Customer, CustomerDto>, IDisposable
    {
        private static int _created;
        private static int _disposed;

        public DisposableCustomerMapper() => Interlocked.Increment(ref _created);

        public static int Created => Volatile.Read(ref _created);

        public static int Disposed => Volatile.Read(ref _disposed);

        public static void Reset()
        {
            Volatile.Write(ref _created, 0);
            Volatile.Write(ref _disposed, 0);
        }

        public CustomerDto Map(Customer source) => new(source.Name);

        public void Dispose() => Interlocked.Increment(ref _disposed);
    }

    /// <summary>Generic in a third parameter, closed by the caller rather than by the container.</summary>
    public sealed class GenericMapper<TEntity, TModel, TTranslation> : IMapper<TEntity, TModel>
        where TEntity : notnull
        where TModel : notnull
    {
        public TModel Map(TEntity source) =>
            throw new NotSupportedException("registration-only fixture");
    }

    public sealed record ProductEntity(string Name);

    public sealed record ProductModel(string Name);

    public sealed record ProductTranslation(string Text);

    public sealed record VendorEntity(string Name);

    public sealed record VendorModel(string Name);

    public sealed record VendorTranslation(string Text);

    /// <summary>
    /// Generic in three parameters while implementing a two-parameter interface — the shape that
    /// cannot be registered as an open generic and so must be closed at the call site.
    /// </summary>
    public sealed class GenericEntityMapper<TEntity, TModel, TTranslation>(
        Func<TEntity, TModel> convert
    ) : IMapper<TEntity, TModel>
        where TEntity : notnull
        where TModel : notnull
    {
        public Type TranslationType => typeof(TTranslation);

        public TModel Map(TEntity source) => convert(source);
    }

    /// <summary>Two pairs on one class, which inference must refuse rather than pick between.</summary>
    public sealed class DualMapper : IMapper<Customer, CustomerDto>, IMapper<Address, AddressDto>
    {
        public CustomerDto Map(Customer source) => new(source.Name);

        public AddressDto Map(Address source) => new(source.City, source.PostalCode);
    }
}
