using HostLoom.Mapping;
using HostLoom.Mapping.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

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
}
