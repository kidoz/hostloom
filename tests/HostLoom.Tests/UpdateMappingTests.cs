using HostLoom.Mapping;
using HostLoom.Mapping.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Tests;

public sealed class UpdateMappingTests
{
    // -- Contract: the supplied instance is updated, never replaced -----------------------------

    [Fact]
    public void An_update_writes_into_the_supplied_instance_and_keeps_untouched_members()
    {
        var entity = NewVendor();
        var identifier = entity.Id;
        var createdAt = entity.CreatedAt;
        var update = new VendorUpdate("Acme Trading", "eu", new AddressUpdate("Lyon"), ["books"]);

        new VendorUpdateMapper(new AddressUpdateMapper()).MapInto(update, entity);

        Assert.Equal("Acme Trading", entity.Name);
        Assert.Equal("eu", entity.Region);
        // Identity and audit members are not part of the update and keep their values.
        Assert.Equal(identifier, entity.Id);
        Assert.Equal(createdAt, entity.CreatedAt);
        // A settable member the map deliberately ignores is left alone too.
        Assert.Equal("preferred", entity.Notes);
    }

    [Fact]
    public void A_nested_update_map_updates_the_nested_instance_in_place()
    {
        var entity = NewVendor();
        var address = entity.Address;
        var update = new VendorUpdate("acme", "eu", new AddressUpdate("Lyon"), []);

        new VendorUpdateMapper(new AddressUpdateMapper()).MapInto(update, entity);

        Assert.Same(address, entity.Address);
        Assert.Equal("Lyon", entity.Address.City);
        Assert.Equal("69001", entity.Address.PostalCode);
    }

    [Fact]
    public void Collection_behavior_is_chosen_in_the_map_body()
    {
        var entity = NewVendor();
        var categories = entity.Categories;
        var update = new VendorUpdate("acme", "eu", new AddressUpdate("Lyon"), ["books", "music"]);

        new VendorUpdateMapper(new AddressUpdateMapper()).MapInto(update, entity);

        // This map replaces the contents; the list instance the entity owns is retained.
        Assert.Same(categories, entity.Categories);
        Assert.Equal(["books", "music"], entity.Categories);
    }

    [Fact]
    public void An_update_map_rejects_null_arguments()
    {
        var mapper = new VendorUpdateMapper(new AddressUpdateMapper());
        var update = new VendorUpdate("acme", "eu", new AddressUpdate("Lyon"), []);

        Assert.Throws<ArgumentNullException>(() => mapper.MapInto(null!, NewVendor()));
        Assert.Throws<ArgumentNullException>(() => mapper.MapInto(update, null!));
    }

    // -- Registration --------------------------------------------------------------------------

    [Fact]
    public void An_update_map_registers_from_the_pair_its_interface_declares()
    {
        var services = new ServiceCollection();
        services.AddHostLoomMapping(mapping =>
            mapping.AddUpdate<AddressUpdateMapper>().AddUpdate<VendorUpdateMapper>()
        );
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );
        using var scope = provider.CreateScope();
        var entity = NewVendor();

        var mapper = scope.ServiceProvider.GetRequiredService<
            IUpdateMapper<VendorUpdate, VendorEntity>
        >();
        mapper.MapInto(
            new VendorUpdate("Acme Trading", "eu", new AddressUpdate("Lyon"), []),
            entity
        );

        Assert.Equal("Acme Trading", entity.Name);
        Assert.Equal("Lyon", entity.Address.City);
    }

    [Fact]
    public void A_creation_map_and_an_update_map_for_the_same_pair_coexist()
    {
        var services = new ServiceCollection();
        services.AddHostLoomMapping(mapping =>
            mapping
                .Add<AddressMapper>()
                .Add<VendorMapper>()
                .AddUpdate<AddressUpdateMapper>()
                .AddUpdate<VendorUpdateMapper>()
        );
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );
        using var scope = provider.CreateScope();
        var update = new VendorUpdate("acme", "eu", new AddressUpdate("Lyon"), ["books"]);

        var created = scope
            .ServiceProvider.GetRequiredService<IMapper<VendorUpdate, VendorEntity>>()
            .Map(update);
        var existing = NewVendor();
        scope
            .ServiceProvider.GetRequiredService<IUpdateMapper<VendorUpdate, VendorEntity>>()
            .MapInto(update, existing);

        // Two services, two identities: the creation map returns a new instance and the update
        // map writes into the one it was handed.
        Assert.NotSame(existing, created);
        Assert.Equal(created.Name, existing.Name);
        Assert.NotEqual(created.Id, existing.Id);

        // The dispatcher still reaches the creation map and nothing else.
        var dispatched = scope
            .ServiceProvider.GetRequiredService<IMapper>()
            .From(update)
            .To<VendorEntity>();
        Assert.Equal("acme", dispatched.Name);

        MappedPairRegistry registry = services.GetMappedPairs();
        Assert.True(registry.Contains(typeof(VendorUpdate), typeof(VendorEntity)));
        Assert.True(registry.ContainsUpdate(typeof(VendorUpdate), typeof(VendorEntity)));
        Assert.Equal(2, registry.Pairs.Count);
        Assert.Equal(2, registry.UpdatePairs.Count);
    }

    [Fact]
    public void Inference_reads_only_the_contract_each_overload_registers()
    {
        var services = new ServiceCollection();
        services.AddHostLoomMapping(mapping => mapping.Add<DualKindMapper>());

        // Add<TMapper> read the creation pair alone; the update contract is not registered until
        // AddUpdate<TMapper> is called, and then each call has added exactly one service.
        Assert.Null(
            services.BuildServiceProvider().GetService<IUpdateMapper<AddressUpdate, Address>>()
        );

        services.AddHostLoomMapping(mapping => mapping.AddUpdate<DualKindMapper>());
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true }
        );

        Assert.NotNull(provider.GetService<IMapper<AddressUpdate, Address>>());
        Assert.NotNull(provider.GetService<IUpdateMapper<AddressUpdate, Address>>());
        MappedPairRegistry registry = services.GetMappedPairs();
        Assert.Single(registry.Pairs);
        Assert.Single(registry.UpdatePairs);
    }

    [Fact]
    public void An_update_pair_is_not_offered_as_a_near_miss_for_a_missing_creation_pair()
    {
        var services = new ServiceCollection();
        services.AddHostLoomMapping(mapping => mapping.AddUpdate<AddressUpdateMapper>());
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var mapper = scope.ServiceProvider.GetRequiredService<IMapper>();

        var exception = Assert.Throws<MappingNotFoundException>(() =>
            mapper.Map<AddressUpdate, Address>(new AddressUpdate("Lyon"))
        );

        // The dispatcher cannot resolve an update map, so suggesting it would send the reader
        // to a registration that does not answer the request.
        Assert.Empty(exception.RegisteredDestinations);
        Assert.Empty(services.GetMappedPairs().DestinationsFor(typeof(AddressUpdate)));
    }

    [Fact]
    public void Duplicate_update_pairs_fail_during_registration()
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services.AddHostLoomMapping(mapping =>
                mapping
                    .AddUpdate<AddressUpdateMapper>()
                    .AddUpdate<AddressUpdate, Address, AlternateAddressUpdateMapper>()
            )
        );

        Assert.StartsWith("An update mapping from", exception.Message, StringComparison.Ordinal);
        Assert.Contains(
            typeof(AddressUpdate).FullName!,
            exception.Message,
            StringComparison.Ordinal
        );
        Assert.Contains(typeof(Address).FullName!, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_type_that_implements_no_update_map_cannot_have_a_pair_inferred()
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services.AddHostLoomMapping(mapping => mapping.AddUpdate<AddressMapper>())
        );

        // A creation map is not an update map; the message names the contract that is missing.
        Assert.Contains(
            "does not implement IUpdateMapper<TSource, TDestination>",
            exception.Message,
            StringComparison.Ordinal
        );
    }

    [Fact]
    public void An_update_map_implementing_several_pairs_points_at_the_explicit_overload()
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services.AddHostLoomMapping(mapping => mapping.AddUpdate<DualPairUpdateMapper>())
        );

        Assert.Contains(
            "AddUpdate<TSource, TDestination, TMapper>",
            exception.Message,
            StringComparison.Ordinal
        );
        Assert.Contains(
            typeof(VendorEntity).FullName!,
            exception.Message,
            StringComparison.Ordinal
        );
        Assert.Contains(typeof(Address).FullName!, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_update_registry_spans_every_registration_overload()
    {
        var services = new ServiceCollection();

        services
            .AddHostLoomMapping()
            .AddUpdate<AddressUpdateMapper>()
            .AddUpdate<VendorUpdate, VendorEntity, VendorUpdateMapper>()
            .AddUpdate<VendorUpdate, Address>(_ => new DualPairUpdateMapper())
            .AddUpdate<AddressUpdate, VendorEntity>(new DualPairUpdateMapper());

        Assert.Equal(4, services.GetMappedPairs().UpdatePairs.Count);
        Assert.Empty(services.GetMappedPairs().Pairs);
    }

    [Fact]
    public void An_update_factory_returning_null_is_reported_as_the_factory()
    {
        var services = new ServiceCollection();
        services.AddHostLoomMapping(mapping =>
            mapping.AddUpdate<AddressUpdate, Address>(_ => null!)
        );
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            provider.GetRequiredService<IUpdateMapper<AddressUpdate, Address>>()
        );

        Assert.Contains("update mapping", exception.Message, StringComparison.Ordinal);
        Assert.Contains("returned null", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Update_maps_are_transient_by_default_and_honour_an_explicit_lifetime()
    {
        var services = new ServiceCollection();
        services.AddHostLoomMapping(mapping =>
            mapping
                .AddUpdate<AddressUpdateMapper>()
                .AddUpdate<VendorUpdateMapper>(ServiceLifetime.Singleton)
        );
        using var provider = services.BuildServiceProvider();

        Assert.NotSame(
            provider.GetRequiredService<IUpdateMapper<AddressUpdate, Address>>(),
            provider.GetRequiredService<IUpdateMapper<AddressUpdate, Address>>()
        );
        Assert.Same(
            provider.GetRequiredService<IUpdateMapper<VendorUpdate, VendorEntity>>(),
            provider.GetRequiredService<IUpdateMapper<VendorUpdate, VendorEntity>>()
        );
    }

    [Fact]
    public void A_singleton_dispatcher_does_not_constrain_update_maps()
    {
        var services = new ServiceCollection();

        // The dispatcher never resolves an update map, so the rule that every map it can reach
        // must be a singleton has nothing to protect here.
        services.AddHostLoomMapping(
            mapping => mapping.AddUpdate<AddressUpdateMapper>(),
            ServiceLifetime.Singleton
        );
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );
        using var scope = provider.CreateScope();

        Assert.NotNull(scope.ServiceProvider.GetService<IUpdateMapper<AddressUpdate, Address>>());
    }

    [Fact]
    public void Null_update_registrations_are_rejected()
    {
        var mapping = new ServiceCollection().AddHostLoomMapping();

        Assert.Throws<ArgumentNullException>(() =>
            mapping.AddUpdate<AddressUpdate, Address>(
                (Func<IServiceProvider, IUpdateMapper<AddressUpdate, Address>>)null!
            )
        );
        Assert.Throws<ArgumentNullException>(() =>
            mapping.AddUpdate<AddressUpdate, Address>((IUpdateMapper<AddressUpdate, Address>)null!)
        );
    }

    // -- Fixtures ------------------------------------------------------------------------------

    private static VendorEntity NewVendor() =>
        new()
        {
            Id = Guid.NewGuid(),
            CreatedAt = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
            Name = "acme",
            Region = "us",
            Notes = "preferred",
            Address = new Address { City = "Paris", PostalCode = "69001" },
        };

    public sealed class VendorEntity
    {
        public Guid Id { get; init; }

        public DateTimeOffset CreatedAt { get; init; }

        public required string Name { get; set; }

        public required string Region { get; set; }

        public string? Notes { get; set; }

        public required Address Address { get; set; }

        public List<string> Categories { get; } = [];
    }

    public sealed class Address
    {
        public required string City { get; set; }

        public required string PostalCode { get; set; }
    }

    public sealed record AddressUpdate(string City);

    public sealed record VendorUpdate(
        string Name,
        string Region,
        AddressUpdate Address,
        IReadOnlyList<string> Categories
    );

    /// <summary>Updates the city only; the postal code is outside the update contract.</summary>
    public sealed class AddressUpdateMapper : IUpdateMapper<AddressUpdate, Address>
    {
        public void MapInto(AddressUpdate source, Address destination)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(destination);
            destination.City = source.City;
        }
    }

    public sealed class AlternateAddressUpdateMapper : IUpdateMapper<AddressUpdate, Address>
    {
        public void MapInto(AddressUpdate source, Address destination) =>
            destination.City = source.City.ToUpperInvariant();
    }

    /// <summary>
    /// Leaves <see cref="VendorEntity.Id"/>, <see cref="VendorEntity.CreatedAt"/>, and
    /// <see cref="VendorEntity.Notes"/> alone, composes the address update, and replaces the
    /// category list contents in place.
    /// </summary>
    public sealed class VendorUpdateMapper(IUpdateMapper<AddressUpdate, Address> address)
        : IUpdateMapper<VendorUpdate, VendorEntity>
    {
        public void MapInto(VendorUpdate source, VendorEntity destination)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(destination);
            destination.Name = source.Name;
            destination.Region = source.Region;
            address.MapInto(source.Address, destination.Address);
            destination.Categories.Clear();
            destination.Categories.AddRange(source.Categories);
        }
    }

    public sealed class AddressMapper : IMapper<AddressUpdate, Address>
    {
        public Address Map(AddressUpdate source) =>
            new() { City = source.City, PostalCode = string.Empty };
    }

    public sealed class VendorMapper(IMapper<AddressUpdate, Address> address)
        : IMapper<VendorUpdate, VendorEntity>
    {
        public VendorEntity Map(VendorUpdate source)
        {
            var entity = new VendorEntity
            {
                Id = Guid.NewGuid(),
                CreatedAt = DateTimeOffset.UnixEpoch,
                Name = source.Name,
                Region = source.Region,
                Address = address.Map(source.Address),
            };
            entity.Categories.AddRange(source.Categories);
            return entity;
        }
    }

    /// <summary>Creates and updates the same pair; each overload registers one of the two.</summary>
    public sealed class DualKindMapper
        : IMapper<AddressUpdate, Address>,
            IUpdateMapper<AddressUpdate, Address>
    {
        public Address Map(AddressUpdate source) =>
            new() { City = source.City, PostalCode = string.Empty };

        public void MapInto(AddressUpdate source, Address destination) =>
            destination.City = source.City;
    }

    public sealed class DualPairUpdateMapper
        : IUpdateMapper<VendorUpdate, Address>,
            IUpdateMapper<AddressUpdate, VendorEntity>
    {
        public void MapInto(VendorUpdate source, Address destination) =>
            destination.City = source.Address.City;

        public void MapInto(AddressUpdate source, VendorEntity destination) =>
            destination.Address.City = source.City;
    }
}
