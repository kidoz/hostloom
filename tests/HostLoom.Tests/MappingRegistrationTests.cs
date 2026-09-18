using HostLoom.Mapping;
using HostLoom.Mapping.DependencyInjection;
using HostLoom.Mapping.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Registration and resolution beyond the happy path: what container validation catches and
/// what it cannot, keyed descriptors, multi-pair classes, the registry's shape, concurrent
/// dispatch, and the test builder's edges.
/// </summary>
public sealed class MappingRegistrationTests
{
    // -- What startup validation catches ----------------------------------------------------------

    [Fact]
    public void A_missing_inner_map_fails_at_build_when_the_container_constructs_the_outer_one()
    {
        // The README's argument for ValidateOnBuild: a map that composes another is checked at
        // host build like any registration, so the missing pair never reaches the first message.
        var services = new ServiceCollection();
        services.AddHostLoomMapping(mapping => mapping.Add<OrderMapper>());

        var exception = Assert.Throws<AggregateException>(() =>
            services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true })
        );

        Assert.Contains(
            typeof(IMapper<Line, LineDto>).Name,
            exception.ToString(),
            StringComparison.Ordinal
        );
    }

    [Fact]
    public void A_factory_hides_a_missing_inner_map_until_first_resolution()
    {
        // The documented asymmetry: a factory body is opaque to ValidateOnBuild, so the same
        // omission becomes a first-use failure. Pinned so the README's advice stays true.
        var services = new ServiceCollection();
        services.AddHostLoomMapping(mapping =>
            mapping.Add<Order, OrderDto>(provider => new OrderMapper(
                provider.GetRequiredService<IMapper<Line, LineDto>>()
            ))
        );

        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true }
        );

        Assert.Throws<InvalidOperationException>(() =>
            provider.GetRequiredService<IMapper<Order, OrderDto>>()
        );
    }

    [Fact]
    public void A_keyed_only_registration_is_invisible_to_the_dispatcher_and_the_registry()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new Trim(false));
        services.AddKeyedTransient<IMapper<Line, LineDto>, LineMapper>("audit");
        services.AddHostLoomMapping();
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.NotNull(
            scope.ServiceProvider.GetRequiredKeyedService<IMapper<Line, LineDto>>("audit")
        );
        Assert.Throws<MappingNotFoundException>(() =>
            scope.ServiceProvider.GetRequiredService<IMapper>().Map<Line, LineDto>(new Line("a", 1))
        );
        Assert.False(services.GetMappedPairs().Contains(typeof(Line), typeof(LineDto)));
    }

    // -- Multi-pair classes -----------------------------------------------------------------------

    [Fact]
    public void A_class_with_two_pairs_registers_each_through_the_explicit_overload()
    {
        var services = new ServiceCollection();
        services.AddHostLoomMapping(mapping =>
            mapping
                .Add<Line, LineDto, TwoPairMapper>()
                .Add<Order, OrderDto, TwoPairMapper>(ServiceLifetime.Singleton)
        );
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );
        using var scope = provider.CreateScope();
        var mapper = scope.ServiceProvider.GetRequiredService<IMapper>();

        Assert.Equal("a", mapper.From(new Line("a", 1)).To<LineDto>().Sku);
        Assert.Equal("o1", mapper.From(new Order("o1", [])).To<OrderDto>().Reference);

        // Two descriptors, two lifetimes, one implementation type: each pair is its own service.
        Assert.NotSame(
            scope.ServiceProvider.GetRequiredService<IMapper<Line, LineDto>>(),
            scope.ServiceProvider.GetRequiredService<IMapper<Line, LineDto>>()
        );
        Assert.Same(
            scope.ServiceProvider.GetRequiredService<IMapper<Order, OrderDto>>(),
            scope.ServiceProvider.GetRequiredService<IMapper<Order, OrderDto>>()
        );
        Assert.Equal(2, services.GetMappedPairs().Pairs.Count);
    }

    [Fact]
    public void A_pair_registered_twice_through_one_multi_pair_class_is_still_a_duplicate()
    {
        var services = new ServiceCollection();

        Assert.Throws<InvalidOperationException>(() =>
            services.AddHostLoomMapping(mapping =>
                mapping.Add<Line, LineDto, TwoPairMapper>().Add<Line, LineDto, TwoPairMapper>()
            )
        );
    }

    // -- Registry shape ---------------------------------------------------------------------------

    [Fact]
    public void Reading_the_pairs_before_any_registration_registers_nothing()
    {
        var services = new ServiceCollection();

        MappedPairRegistry registry = services.GetMappedPairs();

        Assert.Empty(registry.Pairs);
        Assert.Empty(registry.UpdatePairs);
        Assert.Empty(services);
    }

    [Fact]
    public void The_registry_the_provider_resolves_is_the_one_the_collection_exposes()
    {
        var services = new ServiceCollection();
        services.AddHostLoomMapping(mapping => mapping.Add<LineMapper>());
        using var provider = services.BuildServiceProvider();

        Assert.Same(services.GetMappedPairs(), provider.GetRequiredService<MappedPairRegistry>());
    }

    [Fact]
    public void The_pair_lists_are_live_read_only_views()
    {
        var services = new ServiceCollection();
        MappingBuilder mapping = services.AddHostLoomMapping();
        IReadOnlyList<MappedTypePair> pairs = services.GetMappedPairs().Pairs;
        IReadOnlyList<MappedTypePair> updates = services.GetMappedPairs().UpdatePairs;

        mapping.Add<LineMapper>().AddUpdate<LineUpdateMapper>();

        Assert.Equal([new MappedTypePair(typeof(Line), typeof(LineDto))], pairs);
        Assert.Equal([new MappedTypePair(typeof(Line), typeof(LineDto))], updates);
        Assert.IsNotType<List<MappedTypePair>>(pairs, exactMatch: false);
        Assert.IsNotType<List<MappedTypePair>>(updates, exactMatch: false);
    }

    [Fact]
    public void The_registry_rejects_null_lookups()
    {
        var registry = new MappedPairRegistry();

        Assert.Throws<ArgumentNullException>(() => registry.DestinationsFor(null!));
        Assert.Throws<ArgumentNullException>(() => registry.Contains(null!, typeof(LineDto)));
        Assert.Throws<ArgumentNullException>(() => registry.Contains(typeof(Line), null!));
        Assert.Throws<ArgumentNullException>(() => registry.ContainsUpdate(null!, typeof(LineDto)));
        Assert.Throws<ArgumentNullException>(() => registry.ContainsUpdate(typeof(Line), null!));
    }

    // -- Update map registration edges -------------------------------------------------------------

    [Fact]
    public void An_update_factory_receives_the_provider_and_honours_its_lifetime()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new Trim(true));
        services.AddHostLoomMapping(mapping =>
            mapping.AddUpdate<Line, LineDto>(
                provider => new LineUpdateMapper(provider.GetRequiredService<Trim>()),
                ServiceLifetime.Singleton
            )
        );
        using var provider = services.BuildServiceProvider();
        var destination = new LineDto { Sku = "old", Quantity = 0 };

        var mapper = provider.GetRequiredService<IUpdateMapper<Line, LineDto>>();
        mapper.MapInto(new Line("  a  ", 2), destination);

        Assert.Equal("a", destination.Sku);
        Assert.Same(mapper, provider.GetRequiredService<IUpdateMapper<Line, LineDto>>());
    }

    [Fact]
    public void An_update_map_with_a_scoped_dependency_resolves_per_scope()
    {
        var services = new ServiceCollection();
        services.AddScoped<Trim>(_ => new Trim(false));
        services.AddHostLoomMapping(mapping => mapping.AddUpdate<LineUpdateMapper>());
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );

        using var first = provider.CreateScope();
        using var second = provider.CreateScope();

        Assert.NotSame(
            first.ServiceProvider.GetRequiredService<IUpdateMapper<Line, LineDto>>(),
            second.ServiceProvider.GetRequiredService<IUpdateMapper<Line, LineDto>>()
        );
        Assert.Throws<InvalidOperationException>(() =>
            provider.GetRequiredService<IUpdateMapper<Line, LineDto>>()
        );
    }

    // -- Concurrency ------------------------------------------------------------------------------

    [Fact]
    public async Task The_dispatcher_maps_concurrently_across_scopes_without_cross_talk()
    {
        var services = new ServiceCollection();
        services.AddScoped<Trim>(_ => new Trim(true));
        services.AddHostLoomMapping(mapping => mapping.Add<LineMapper>());
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateScopes = true }
        );

        using var start = new ManualResetEventSlim();
        var tasks = Enumerable
            .Range(0, 32)
            .Select(worker =>
                Task.Run(() =>
                {
                    start.Wait();
                    using var scope = provider.CreateScope();
                    var mapper = scope.ServiceProvider.GetRequiredService<IMapper>();
                    var results = new List<string>(64);
                    for (var i = 0; i < 64; i++)
                    {
                        results.Add(mapper.From(new Line($" {worker}-{i} ", i)).To<LineDto>().Sku);
                    }

                    return results;
                })
            )
            .ToArray();
        start.Set();

        List<string>[] all = await Task.WhenAll(tasks)
            .WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

        for (var worker = 0; worker < all.Length; worker++)
        {
            Assert.Equal(Enumerable.Range(0, 64).Select(i => $"{worker}-{i}"), all[worker]);
        }
    }

    [Fact]
    public async Task Concurrent_first_registrations_of_one_map_class_agree_on_its_pair()
    {
        // Pair inference is cached in a per-class static. Racing the first use from several
        // composition roots must yield the same closed descriptor in each.
        using var start = new ManualResetEventSlim();
        var tasks = Enumerable
            .Range(0, 16)
            .Select(_ =>
                Task.Run(() =>
                {
                    start.Wait();
                    var services = new ServiceCollection();
                    services.AddHostLoomMapping(mapping => mapping.Add<RacedMapper>());
                    return services.Single(descriptor =>
                        descriptor.ServiceType.IsGenericType
                        && descriptor.ServiceType.GetGenericTypeDefinition() == typeof(IMapper<,>)
                    );
                })
            )
            .ToArray();
        start.Set();

        ServiceDescriptor[] descriptors = await Task.WhenAll(tasks)
            .WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

        Assert.All(
            descriptors,
            descriptor =>
            {
                Assert.Equal(typeof(IMapper<Order, OrderDto>), descriptor.ServiceType);
                Assert.Equal(typeof(RacedMapper), descriptor.ImplementationType);
            }
        );
    }

    // -- Test builder edges -----------------------------------------------------------------------

    [Fact]
    public void Each_build_takes_its_own_snapshot_of_the_test_builder()
    {
        var builder = new TestMapperBuilder().Add<Line, LineDto>(new LineMapper(new Trim(false)));
        IMapper first = builder.Build();
        builder.Add<Order, OrderDto>(order => new OrderDto { Reference = order.Reference });
        IMapper second = builder.Build();

        Assert.Throws<MappingNotFoundException>(() =>
            first.Map<Order, OrderDto>(new Order("o1", []))
        );
        Assert.Equal("o1", second.Map<Order, OrderDto>(new Order("o1", [])).Reference);
    }

    [Fact]
    public void A_delegate_map_that_returns_null_returns_it_through_the_non_null_contract()
    {
        // Pinned as observed: the test builder does not guard a null delegate result the way
        // the container's factory overload guards a null factory result. A test double that
        // returns null therefore hands null to a caller typed as non-null.
        IMapper mapper = new TestMapperBuilder().Add<Line, LineDto>(_ => null!).Build();

        LineDto result = mapper.Map<Line, LineDto>(new Line("a", 1));

        Assert.Null(result);
    }

    // -- Fixtures ----------------------------------------------------------------------------------

    public sealed record Line(string Sku, int Quantity);

    public sealed class LineDto
    {
        public required string Sku { get; set; }

        public required int Quantity { get; set; }
    }

    public sealed record Order(string Reference, IReadOnlyList<Line> Lines);

    public sealed class OrderDto
    {
        public required string Reference { get; set; }

        public IReadOnlyList<LineDto> Lines { get; set; } = [];
    }

    public sealed record Trim(bool Enabled);

    public sealed class LineMapper(Trim trim) : IMapper<Line, LineDto>
    {
        public LineDto Map(Line source) =>
            new()
            {
                Sku = trim.Enabled ? source.Sku.Trim() : source.Sku,
                Quantity = source.Quantity,
            };
    }

    public sealed class LineUpdateMapper(Trim trim) : IUpdateMapper<Line, LineDto>
    {
        public void MapInto(Line source, LineDto destination)
        {
            destination.Sku = trim.Enabled ? source.Sku.Trim() : source.Sku;
            destination.Quantity = source.Quantity;
        }
    }

    public sealed class OrderMapper(IMapper<Line, LineDto> lines) : IMapper<Order, OrderDto>
    {
        public OrderDto Map(Order source) =>
            new() { Reference = source.Reference, Lines = lines.MapMany(source.Lines) };
    }

    public sealed class TwoPairMapper : IMapper<Line, LineDto>, IMapper<Order, OrderDto>
    {
        public LineDto Map(Line source) => new() { Sku = source.Sku, Quantity = source.Quantity };

        public OrderDto Map(Order source) => new() { Reference = source.Reference };
    }

    public sealed class RacedMapper : IMapper<Order, OrderDto>
    {
        public OrderDto Map(Order source) => new() { Reference = source.Reference };
    }
}
