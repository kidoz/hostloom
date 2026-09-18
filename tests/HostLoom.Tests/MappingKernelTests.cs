using System.Collections;
using HostLoom.Mapping;
using HostLoom.Mapping.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// The dependency-free kernel on its own: contract variance, every path through the sequence
/// extensions, the source wrapper, and the exception and attribute types.
/// </summary>
public sealed class MappingKernelTests
{
    // -- Contract variance -------------------------------------------------------------------------

    [Fact]
    public void A_creation_map_is_contravariant_in_its_source_and_covariant_in_its_destination()
    {
        // The variance is part of the public contract: a map over a base source serves a derived
        // one, and a map producing a derived destination satisfies a consumer of its base.
        IMapper<Item, Preferred> mapper = new PreferredMapper();

        IMapper<Product, Preferred> narrowedSource = mapper;
        IMapper<Item, Summary> widenedDestination = mapper;
        IMapper<Product, Summary> both = mapper;

        Assert.Equal("acme notebook", narrowedSource.Map(new Product("notebook", "acme")).Name);
        Assert.Equal("book", widenedDestination.Map(new Item("book")).Name);
        Assert.IsType<Preferred>(both.Map(new Product("music", "acme")));
    }

    [Fact]
    public void An_update_map_is_contravariant_in_both_parameters()
    {
        IUpdateMapper<Item, Summary> mapper = new SummaryUpdateMapper();

        IUpdateMapper<Product, Preferred> narrowed = mapper;
        var destination = new Preferred { Name = "old", Vendor = "acme" };

        narrowed.MapInto(new Product("notebook", "other"), destination);

        Assert.Equal("notebook", destination.Name);
        Assert.Equal("acme", destination.Vendor);
    }

    // -- MapMany paths -----------------------------------------------------------------------------

    [Fact]
    public void MapMany_takes_the_indexed_path_for_any_read_only_list()
    {
        var mapper = new CountingMapper();
        var source = new IndexOnlyList([new Item("a"), new Item("b"), new Item("c")]);

        IReadOnlyList<Summary> mapped = mapper.MapMany(source);

        Assert.Equal(["a", "b", "c"], mapped.Select(summary => summary.Name));
        Assert.Equal(0, source.Enumerations);
        Assert.Equal(3, source.IndexReads);
        Assert.Equal(3, mapper.Calls);
    }

    [Fact]
    public void MapMany_sizes_from_a_counted_collection_and_grows_from_an_iterator()
    {
        var mapper = new CountingMapper();
        var counted = new HashSet<Item> { new("a"), new("b") };

        IReadOnlyList<Summary> fromCounted = mapper.MapMany(counted);
        IReadOnlyList<Summary> fromIterator = mapper.MapMany(Items("c", "d", "e"));

        Assert.Equal(2, fromCounted.Count);
        Assert.Equal(["c", "d", "e"], fromIterator.Select(summary => summary.Name));
        Assert.Equal(5, mapper.Calls);
    }

    [Fact]
    public void MapMany_returns_an_empty_result_for_every_empty_shape()
    {
        var mapper = new CountingMapper();

        Assert.Empty(mapper.MapMany(new IndexOnlyList([])));
        Assert.Empty(mapper.MapMany(new HashSet<Item>()));
        Assert.Empty(mapper.MapMany(Items()));
        Assert.Empty(mapper.MapManyOrEmpty(Items()));
        Assert.Equal(0, mapper.Calls);
    }

    [Fact]
    public void MapMany_does_not_hand_back_or_retain_the_source_collection()
    {
        var mapper = new CountingMapper();
        var source = new List<Item> { new("a") };

        IReadOnlyList<Summary> mapped = mapper.MapMany(source);
        source.Add(new Item("b"));

        Assert.NotSame(source, mapped);
        Assert.Single(mapped);
    }

    [Fact]
    public void A_map_that_throws_mid_sequence_surfaces_the_failure_and_stops()
    {
        var mapper = new CountingMapper(failOn: "b");

        var exception = Assert.Throws<InvalidOperationException>(() =>
            mapper.MapMany([new Item("a"), new Item("b"), new Item("c")])
        );

        Assert.Contains("b", exception.Message, StringComparison.Ordinal);
        Assert.Equal(2, mapper.Calls);
    }

    [Fact]
    public void A_deferred_sequence_maps_again_on_every_enumeration()
    {
        var mapper = new CountingMapper();
        IEnumerable<Summary> deferred = mapper.MapManyDeferred([new Item("a"), new Item("b")]);

#pragma warning disable CA1851 // Enumerating twice is the behavior under test.
        var first = deferred.ToList();
        var second = deferred.ToList();
#pragma warning restore CA1851

        // Nothing is cached: a deferred map is a view, and each pass pays for its elements again.
        Assert.Equal(first.Select(s => s.Name), second.Select(s => s.Name));
        Assert.Equal(4, mapper.Calls);
    }

    [Fact]
    public void A_deferred_sequence_that_outlives_its_scope_enumerates_a_disposed_map()
    {
        // The documented hazard, pinned so a change in container behavior cannot make the README
        // wrong in silence: the deferred result holds the map, and the map's scoped dependency
        // is gone once the scope is.
        var services = new ServiceCollection();
        services.AddScoped<ScopedResource>();
        services.AddHostLoomMapping(mapping => mapping.Add<ScopedResourceMapper>());
        using var provider = services.BuildServiceProvider();

        IEnumerable<Summary> deferred;
        using (var scope = provider.CreateScope())
        {
            deferred = scope
                .ServiceProvider.GetRequiredService<IMapper<Item, Summary>>()
                .MapManyDeferred([new Item("a")]);
        }

        Assert.Throws<ObjectDisposedException>(() => deferred.ToList());
    }

    // -- Source wrapper ----------------------------------------------------------------------------

    [Fact]
    public void From_rejects_a_null_mapper_or_source_before_a_destination_is_chosen()
    {
        IMapper dispatcher = new TestMapperStub();

        Assert.Throws<ArgumentNullException>(() => ((IMapper)null!).From(new Item("a")));
        Assert.Throws<ArgumentNullException>(() => dispatcher.From<Item>(null!));
    }

    [Fact]
    public void A_mapping_source_is_a_value_that_compares_by_mapper_and_source()
    {
        IMapper dispatcher = new TestMapperStub();
        var item = new Item("a");

        Assert.Equal(dispatcher.From(item), dispatcher.From(item));
        Assert.NotEqual(dispatcher.From(item), dispatcher.From(new Item("b")));
    }

    // -- Exception and attribute -------------------------------------------------------------------

    [Fact]
    public void The_not_found_exception_rejects_null_types_and_lists()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new MappingNotFoundException(null!, typeof(Summary))
        );
        Assert.Throws<ArgumentNullException>(() =>
            new MappingNotFoundException(typeof(Item), null!)
        );
        Assert.Throws<ArgumentNullException>(() =>
            new MappingNotFoundException(typeof(Item), typeof(Summary), null!)
        );
    }

    [Fact]
    public void The_not_found_exception_is_an_invalid_operation_with_an_empty_default_list()
    {
        var exception = new MappingNotFoundException(typeof(Item), typeof(Summary));

        Assert.IsAssignableFrom<InvalidOperationException>(exception);
        Assert.Empty(exception.RegisteredDestinations);
        Assert.Contains("AddHostLoomMapping", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(
            "is registered to map to",
            exception.Message,
            StringComparison.Ordinal
        );
    }

    [Fact]
    public void The_not_found_exception_lists_every_registered_destination_in_order()
    {
        var exception = new MappingNotFoundException(
            typeof(Item),
            typeof(Preferred),
            [typeof(Summary), typeof(Product)]
        );

        Assert.Equal([typeof(Summary), typeof(Product)], exception.RegisteredDestinations);
        var summary = exception.Message.IndexOf(
            typeof(Summary).FullName!,
            StringComparison.Ordinal
        );
        var product = exception.Message.IndexOf(
            typeof(Product).FullName!,
            StringComparison.Ordinal
        );
        Assert.True(summary >= 0 && product > summary);
    }

    [Fact]
    public void The_unmapped_members_attribute_keeps_its_names_and_rejects_null()
    {
        var attribute = new UnmappedMembersAttribute("ImageUrl", "VendorId");

        Assert.Equal(["ImageUrl", "VendorId"], attribute.Members);
        Assert.Empty(new UnmappedMembersAttribute().Members);
        Assert.Throws<ArgumentNullException>(() => new UnmappedMembersAttribute(null!));
        var usage = Assert.Single(
            typeof(UnmappedMembersAttribute).GetCustomAttributes(
                typeof(AttributeUsageAttribute),
                false
            )
        );
        Assert.Equal(AttributeTargets.Class, ((AttributeUsageAttribute)usage).ValidOn);
        Assert.False(((AttributeUsageAttribute)usage).AllowMultiple);
    }

    // -- Fixtures ----------------------------------------------------------------------------------

    private static IEnumerable<Item> Items(params string[] names)
    {
        foreach (var name in names)
        {
            yield return new Item(name);
        }
    }

    public record Item(string Name);

    public sealed record Product(string Name, string Vendor) : Item(Name);

    public class Summary
    {
        public required string Name { get; set; }
    }

    public sealed class Preferred : Summary
    {
        public required string Vendor { get; set; }
    }

    public sealed class PreferredMapper : IMapper<Item, Preferred>
    {
        public Preferred Map(Item source) =>
            source is Product product
                ? new Preferred
                {
                    Name = $"{product.Vendor} {product.Name}",
                    Vendor = product.Vendor,
                }
                : new Preferred { Name = source.Name, Vendor = string.Empty };
    }

    public sealed class SummaryUpdateMapper : IUpdateMapper<Item, Summary>
    {
        public void MapInto(Item source, Summary destination) => destination.Name = source.Name;
    }

    public sealed class CountingMapper(string? failOn = null) : IMapper<Item, Summary>
    {
        public int Calls { get; private set; }

        public Summary Map(Item source)
        {
            Calls++;
            return source.Name == failOn
                ? throw new InvalidOperationException($"cannot map '{source.Name}'")
                : new Summary { Name = source.Name };
        }
    }

    public sealed class ScopedResource : IDisposable
    {
        private bool _disposed;

        public string Read()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return "live";
        }

        public void Dispose() => _disposed = true;
    }

    public sealed class ScopedResourceMapper(ScopedResource resource) : IMapper<Item, Summary>
    {
        public Summary Map(Item source) => new() { Name = $"{source.Name}:{resource.Read()}" };
    }

    /// <summary>A read-only list that is not an array or List, counting how it is consumed.</summary>
    public sealed class IndexOnlyList(Item[] items) : IReadOnlyList<Item>
    {
        public int Enumerations { get; private set; }

        public int IndexReads { get; private set; }

        public int Count => items.Length;

        public Item this[int index]
        {
            get
            {
                IndexReads++;
                return items[index];
            }
        }

        public IEnumerator<Item> GetEnumerator()
        {
            Enumerations++;
            return ((IEnumerable<Item>)items).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private sealed class TestMapperStub : IMapper
    {
        public TDestination Map<TSource, TDestination>(TSource source)
            where TSource : notnull
            where TDestination : notnull =>
            throw new MappingNotFoundException(typeof(TSource), typeof(TDestination));
    }
}
