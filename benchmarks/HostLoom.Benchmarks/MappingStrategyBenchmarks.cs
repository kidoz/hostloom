using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using HostLoom.Mapping;
using HostLoom.Mapping.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

// CA1707: underscored benchmark names are how the results table stays readable.
// CA2000: provider lifetimes are owned by GlobalSetup/GlobalCleanup.
// CA1822: BenchmarkDotNet discovers instance methods, so a stateless benchmark cannot be static.
#pragma warning disable CA1707, CA2000, CA1822

namespace HostLoom.Benchmarks;

/// <summary>
/// Candidate element-access strategies for <c>MapMany</c>, measured rather than argued. The
/// shipped implementation indexes through <see cref="IReadOnlyList{T}"/>, which is one interface
/// call per element; the alternatives trade that for a span, or avoid indexing entirely. The two
/// source shapes are the ones a map is realistically handed — an array, and the <c>List&lt;T&gt;</c>
/// that a protobuf repeated field materializes as.
/// </summary>
[MemoryDiagnoser]
public class MapManyStrategyBenchmarks
{
    private readonly CustomerMapper _mapper = new();
    private Customer[] _array = null!;
    private List<Customer> _list = null!;

    [Params(100, 1000)]
    public int Count { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _array = MappingData.CustomerBatch(Count);
        _list = [.. _array];
    }

    // -- Array source -------------------------------------------------------------------------

    /// <summary>What ships today: one IReadOnlyList indexer call per element.</summary>
    [Benchmark(Baseline = true, Description = "IReadOnlyList index")]
    [BenchmarkCategory("Array")]
    public IReadOnlyList<CustomerDto> Array_Indexed() => Indexed(_mapper, _array);

    [Benchmark(Description = "span")]
    [BenchmarkCategory("Array")]
    public IReadOnlyList<CustomerDto> Array_Span() => Spanned(_mapper, _array);

    [Benchmark(Description = "enumerate into List")]
    [BenchmarkCategory("Array")]
    public IReadOnlyList<CustomerDto> Array_Enumerated() => Enumerated(_mapper, _array);

    // -- List<T> source -----------------------------------------------------------------------

    [Benchmark(Description = "IReadOnlyList index (List)")]
    [BenchmarkCategory("List")]
    public IReadOnlyList<CustomerDto> List_Indexed() => Indexed(_mapper, _list);

    [Benchmark(Description = "span (List)")]
    [BenchmarkCategory("List")]
    public IReadOnlyList<CustomerDto> List_Span() => Spanned(_mapper, _list);

    [Benchmark(Description = "enumerate into List (List)")]
    [BenchmarkCategory("List")]
    public IReadOnlyList<CustomerDto> List_Enumerated() => Enumerated(_mapper, _list);

    // -- Candidate implementations ------------------------------------------------------------

    private static IReadOnlyList<TDestination> Indexed<TSource, TDestination>(
        IMapper<TSource, TDestination> mapper,
        IEnumerable<TSource> source
    )
        where TSource : notnull
        where TDestination : notnull
    {
        if (source is IReadOnlyList<TSource> indexable)
        {
            var mapped = new TDestination[indexable.Count];
            for (var index = 0; index < mapped.Length; index++)
            {
                mapped[index] = mapper.Map(indexable[index]);
            }

            return mapped;
        }

        return Enumerated(mapper, source);
    }

    private static IReadOnlyList<TDestination> Spanned<TSource, TDestination>(
        IMapper<TSource, TDestination> mapper,
        IEnumerable<TSource> source
    )
        where TSource : notnull
        where TDestination : notnull
    {
        // Both concrete shapes expose contiguous storage, so the per-element interface call and
        // its bounds check can be dropped entirely.
        if (source is TSource[] array)
        {
            return FromSpan(mapper, array);
        }

        if (source is List<TSource> list)
        {
            return FromSpan(mapper, CollectionsMarshal.AsSpan(list));
        }

        return Indexed(mapper, source);
    }

    private static TDestination[] FromSpan<TSource, TDestination>(
        IMapper<TSource, TDestination> mapper,
        ReadOnlySpan<TSource> source
    )
        where TSource : notnull
        where TDestination : notnull
    {
        var mapped = new TDestination[source.Length];
        for (var index = 0; index < source.Length; index++)
        {
            mapped[index] = mapper.Map(source[index]);
        }

        return mapped;
    }

    private static List<TDestination> Enumerated<TSource, TDestination>(
        IMapper<TSource, TDestination> mapper,
        IEnumerable<TSource> source
    )
        where TSource : notnull
        where TDestination : notnull
    {
        var results = source.TryGetNonEnumeratedCount(out var count)
            ? new List<TDestination>(count)
            : [];
        foreach (var item in source)
        {
            results.Add(mapper.Map(item));
        }

        return results;
    }
}

/// <summary>
/// What the dispatcher's extra allocation actually is. The closed mapper is injected once and
/// costs nothing per call; the dispatcher resolves the pair from the scope on every call, which
/// constructs the map class when it is registered transient — the package default. Registering a
/// stateless map as a singleton is the one lever available, and this measures whether it closes
/// the gap or merely narrows it.
/// </summary>
[MemoryDiagnoser]
public class MappingLifetimeBenchmarks
{
    private ServiceProvider _transient = null!;
    private ServiceProvider _singleton = null!;
    private IServiceScope _transientScope = null!;
    private IServiceScope _singletonScope = null!;
    private IMapper _transientDispatcher = null!;
    private IMapper _singletonDispatcher = null!;
    private IMapper<Customer, CustomerDto> _closed = null!;

    private readonly Customer _customer = MappingData.Customer;

    [GlobalSetup]
    public void Setup()
    {
        var transientServices = new ServiceCollection();
        transientServices.AddHostLoomMapping(mapping => mapping.Add<CustomerMapper>());
        _transient = transientServices.BuildServiceProvider();

        var singletonServices = new ServiceCollection();
        singletonServices.AddHostLoomMapping(mapping =>
            mapping.Add<CustomerMapper>(ServiceLifetime.Singleton)
        );
        _singleton = singletonServices.BuildServiceProvider();

        _transientScope = _transient.CreateScope();
        _singletonScope = _singleton.CreateScope();
        _transientDispatcher = _transientScope.ServiceProvider.GetRequiredService<IMapper>();
        _singletonDispatcher = _singletonScope.ServiceProvider.GetRequiredService<IMapper>();
        _closed = _transientScope.ServiceProvider.GetRequiredService<
            IMapper<Customer, CustomerDto>
        >();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _transientScope.Dispose();
        _singletonScope.Dispose();
        _transient.Dispose();
        _singleton.Dispose();
    }

    /// <summary>The floor: an injected closed map, no container involvement per call.</summary>
    [Benchmark(Baseline = true)]
    public CustomerDto Closed_Injected() => _closed.Map(_customer);

    /// <summary>The package default — the map class is constructed on every dispatch.</summary>
    [Benchmark]
    public CustomerDto Dispatcher_TransientMap() =>
        _transientDispatcher.Map<Customer, CustomerDto>(_customer);

    /// <summary>The same dispatch against a map registered once for the process.</summary>
    [Benchmark]
    public CustomerDto Dispatcher_SingletonMap() =>
        _singletonDispatcher.Map<Customer, CustomerDto>(_customer);
}

/// <summary>
/// Whether inferring a pair should be cached, isolated to the step that actually differs: reading
/// the closed <c>IMapper&lt;,&gt;</c> off a map class. The shipped implementation walks
/// <see cref="Type.GetInterfaces"/> on every registration; a static field on a generic type walks
/// once per closed map class for the life of the process. Registration itself is excluded so the
/// container's own cost does not mask the difference.
/// </summary>
[MemoryDiagnoser]
public class MappingInferenceBenchmarks
{
    /// <summary>The pair named by the caller: a token load, and the floor for the other two.</summary>
    [Benchmark(Baseline = true)]
    public Type Explicit() => typeof(IMapper<Customer, CustomerDto>);

    /// <summary>Inference as shipped — one interface walk and one Type[] per registration.</summary>
    [Benchmark]
    public Type Walked() => ResolveByWalk(typeof(CustomerMapper));

    /// <summary>The same answer from a per-map-class static, walked once and then read.</summary>
    [Benchmark]
    public Type Cached() => CachedPair<CustomerMapper>.ServiceType;

    private static Type ResolveByWalk(Type mapperType)
    {
        foreach (var candidate in mapperType.GetInterfaces())
        {
            if (
                candidate.IsGenericType
                && candidate.GetGenericTypeDefinition() == typeof(IMapper<,>)
            )
            {
                return candidate;
            }
        }

        throw new InvalidOperationException("unreachable in this benchmark");
    }

    private static class CachedPair<TMapper>
    {
        public static readonly Type ServiceType = ResolveByWalk(typeof(TMapper));
    }
}
