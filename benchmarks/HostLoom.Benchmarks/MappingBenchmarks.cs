using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using HostLoom.Mapping;
using Microsoft.Extensions.DependencyInjection;

// CA1707: underscored benchmark names are how the results table stays readable.
// CA2000: provider lifetimes are owned by GlobalSetup/GlobalCleanup.
// CA1822: BenchmarkDotNet discovers instance methods, so a stateless benchmark cannot be static.
#pragma warning disable CA1707, CA2000, CA1822

namespace HostLoom.Benchmarks;

/// <summary>
/// One map, steady state, for a flat contract and a nested one. Both libraries are fully warm:
/// AutoMapper's execution plans are compiled in <see cref="Setup"/>, so these numbers are the
/// per-call cost after startup, not the cost of getting there — <see cref="MappingStartupBenchmarks"/>
/// measures that separately. The closed-map and AutoMapper rows call a mapper resolved once in
/// setup; the dispatcher rows resolve the pair from an already-entered scope on every call, which
/// is what the dispatcher does by design and part of what these rows measure. Entering a scope per
/// unit of work is the separate cost, in <see cref="MappingResolutionBenchmarks"/>.
/// </summary>
[MemoryDiagnoser]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class MappingBenchmarks
{
    private const string Flat = "Flat";
    private const string Nested = "Nested";

    private ServiceProvider _hostLoom = null!;
    private IServiceScope _scope = null!;
    private IMapper _dispatcher = null!;
    private IMapper<Customer, CustomerDto> _customerMapper = null!;
    private IMapper<Invoice, InvoiceDto> _invoiceMapper = null!;
    private AutoMapper.IMapper _autoMapper = null!;

    private readonly Customer _customer = MappingData.Customer;
    private readonly Invoice _invoice = MappingData.Invoice;

    [GlobalSetup]
    public void Setup()
    {
        _hostLoom = MappingRegistration
            .AddHostLoomMaps(new ServiceCollection())
            .BuildServiceProvider();
        _scope = _hostLoom.CreateScope();
        _dispatcher = _scope.ServiceProvider.GetRequiredService<IMapper>();
        _customerMapper = _scope.ServiceProvider.GetRequiredService<
            IMapper<Customer, CustomerDto>
        >();
        _invoiceMapper = _scope.ServiceProvider.GetRequiredService<IMapper<Invoice, InvoiceDto>>();
        _autoMapper = MappingRegistration.CreateAutoMapper();
        MappingRegistration.VerifyEquivalence(_customerMapper, _invoiceMapper, _autoMapper);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _scope.Dispose();
        _hostLoom.Dispose();
    }

    // -- Flat: eight scalars, name-for-name ---------------------------------------------------

    /// <summary>A direct call on the injected closed map — a plain virtual call and a constructor.</summary>
    [Benchmark(Baseline = true, Description = "HostLoom closed map"), BenchmarkCategory(Flat)]
    public CustomerDto HostLoom_Closed_Flat() => _customerMapper.Map(_customer);

    /// <summary>The dispatcher facade, which resolves the pair from the scope on every call.</summary>
    [Benchmark(Description = "HostLoom dispatcher"), BenchmarkCategory(Flat)]
    public CustomerDto HostLoom_Dispatcher_Flat() =>
        _dispatcher.Map<Customer, CustomerDto>(_customer);

    /// <summary>The ergonomic wrapper. It should cost the dispatcher and nothing more.</summary>
    [Benchmark(Description = "HostLoom From/To"), BenchmarkCategory(Flat)]
    public CustomerDto HostLoom_FromTo_Flat() => _dispatcher.From(_customer).To<CustomerDto>();

    [Benchmark(Description = "AutoMapper"), BenchmarkCategory(Flat)]
    public CustomerDto AutoMapper_Flat() => _autoMapper.Map<Customer, CustomerDto>(_customer);

    // -- Nested: child object plus a three-element child collection ---------------------------

    [Benchmark(Baseline = true, Description = "HostLoom closed map"), BenchmarkCategory(Nested)]
    public InvoiceDto HostLoom_Closed_Nested() => _invoiceMapper.Map(_invoice);

    [Benchmark(Description = "HostLoom dispatcher"), BenchmarkCategory(Nested)]
    public InvoiceDto HostLoom_Dispatcher_Nested() =>
        _dispatcher.Map<Invoice, InvoiceDto>(_invoice);

    [Benchmark(Description = "AutoMapper"), BenchmarkCategory(Nested)]
    public InvoiceDto AutoMapper_Nested() => _autoMapper.Map<Invoice, InvoiceDto>(_invoice);
}

/// <summary>
/// The same flat map applied to a batch. HostLoom has no collection feature, so its side is the
/// hand-written loop an application writes; AutoMapper's side is its built-in array map. Both
/// produce a <c>CustomerDto[]</c> of the same length, so the allocation columns are comparable.
/// </summary>
[MemoryDiagnoser]
public class MappingCollectionBenchmarks
{
    private ServiceProvider _hostLoom = null!;
    private IServiceScope _scope = null!;
    private IMapper<Customer, CustomerDto> _customerMapper = null!;
    private AutoMapper.IMapper _autoMapper = null!;
    private Customer[] _batch = null!;

    /// <summary>A page of results and a bulk import — the two sizes that actually show up.</summary>
    [Params(100, 1000)]
    public int Count { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _hostLoom = MappingRegistration
            .AddHostLoomMaps(new ServiceCollection())
            .BuildServiceProvider();
        _scope = _hostLoom.CreateScope();
        _customerMapper = _scope.ServiceProvider.GetRequiredService<
            IMapper<Customer, CustomerDto>
        >();
        _autoMapper = MappingRegistration.CreateAutoMapper();
        _batch = MappingData.CustomerBatch(Count);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _scope.Dispose();
        _hostLoom.Dispose();
    }

    [Benchmark(Baseline = true)]
    public CustomerDto[] HostLoom_Loop()
    {
        var result = new CustomerDto[_batch.Length];
        for (var index = 0; index < _batch.Length; index++)
        {
            result[index] = _customerMapper.Map(_batch[index]);
        }

        return result;
    }

    [Benchmark]
    public CustomerDto[] AutoMapper_Collection() =>
        _autoMapper.Map<Customer[], CustomerDto[]>(_batch);

    /// <summary>
    /// The shipped helper against the hand-written loop it exists to delete. R1's premise is that
    /// a consumer can drop its own extension class without paying for it, so this row is the one
    /// that has to stay level with the baseline rather than merely beat AutoMapper.
    /// </summary>
    [Benchmark]
    public IReadOnlyList<CustomerDto> HostLoom_MapMany() => _customerMapper.MapMany(_batch);
}

/// <summary>
/// The shape application code actually pays per unit of work: enter a scope, resolve a mapper,
/// map, leave. HostLoom's dispatcher is registered scoped and resolves the closed pair from the
/// scope on each call; AutoMapper's <c>IMapper</c> is registered by its own DI extension. The
/// closed-map row has no AutoMapper counterpart — AutoMapper exposes no per-pair service type.
/// </summary>
[MemoryDiagnoser]
public class MappingResolutionBenchmarks
{
    private ServiceProvider _hostLoom = null!;
    private ServiceProvider _hostLoomFactory = null!;
    private ServiceProvider _autoMapper = null!;

    private readonly Customer _customer = MappingData.Customer;

    [GlobalSetup]
    public void Setup()
    {
        _hostLoom = MappingRegistration
            .AddHostLoomMaps(new ServiceCollection())
            .BuildServiceProvider();
        _hostLoomFactory = MappingRegistration
            .AddHostLoomMapsFactory(new ServiceCollection())
            .BuildServiceProvider();
        _autoMapper = MappingRegistration
            .AddAutoMapperMaps(new ServiceCollection())
            .BuildServiceProvider();

        // Resolve once outside the measurement so neither side pays first-resolution call-site
        // construction or first-map plan compilation inside a benchmark iteration.
        using var hostLoomScope = _hostLoom.CreateScope();
        hostLoomScope
            .ServiceProvider.GetRequiredService<IMapper>()
            .Map<Customer, CustomerDto>(_customer);
        using var factoryScope = _hostLoomFactory.CreateScope();
        factoryScope
            .ServiceProvider.GetRequiredService<IMapper>()
            .Map<Customer, CustomerDto>(_customer);
        using var autoMapperScope = _autoMapper.CreateScope();
        autoMapperScope
            .ServiceProvider.GetRequiredService<AutoMapper.IMapper>()
            .Map<Customer, CustomerDto>(_customer);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _hostLoom.Dispose();
        _hostLoomFactory.Dispose();
        _autoMapper.Dispose();
    }

    [Benchmark(Baseline = true)]
    public CustomerDto HostLoom_Scope_Dispatcher()
    {
        using var scope = _hostLoom.CreateScope();
        return scope
            .ServiceProvider.GetRequiredService<IMapper>()
            .Map<Customer, CustomerDto>(_customer);
    }

    [Benchmark]
    public CustomerDto HostLoom_Scope_ClosedMapper()
    {
        using var scope = _hostLoom.CreateScope();
        return scope
            .ServiceProvider.GetRequiredService<IMapper<Customer, CustomerDto>>()
            .Map(_customer);
    }

    /// <summary>
    /// A factory-registered pair, which is how a generic map class is closed. The factory replaces
    /// the container's own activator, so this row answers whether a service registering many pairs
    /// that way — one per closed generic — pays for it on every resolve.
    /// </summary>
    [Benchmark]
    public CustomerDto HostLoom_Scope_FactoryRegistered()
    {
        using var scope = _hostLoomFactory.CreateScope();
        return scope
            .ServiceProvider.GetRequiredService<IMapper<Customer, CustomerDto>>()
            .Map(_customer);
    }

    [Benchmark]
    public CustomerDto AutoMapper_Scope()
    {
        using var scope = _autoMapper.CreateScope();
        return scope
            .ServiceProvider.GetRequiredService<AutoMapper.IMapper>()
            .Map<Customer, CustomerDto>(_customer);
    }
}

/// <summary>
/// Registration only, with no container built and nothing resolved: what each way of declaring the
/// same four pairs costs. The question is whether inferring a pair from the map class's interface,
/// or closing it through a factory, is affordable at the scale a platform registers — 19 services
/// with about 95 pairs between them. Every row allocates one <see cref="ServiceCollection"/>, so
/// that constant is in the baseline and the difference is the registration itself.
/// </summary>
[MemoryDiagnoser]
public class MappingRegistrationBenchmarks
{
    /// <summary>The pair restated in the call, which is what the map class already declares.</summary>
    [Benchmark(Baseline = true)]
    public IServiceCollection HostLoom_Explicit() =>
        MappingRegistration.AddHostLoomMaps(new ServiceCollection());

    /// <summary>The pair read off the interface — one GetInterfaces walk per registration.</summary>
    [Benchmark]
    public IServiceCollection HostLoom_Inferred() =>
        MappingRegistration.AddHostLoomMapsInferred(new ServiceCollection());

    /// <summary>The pair closed at the call site, as a generic map class must be.</summary>
    [Benchmark]
    public IServiceCollection HostLoom_Factory() =>
        MappingRegistration.AddHostLoomMapsFactory(new ServiceCollection());

    /// <summary>
    /// AutoMapper's equivalent declaration. It only records the maps here — the expression
    /// compilation those declarations imply is charged in <see cref="MappingStartupBenchmarks"/>.
    /// </summary>
    [Benchmark]
    public IServiceCollection AutoMapper_Declare() =>
        MappingRegistration.AddAutoMapperMaps(new ServiceCollection());
}

/// <summary>
/// Cold start: nothing configured, one mapped object at the end. This is where a convention
/// mapper's expression compilation lands, and it is the number that matters for a serverless
/// cold start or a short-lived worker. HostLoom compiles its maps with the rest of the assembly,
/// so its cost here is container registration only.
/// </summary>
[MemoryDiagnoser]
public class MappingStartupBenchmarks
{
    private readonly Customer _customer = MappingData.Customer;

    /// <summary>Register four maps, build the container, enter a scope, map once.</summary>
    [Benchmark(Baseline = true)]
    public CustomerDto HostLoom_ColdStart_ThroughContainer()
    {
        using var provider = MappingRegistration
            .AddHostLoomMaps(new ServiceCollection())
            .BuildServiceProvider();
        using var scope = provider.CreateScope();
        return scope
            .ServiceProvider.GetRequiredService<IMapper>()
            .Map<Customer, CustomerDto>(_customer);
    }

    /// <summary>The same through AutoMapper's DI extension. Only the used plan is compiled.</summary>
    [Benchmark]
    public CustomerDto AutoMapper_ColdStart_ThroughContainer()
    {
        using var provider = MappingRegistration
            .AddAutoMapperMaps(new ServiceCollection())
            .BuildServiceProvider();
        using var scope = provider.CreateScope();
        return scope
            .ServiceProvider.GetRequiredService<AutoMapper.IMapper>()
            .Map<Customer, CustomerDto>(_customer);
    }

    /// <summary>
    /// AutoMapper without a container, so the row isolates its own configuration cost from the
    /// container's. Still lazy: only the flat plan is compiled, by the map call itself.
    /// </summary>
    [Benchmark]
    public CustomerDto AutoMapper_ColdStart_DirectConfiguration()
    {
        var configuration = new AutoMapper.MapperConfiguration(
            MappingRegistration.ConfigureAutoMapper,
            Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance
        );
        return configuration.CreateMapper().Map<Customer, CustomerDto>(_customer);
    }

    /// <summary>
    /// The same, but with every execution plan compiled up front — what an application does when
    /// it would rather pay at startup than on the first request to touch each map.
    /// </summary>
    [Benchmark]
    public CustomerDto AutoMapper_ColdStart_AllPlansCompiled() =>
        MappingRegistration.CreateAutoMapper().Map<Customer, CustomerDto>(_customer);
}
