# HostLoom.Mapping

`HostLoom.Mapping` is a small, explicit alternative to runtime convention mappers. A map is an
ordinary C# class, so member access, nullability, constructor choice, and conversions are checked
by the compiler. Mapping performs no assembly scanning, reflection-based member mapping,
expression compilation, runtime code generation, or global configuration.

The core package has no dependency-injection dependency. Install
`HostLoom.Mapping.DependencyInjection` to register map classes with the built-in .NET container.

```csharp
public sealed record Customer(string Name, Address Address);
public sealed record CustomerDto(string Name, string City);

public sealed class CustomerMapper : IMapper<Customer, CustomerDto>
{
    public CustomerDto Map(Customer source) =>
        new(source.Name, source.Address.City);
}

services.AddHostLoomMapping(mapping =>
    mapping.Add<CustomerMapper>());

// Prefer the closed mapper when only this pair is needed.
var customerMapper = provider.GetRequiredService<IMapper<Customer, CustomerDto>>();
var dto = customerMapper.Map(customer);

// Or use the dispatcher when an orchestration maps several pairs. It is scoped, so resolve it
// from a scope — a request, a delivery, or an explicit CreateScope.
using var scope = provider.CreateScope();
var mapper = scope.ServiceProvider.GetRequiredService<IMapper>();
var sameDto = mapper.From(customer).To<CustomerDto>();
```

`IMapper` is registered scoped, which has two consequences worth knowing before the first run.
Resolving it from the root provider throws once scope validation is on — the default for the
generic host in Development, so this fails there and would have succeeded in Production. For the
same reason it cannot be constructor-injected into a singleton; inject the closed
`IMapper<TSource, TDestination>` there, or take `IServiceScopeFactory` and resolve per unit of
work. Closed maps registered with the default transient lifetime have neither restriction.

Both of those failures, and a map whose constructor asks for a pair nobody registered, are caught
by the container's own validation — which `Host.CreateDefaultBuilder` enables only in Development.
That is the wrong way round: it means the environment least like production is the only one that
checks. Turn both on everywhere, so a missing inner map fails at host build rather than inside the
first message that needs it:

```csharp
builder.Host.UseDefaultServiceProvider(options =>
{
    options.ValidateScopes = true;
    options.ValidateOnBuild = true;
});
```

`Add<TMapper>()` reads the pair from the single closed `IMapper<TSource, TDestination>` the map
class implements, so a registration does not restate a type triple the class already declares —
and the file that registers a map needs no `using` for the contracts it maps between. Inference
reads metadata once per registration; nothing is reflected on the map path. Use the explicit
`Add<TSource, TDestination, TMapper>()` overload for a class that implements more than one pair,
or to close an open generic map. A class implementing no pair, or more than one, fails at
registration with both alternatives named.

### Generic map classes

A map class generic in more than its pair — `EntityMapper<TEntity, TModel, TTranslation>`
implementing `IMapper<TEntity, TModel>` — cannot be registered as an open generic, because the
container requires the open service type and open implementation type to have equal arity and
these never do. Close it at the call site with a factory instead, from a generic helper:

```csharp
static void AddEntityMap<TEntity, TModel, TTranslation>(MappingBuilder mapping)
    where TEntity : notnull where TModel : notnull =>
    mapping
        .Add<TEntity, TModel>(_ => new EntityMapper<TEntity, TModel, TTranslation>())
        .Add<TModel, TEntity>(_ => new ModelMapper<TEntity, TModel, TTranslation>());

services.AddHostLoomMapping(mapping =>
{
    AddEntityMap<SportEntity, Sport, SportTranslation>(mapping);
    AddEntityMap<TeamEntity, Team, TeamTranslation>(mapping);
});
```

Every type argument stays visible to the compiler, so this needs no `MakeGenericType` and keeps the
trimming and Native AOT analyzers clean. Each call still produces one closed descriptor, so the
registered pairs remain enumerable and duplicate detection still applies. The factory also takes
the `IServiceProvider`, so a generic map can resolve dependencies like any other.

Maps are transient by default so constructor-injected scoped dependencies remain safe. Choose a
different `ServiceLifetime` only when its dependency graph supports that lifetime. A singleton map
instance can also be registered explicitly. Container-created map classes expose a public
constructor. Duplicate type pairs fail during registration — across both overloads, since the
inferred pair is the same closed service type — and a missing pair throws
`MappingNotFoundException` with both requested types.

## Sequences and null

A map handles one value. Sequences are extension methods on the closed mapper, and the null policy
is in the method name rather than in configuration:

```csharp
IReadOnlyList<CustomerDto> all   = customerMapper.MapMany(customers);          // null source throws
IReadOnlyList<CustomerDto> safe  = customerMapper.MapManyOrEmpty(maybeNull);   // null source -> []
IEnumerable<CustomerDto>   lazy  = customerMapper.MapManyDeferred(hugeScan);   // one at a time
CustomerDto?               maybe = customerMapper.MapOrNull(maybeNullCustomer);
```

`MapMany` and `MapManyOrEmpty` return `IReadOnlyList<TDestination>` and are sized in one allocation
when the source can report a count. `IReadOnlyList<T>` rather than `List<T>` keeps the signature
clear of CA1002; the knock-on in a repository running all analyzers is that `.First()` on the
result then trips CA1826, so index it with `[0]`. `MapManyDeferred` validates its arguments eagerly but maps
lazily — do not let its result outlive the scope that resolved the mapper, or a map class holding
scoped dependencies will be enumerated after they are disposed.

### Migrating from a convention mapper

AutoMapper's null handling differs from these contracts in three places, all verified against
AutoMapper 14 rather than assumed:

| Source                          | AutoMapper (default)   | HostLoom `Map` / `MapMany` | Behaviour-preserving |
| ------------------------------- | ---------------------- | -------------------------- | -------------------- |
| null scalar                     | `null`                 | `ArgumentNullException`    | `MapOrNull`          |
| null collection                 | **empty** collection   | `ArgumentNullException`    | `MapManyOrEmpty`     |
| null collection *member*        | **empty** collection   | whatever the map writes    | `MapManyOrEmpty`     |

The third row is the one that surprises. `AllowNullCollections = false` is not only a top-level
rule — it also rewrites null collection members to empty during ordinary object mapping. So a
destination whose collection member came back empty may have had a null source all along, and a
hand-written map that forwards the null is a behaviour change even though no call site changed.

Translate to the `OrEmpty` and `OrNull` forms first, which preserves behaviour exactly, then treat
each one as its own decision. Because the tolerance is named at the call site rather than set
globally, every place that depends on it stays greppable — which a configuration flag does not.

## Design rules

- Use different destination types for semantically different views instead of selecting a hidden
  named profile for the same pair. The *pair* is the key, so several sources mapping to one shared
  destination is fine and expected — what this rule forbids is one pair with two meanings.
- When a source member is nullable and the destination contract is not, forward the null with `!`
  and record the mismatch as its own change. Substituting a default inside an unrelated refactor
  buries a contract defect in a diff nobody is reviewing for it.
- Keep mapping synchronous and deterministic. Fetch or enrich data outside the map.
- Inject a closed `IMapper<TSource, TDestination>` into focused consumers. Use `IMapper` only when
  coordinating multiple pairs.
- Write database projections directly as `IQueryable.Select` expressions so the provider sees the
  complete expression tree; do not materialize records merely to pass them through a mapper.
- Treat null as an application decision. The contracts require non-null inputs; represent a
  nullable result explicitly in the destination model when that is meaningful.
- Prefer immutable destination records and constructor mapping. Required members and constructors
  make incomplete mappings compile-time failures.

Both packages target .NET 10, enable the SDK's Native AOT and trimming analyzers, and keep the map
dispatch path free of reflection and dynamic code generation.
