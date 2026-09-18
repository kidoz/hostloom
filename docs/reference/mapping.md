# Mapping

The `HostLoom.Mapping` family: explicit, compile-time-safe object
mapping. The core package is dependency-free; all three packages enable
the Native AOT and trimming analyzers. Namespaces follow the package
names.

```text
dotnet add package HostLoom.Mapping
dotnet add package HostLoom.Mapping.DependencyInjection   # container registration
dotnet add package HostLoom.Mapping.Testing               # container-free composition
```

## Contracts

| Type | Members |
| --- | --- |
| `IMapper<in TSource, out TDestination>` | `TDestination Map(TSource source)` — implement this per pair; constraints `TSource : notnull`, `TDestination : notnull` |
| `IUpdateMapper<in TSource, in TDestination>` | `void MapInto(TSource source, TDestination destination)` — writes into the supplied instance and never replaces it; constraints `TSource : notnull`, `TDestination : class`; not reachable through the dispatcher |
| `IMapper` (dispatcher) | `TDestination Map<TSource, TDestination>(TSource source)` — creation maps only |
| `MappingSource<TSource>` | returned by `From`; `To<TDestination>()` completes the fluent call |

The fluent shape `mapper.From(customer).To<CustomerDto>()` comes from the
`MapperExtensions.From` extension on the dispatcher.

An update map is a partial write by design: members it does not assign keep their values, the
caller keeps the identity it already holds (an entity tracked by a persistence context, for
example), and collection behavior — replace, merge, or leave alone — is chosen in the map body.
Both arguments are non-null by contract; an implementation throws `ArgumentNullException` rather
than treating a null as "nothing to update". `HLM0004`/`HLM0005` do not inspect `MapInto`
([boundaries](analyzer-rules.md#mapping-completeness-boundaries)).

## Sequence and null extensions (`MapperExtensions`)

The name carries the null policy:

| Method | Null source | Returns |
| --- | --- | --- |
| `MapMany(IEnumerable<TSource>)` | rejected | `IReadOnlyList<TDestination>` |
| `MapManyOrEmpty(IEnumerable<TSource>?)` | treated as empty | `IReadOnlyList<TDestination>` |
| `MapManyDeferred(IEnumerable<TSource>)` | rejected | lazy `IEnumerable<TDestination>` |
| `MapOrNull(TSource?)` | maps to null | `TDestination?`; constrained `class`/`class`, unlike the others (`notnull`) |

## Registration (DependencyInjection)

```csharp
IServiceCollection AddHostLoomMapping(Action<MappingBuilder> configure,
    ServiceLifetime dispatcherLifetime = ServiceLifetime.Scoped);
MappingBuilder AddHostLoomMapping(
    ServiceLifetime dispatcherLifetime = ServiceLifetime.Scoped);
MappedPairRegistry GetMappedPairs(this IServiceCollection services);
```

The `IMapper` dispatcher defaults to **scoped** (capturing it in a
singleton is the `HLM0006` [analyzer rule](analyzer-rules.md)).

Repeated `AddHostLoomMapping` calls retain the existing unkeyed dispatcher. A later
`dispatcherLifetime` argument does not replace it: new maps are checked against the lifetime of
the last registered unkeyed dispatcher. For example, after registering a singleton dispatcher,
a subsequent call with default arguments still rejects a transient map. If the retained dispatcher
is scoped, a later request for singleton does not make it singleton. A singleton dispatcher
requires singleton maps whose dependency graphs are safe to resolve from the root provider.

`MappingBuilder.Add` overloads:

| Overload | Lifetime |
| --- | --- |
| `Add<TMapper>(ServiceLifetime = Transient)` — pair inferred from the one `IMapper<,>` the class implements | transient by default |
| `Add<TSource, TDestination, TMapper>(ServiceLifetime = Transient)` — explicit pair | transient by default |
| `Add<TSource, TDestination>(Func<IServiceProvider, IMapper<TSource, TDestination>> factory, ServiceLifetime = Transient)` — closes generic map classes; one class, many pairs | transient by default |
| `Add<TSource, TDestination>(IMapper<TSource, TDestination> mapper)` — instance | always singleton |

Registration throws `InvalidOperationException` when: the pair is already
registered; the map class implements zero or more than one closed
`IMapper<,>`; the dispatcher is singleton but a map is not; a factory
returns null at resolve time.

`MappingBuilder.AddUpdate` overloads register an `IUpdateMapper<TSource, TDestination>` with the
same four shapes — `AddUpdate<TMapper>()` inferring the pair from the one `IUpdateMapper<,>` the
class implements, `AddUpdate<TSource, TDestination, TMapper>()`, a factory, and an instance — and
the same defaults. Each call registers exactly one contract: a class implementing both
`IMapper<,>` and `IUpdateMapper<,>` for a pair goes through `Add` and `AddUpdate`. A creation
map and an update map for one pair coexist; duplicates are rejected per contract. The dispatcher
never resolves an update map, so the singleton-dispatcher rule does not constrain one.

`MappedPairRegistry` (singleton) exposes `Pairs` (creation maps, registration order),
`UpdatePairs`, `DestinationsFor(Type)` (creation maps), `Contains(source, destination)`, and
`ContainsUpdate(source, destination)` — useful for architecture tests. The two lists are kept
apart so `MappingNotFoundException` never suggests an update map the dispatcher cannot reach.

## Exceptions and attributes

| Type | Purpose |
| --- | --- |
| `MappingNotFoundException` (`: InvalidOperationException`) | unknown pair at dispatch; `SourceType`, `DestinationType`, `RegisteredDestinations` — what the source *is* registered to map to |
| `UnmappedMembersAttribute(params string[] members)` | on a map class, declares destination members intentionally left unassigned, for the `HLM0004` analyzer |

## Testing

`TestMapperBuilder` composes a dispatcher without a container:

```csharp
var mapper = new TestMapperBuilder()
    .Add<Customer, CustomerDto>(c => new CustomerDto(c.Id, c.Name))
    .Build();
```

`Add` accepts an `IMapper<TSource, TDestination>` instance or a plain
`Func<TSource, TDestination>`; duplicate pairs throw, and the built
dispatcher throws `MappingNotFoundException` for unknown pairs.

A consumer taking a closed `IUpdateMapper<TSource, TDestination>` needs nothing from this
package: construct the update map class and pass it.

## Benchmarks

Mapping comparisons and implementation strategy measurements live in the separate, non-packable
`benchmarks/HostLoom.Mapping.Benchmarks` project. From the repository root:

```sh
dotnet run --project benchmarks/HostLoom.Mapping.Benchmarks -c Release -- --filter "*"
dotnet run --project benchmarks/HostLoom.Mapping.Benchmarks -c Release -- --job Dry --filter "*"
```

The first command measures steady-state mapping, collections, resolution, registration and startup,
plus strategy cases. The dry run checks setup and execution only; its timings are not a baseline.
`just benchmark-mapping-check` runs the flat and nested maps, collection mapping, `MapMany`
strategies, and lifetime suites as a short job and fails when a mean or an allocation regresses
more than 10 % against `benchmarks/baselines/mapping.json`; `benchmark-mapping-update` rewrites
that baseline on the reference machine.
AutoMapper is a comparison dependency of this benchmark project and does not enter runtime packages.

## Limitations

- Mapping is synchronous and performs no I/O by design — fetch and enrich
  outside the map ([why](../explanation/architecture.md#explicit-over-convention)).
- No convention matching, expression compilation, or runtime code
  generation exists to configure; a map does exactly what its class body
  says.
- An update map has no automatic merging or implicit collection
  replacement; what it leaves alone and how it treats collections is
  written in the map body and stated in its documentation.
