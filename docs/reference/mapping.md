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
| `IMapper` (dispatcher) | `TDestination Map<TSource, TDestination>(TSource source)` |
| `MappingSource<TSource>` | returned by `From`; `To<TDestination>()` completes the fluent call |

The fluent shape `mapper.From(customer).To<CustomerDto>()` comes from the
`MapperExtensions.From` extension on the dispatcher.

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

`MappedPairRegistry` (singleton) exposes `Pairs` (registration order),
`DestinationsFor(Type)`, and `Contains(source, destination)` — useful for
architecture tests.

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

## Limitations

- Mapping is synchronous and performs no I/O by design — fetch and enrich
  outside the map ([why](../explanation/architecture.md#explicit-over-convention)).
- No convention matching, expression compilation, or runtime code
  generation exists to configure; a map does exactly what its class body
  says.
