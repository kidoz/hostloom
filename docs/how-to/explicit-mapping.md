# Map objects explicitly

Register and use a compile-time-safe object map with `HostLoom.Mapping` —
no assembly scanning, no reflection-based member matching, Native AOT
compatible.

## Before you begin

Two types to map between, in any .NET application with dependency
injection. The mapping packages have no dependency on HostLoom messaging.

## 1. Install the packages

```text
dotnet add package HostLoom.Mapping
dotnet add package HostLoom.Mapping.DependencyInjection
```

## 2. Write a map

A map is an ordinary class implementing `IMapper<TSource, TDestination>`:

```csharp
using HostLoom.Mapping;

public sealed class CustomerMapper : IMapper<Customer, CustomerDto>
{
    public CustomerDto Map(Customer source) => new(source.Id, source.Name.Trim());
}
```

## 3. Register it

```csharp
using HostLoom.Mapping.DependencyInjection;

builder.Services.AddHostLoomMapping(mapping =>
    mapping.Add<CustomerMapper>());
```

The pair is read from the interface the map class already declares, so
the registration restates nothing. A generic map class is closed through
a factory overload, which is how one class registers many pairs.

## 4. Consume it

Inject `IMapper<Customer, CustomerDto>` where one pair is needed — the
fastest shape. Orchestration code coordinating several pairs can inject
the scoped `IMapper` dispatcher:

```csharp
var dto = mapper.From(customer).To<CustomerDto>();
```

For sequences and nulls, pick the extension whose name carries the
policy: `MapMany` (null source rejected), `MapManyOrEmpty` (null source
treated as empty), `MapOrNull` (null maps to null). Dictionaries need no
extension: copy with an explicit comparer, and decide null at the call
site ([recipe](../reference/mapping.md#dictionaries)).

## 5. Update an existing object

When the caller already holds the destination — an entity a persistence
context is tracking — a creation map would replace it and drop every
member the source does not carry. Write an update map instead:

```csharp
public sealed class CustomerUpdateMapper : IUpdateMapper<CustomerUpdate, Customer>
{
    // Id and CreatedAt are outside the update contract and keep their values.
    public void MapInto(CustomerUpdate source, Customer destination)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(destination);
        destination.Name = source.Name.Trim();
        destination.Region = source.Region;
    }
}

builder.Services.AddHostLoomMapping(mapping =>
    mapping.Add<CustomerMapper>().AddUpdate<CustomerUpdateMapper>());
```

Inject `IUpdateMapper<CustomerUpdate, Customer>` and call `MapInto`; the
instance you passed is the instance that changed. A creation map and an
update map for the same pair are separate services and coexist. The
analyzers do not check an update map for completeness, because leaving
members alone is the point — say which ones in the class documentation
and pin them with a test.

## 6. Verify

Add `HostLoom.Analyzers` to the project: `HLM0004` flags a destination
member the map never assigns and `HLM0005` a map body it cannot verify —
at compile time, before a forgotten member ships as data loss. For unit
tests, `HostLoom.Mapping.Testing` composes mappers without a container.

## Troubleshoot

- **`MappingNotFoundException`** — the pair is not registered; the
  exception names both types and what the source *is* registered to map
  to.
- **Registration throws on a duplicate pair** — two map classes declare
  the same source/destination pair; remove one. A creation map and an
  update map for one pair do not collide; they are different contracts.
- **`HLM0006` warning** — the scoped `IMapper` dispatcher is being
  captured in a singleton; inject the specific `IMapper<TSource, TDestination>`
  there instead.

## Related

- Full API surface, lifetimes, and extension methods:
  [mapping reference](../reference/mapping.md).
- Why mapping is explicit, synchronous, and I/O-free:
  [architecture](../explanation/architecture.md#explicit-over-convention).
