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
    mapping.Add<Customer, CustomerDto, CustomerMapper>());

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

Maps are transient by default so constructor-injected scoped dependencies remain safe. Choose a
different `ServiceLifetime` only when its dependency graph supports that lifetime. A singleton map
instance can also be registered explicitly. Container-created map classes expose a public
constructor. Duplicate type pairs fail during registration, and a missing pair throws
`MappingNotFoundException` with both requested types.

## Design rules

- Use different destination types for semantically different views instead of selecting a hidden
  named profile for the same pair.
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
