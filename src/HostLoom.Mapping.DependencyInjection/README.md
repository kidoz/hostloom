# HostLoom.Mapping.DependencyInjection

This package registers explicit `HostLoom.Mapping` map classes with
`Microsoft.Extensions.DependencyInjection`:

```csharp
services.AddHostLoomMapping(mapping =>
    mapping.Add<Customer, CustomerDto, CustomerMapper>());
```

Each `IMapper<TSource, TDestination>` is transient by default. The non-generic `IMapper`
dispatcher is scoped and resolves closed map contracts from the current scope, so it must be
resolved from a scope rather than the root provider and cannot be injected into a singleton —
inject a closed `IMapper<TSource, TDestination>` there instead. Registration uses closed generic
service descriptors only: there is no assembly scanning, runtime code generation, or
reflection-based map dispatch.

See the `HostLoom.Mapping` package README for the full API and design guidance.
