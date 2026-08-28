# HostLoom.Mapping.DependencyInjection

This package registers explicit `HostLoom.Mapping` map classes with
`Microsoft.Extensions.DependencyInjection`:

```csharp
services.AddHostLoomMapping(mapping =>
    mapping.Add<CustomerMapper>());
```

The pair is inferred from the single closed `IMapper<TSource, TDestination>` the map class
implements, so the registration names only the class and the file needs no `using` for the mapped
contracts. `Add<TSource, TDestination, TMapper>()` states the pair explicitly, which is required
for a class implementing several pairs and for closing an open generic map.

Each `IMapper<TSource, TDestination>` is transient by default. The non-generic `IMapper`
dispatcher is scoped and resolves closed map contracts from the current scope, so it must be
resolved from a scope rather than the root provider and cannot be injected into a singleton —
inject a closed `IMapper<TSource, TDestination>` there instead. Registration uses closed generic
service descriptors only: there is no assembly scanning, runtime code generation, or
reflection-based map dispatch.

See the `HostLoom.Mapping` package README for the full API and design guidance.
