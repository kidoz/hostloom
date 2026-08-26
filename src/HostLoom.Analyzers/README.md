# HostLoom.Analyzers

Roslyn analyzers for correct asynchronous and dependency-injection usage of HostLoom. The package
contains compiler analyzers only: it adds no runtime assembly or dependency to the application.

```text
dotnet add package HostLoom.Analyzers
```

For central package management:

```xml
<PackageReference Include="HostLoom.Analyzers" PrivateAssets="all" />
```

## Rules

### HLM0001

Pass an available cancellation token to HostLoom asynchronous calls. The analyzer recognizes both
a `CancellationToken` parameter and an `IPipeContext.CancellationToken` available from an enclosing
pipeline filter or helper.

```csharp
await client.GetResponseAsync(
    "orders",
    request,
    cancellationToken: cancellationToken
);
```

### HLM0002

Await HostLoom asynchronous operations instead of blocking through `.Result`, `.Wait()`, or
`.GetAwaiter().GetResult()`. The rule also recognizes `.AsTask()` over a HostLoom `ValueTask`.

### HLM0003

Register implementations of `IRequestHandler<,>`, `IEventHandler<>`, and `IRequestBehavior<,>` as
scoped services. HostLoom creates a dependency-injection scope per delivery; a singleton handler or
behavior can share mutable state between deliveries or capture scoped dependencies. `AddHandler`,
`AddSubscriber`, and `AddBehavior` already apply the correct scoped lifetime.

## Configuration

All rules are warnings by default and use the `HLM` diagnostic prefix. Standard `.editorconfig`
configuration can change a rule's severity:

```ini
dotnet_diagnostic.HLM0001.severity = error
```

Generated code is ignored, and analyzer execution is safe for concurrent compiler execution.
