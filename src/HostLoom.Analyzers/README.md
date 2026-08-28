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

### HLM0004

Assign every settable member of a mapped destination. An explicit map makes a forgotten member a
silent data loss rather than a compile error — nothing requires the member to be written — which is
the one axis on which explicit mapping is less safe than the convention mapping it replaces. A
member supplied through the destination's constructor counts as assigned, so positional records and
contracts with real constructors need nothing extra.

Omitting a member is often correct. Name each one, so that a member added to the contract later is
still reported rather than excused by a blanket marker:

```csharp
[UnmappedMembers(nameof(CfaTransfer.CardMask), nameof(CfaTransfer.ProviderId))]
public sealed class TransferModelToCfaTransferMapper : IMapper<TransferModel, CfaTransfer>
```

### HLM0005

Keep a `Map` body in a shape completeness can be verified in. Two are recognised:

- the destination constructed and returned directly, with an object initializer;
- one local constructed, assigned into across any number of statements and branches, and returned.

A body outside both — the local passed to a method, built across two locals, or returned from
somewhere else — is reported rather than skipped, so that "not checked" is never mistaken for
"checked and complete". Conditional assignment counts as assigned: the rule targets forgotten
members, and evidence that the author considered one is the whole signal.

**Known blind spot.** A map whose destination is a type parameter — a generic map class closed
through `MappingBuilder.Add<TSource, TDestination>(factory)` — cannot have its members enumerated,
so it is skipped silently and reports neither rule. Completeness of a generic map is not checked.

## Configuration

All rules are warnings by default and use the `HLM` diagnostic prefix. Standard `.editorconfig`
configuration can change a rule's severity:

```ini
dotnet_diagnostic.HLM0001.severity = error
```

Generated code is ignored, and analyzer execution is safe for concurrent compiler execution.
