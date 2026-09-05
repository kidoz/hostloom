# Migrate registration discovery to composition plans

Use this procedure when replacing application-owned reflection or Scrutor discovery. Keep the
existing registration behavior as the acceptance contract until the replacement passes it.
The [composition reference](../reference/composition.md) lists supported rules and restrictions.

## Pin the current inventory

Before changing discovery, run the current composition root against a fresh `ServiceCollection`
with its real registration order and configuration. Record the exact service, implementation and
lifetime multiset, including duplicate multiplicity. Separately pin the sequence for enumerable
services and any registration where last-registration resolution matters. Check these records
into the consumer's tests and review them before introducing the generated rules.

Do not replace the expected inventory with whatever the new generator emits. Counts are useful
additional checks, but the same count can conceal a missing type and an unintended replacement.
Partition discovery-owned entries from unrelated host registrations explicitly. Keep keyed
registrations and opaque factories in separate tests unless their semantics have been reviewed.

## Express the narrowest rules

Add `HostLoom.Composition` to the application and `HostLoom.Composition.Testing` to its test project.
The application NuGet already includes the generator; no separate generator package is needed.
Move one registration family at a time into a non-generic partial composition class:

```csharp
[CompositionRules(nameof(CreatePlan))]
private static void Declare(CompositionRuleBuilder rules)
{
    rules.AddClasses()
        .AssignableTo(typeof(ICatalogConverter<>))
        .RequireNamespace("Catalog.Services")
        .AsImplementedInterfaces()
        .WithScopedLifetime()
        .ExpectOne()
        .ExpectExactly(2);
}

public static partial CompositionPlan CreatePlan();
```

The sample count is illustrative; replace it with the independently audited consumer count.
Inherited closed interfaces are discovered through abstract or open base classes. Only eligible
concrete closed implementations are registered. Referenced-assembly implementations need explicit
`AddTypes`; another generator's output cannot be discovered. `RequireNamespace` detects a misplaced
match rather than silently filtering it away. Use a positive marker or assignability to bound
`AddClasses`; `AllowEmpty` is an explicit exception and never disables count assertions.

Select lifetime and cardinality per service. `AsImplementedInterfaces` uses the matching interfaces;
`AsAllImplementedInterfaces` opts into incidental interfaces. `AsSelfWithInterfaces` changes
activation to forwarding aliases and may change identity and disposal behavior, so introduce it
only if that behavior matches the pinned contract. Group names organize provenance and are not
container scopes.

## Compare semantics and ordering independently

For ordinary unkeyed type descriptors, use the old discovery function in the characterization test:

```csharp
var oldServices = new ServiceCollection();
RegisterExistingCatalogServices(oldServices); // The existing consumer implementation.
var expected = oldServices
    .Select(descriptor => CompositionRegistrationShape.FromDescriptor(descriptor))
    .ToArray();

var plan = CatalogComposition.CreatePlan();
var actual = CompositionRegistrationShape.Project(plan.Probe());
CompositionAssert.EquivalentRegistrations(expected, actual);
CompositionAssert.RegistrationSequence(expected, actual); // If order is contractual.
```

Keep the independently pinned expected descriptors alongside this comparison; otherwise both
implementations could drift together. `FromDescriptor` rejects opaque activations without an
explicit semantic identity. For a known forwarding alias, supply its reviewed self type as
`aliasTargetType`. For an ordinary factory or instance, supply a test-owned `opaqueIdentity`
describing its behavior. Never derive that identity by executing a factory, comparing delegate
objects or taking the generated result as its own expectation.

Assert matched types, service lifetime/cardinality, origins and rejection reasons separately when
those are contractual. Generated provenance and intentional rejection inventories need not equal
the old scanner's metadata. Review any ordering change explicitly; a multiset pass does not authorize
it. Discovery uses stable type ordering, which can differ from an old assembly scanner's order.

## Validate the complete provider

Apply the plan at the old discovery call's position and run the consumer's full startup tests.
Enable `ValidateOnBuild` and `ValidateScopes` on the final provider, then resolve and exercise the
application's actual services in the appropriate scopes. Check factory paths, lazy dependencies,
enumerables and disposal. Build-time HLM0016 diagnoses only proven capture in known plan paths;
unknown dependencies and uncertain constructor choices remain runtime acceptance work.

For open generics, separately pin every closed construction the application uses before replacing
registrations with `AddOpenGeneric`. Resolve each construction and exercise its dependencies,
including lazy converter paths. Matching generic arity and constraints cannot prove all future
closed services valid. If the consumer ships Native AOT, publish and execute those same known
constructions and alias/disposal paths in its native application.

## Remove obsolete discovery dependencies

After the unchanged pinned assertions and full provider tests pass, remove the old scanner call
and its now-unused package/reference paths. Inspect the resolved package graph to ensure Scrutor
or other discovery libraries have not remained through another dependency. Record the exact SDK,
commands, inventory and tests used to accept the migration, and compare startup cost using the
[performance methodology](../reference/composition-performance.md).

The repository's neutral catalog measurements, generator tests and packed AOT consumers establish
framework behavior. They are not evidence that a separate application's registrations have been
migrated or its independently audited inventory still resolves.
