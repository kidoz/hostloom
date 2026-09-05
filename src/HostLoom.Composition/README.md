# HostLoom.Composition

An explicit dependency-injection plan can be inspected before a provider exists, applied to a
service collection once, and reported without executing factories or constructors.

This is the runtime foundation of the composition feature. Compile-time rule declarations and
the bundled source generator are not implemented yet. There is no assembly discovery API.
Use a project reference during development; package packing is disabled until the generator and
packed-consumer verification are complete.

```csharp
using HostLoom.Composition;
using Microsoft.Extensions.DependencyInjection;

var origin = new CompositionOrigin("DeclareCatalog", "catalog");
var plan = new CompositionPlan("CatalogApplication.CatalogComposition.CreatePlan", [
    new CompositionRegistration(
        ServiceDescriptor.Scoped<ICatalog, Catalog>(),
        CompositionCardinality.One,
        origin)
]);

CompositionPlanProbe intended = plan.Probe();
CompositionApplicationReport applied = plan.ApplyTo(services);
// Or services.AddHostLoomComposition(plan) when only fluent collection chaining is needed.
```

The application supplies its `ICatalog` and `Catalog` types. Keep plan identities stable across
fresh factory results; use an assembly/type/factory identity to avoid accidental collisions.
Registration order is explicit and preserved. Plans, rejection reasons and report collections
are immutable snapshots. A report records application actions, not a live inventory after later
external changes to the collection.

`One` requires one unkeyed descriptor per service. Its default strategy rejects existing
registrations. `Many` appends distinct activations of one lifetime; duplicate implementation types,
identical factory references or identical instance references are errors. Distinct factory bodies
are opaque: planning never executes them to decide whether they create the same implementation.

| Strategy | Collision behavior |
|---|---|
| Default | Throw for One; append distinct activations for Many |
| Append | Append, then enforce cardinality, duplicate and lifetime invariants |
| Skip | Keep existing service descriptors and report the incoming entry skipped |
| Throw | Reject an existing descriptor of the service |
| Replace + ServiceType | Remove unkeyed descriptors of the service, then append |
| Replace + ImplementationType | Remove unkeyed type descriptors of the implementation across services, then append |
| Replace + All | Remove the union of both predicates, then append |

Replacement never infers implementation types from opaque factories or prebuilt instances.
Existing keyed descriptors remain untouched and do not satisfy unkeyed cardinality. A plan's own
conflicts cannot be hidden by a skip/replace policy. Known cardinalities from prior plans remain
binding while their descriptors are present; implementation replacement cannot delete the only
implementation of a known One service.

Application validates a temporary descriptor list before mutating the collection. A validation
failure leaves the collection unchanged and permits retry after correcting the conflict. A
successful application consumes the identity for that collection, including when entries were
skipped. Applying to another collection is supported. No hidden DI services are added.
Composition is single-threaded, as required by `IServiceCollection`; custom collection exceptions
during mutation and later external mutations are outside the atomic-validation guarantee.

`CompositionValidationException` identifies the phase, plan and available rule origins; external
descriptors are described by collection index, lifetime and known implementation type. Explicit
plan construction does not validate assignability or constructor dependency graphs by reflection.
After final registration, use provider/scope validation and separately exercise known closed
open-generic services and factory paths. Merely applying a plan does not prove resolvability.

`HostLoom.Diagnostics` is optional and unreferenced. Applications may record probe output in their
ledger explicitly. Aggregate the ordered implementation/lifetime set per service and qualify it
with plan/group identity; recording every enumerable implementation as a different choice for one
ledger component would produce false conflict warnings. Keep rejected candidates separate, keyed
by rule/type identity, with their reasons.

The [composition AOT example](../../examples/HostLoom.Examples.CompositionAot/Program.cs) exercises
explicit plans, scoped factory aliases, known closed open-generic resolution and disposal.
Factories forwarding to a scoped self-registration preserve scope identity; forwarding to a
transient registration creates a fresh instance on each resolution. Container disposal can capture
a disposable instance through multiple aliases; implementations should dispose idempotently.
