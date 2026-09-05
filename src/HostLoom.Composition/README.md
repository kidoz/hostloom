# HostLoom.Composition

An explicit dependency-injection plan can be inspected before a provider exists, applied to a
service collection once, and reported without executing factories or constructors.

`HostLoom.Composition` bundles its compile-time generator and declaration-use analyzer in the same
NuGet. Applications need one HostLoom package reference; the sole runtime dependency is
`Microsoft.Extensions.DependencyInjection.Abstractions`. The compiler assembly is an analyzer asset,
not a runtime dependency. There is no runtime assembly scanning. Package consumption and Native AOT
are verified. The [performance reference](../../docs/reference/composition-performance.md) publishes
phased measurements and numeric budgets; external consumer migration evidence remains separate.
The generator's supported syntax and diagnostics are documented in its
[reference](https://github.com/kidoz/hostloom/tree/main/src/HostLoom.Composition.Generators).

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
identical factory references, known alias targets or identical instance references are errors. Distinct factory bodies
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

## Generated plans

For a packaged consumer, reference only `HostLoom.Composition`; the package provides the generator
and project-directory compiler property automatically. A provider-building application supplies its
usual DI implementation/host independently. Within this repository, development project references
still name the generator explicitly:

```xml
<ProjectReference Include="../../src/HostLoom.Composition/HostLoom.Composition.csproj" />
<ProjectReference Include="../../src/HostLoom.Composition.Generators/HostLoom.Composition.Generators.csproj"
                  OutputItemType="Analyzer" ReferenceOutputAssembly="false" />
```

Declare a factory and its rules together in a non-generic partial class:

```csharp
internal static partial class CatalogComposition
{
    [CompositionRules(nameof(CreatePlan))]
    private static void Declare(CompositionRuleBuilder rules)
    {
        rules.Group("catalog", group =>
        {
            group.AddClasses()
                .AssignableTo(typeof(ICatalogConverter<>))
                .AsImplementedInterfaces()
                .WithScopedLifetime()
                .ExpectOne();
        });
    }

    public static partial CompositionPlan CreatePlan();
}
```

`ICatalogConverter<>` and its implementations belong to the application. The generator traverses
inherited interfaces, projects the matching closed interfaces, and emits explicit DI descriptors.
Abstract/open classes remain valid inheritance intermediates but are not registered. Public and
internal implementations accessible from the factory are supported and require public constructors.
Call `CatalogComposition.CreatePlan().ApplyTo(services)` from the composition root. The `Declare`
method is declaration-only: executing it or capturing it as a delegate is a compile error when the
analyzer is present; DSL members also throw if executed without the generator/analyzer.

The generator supports groups, explicit types, bounded discovery, inherited attribute filters,
namespace guards, matched/all/explicit projections, self aliases, lifetimes, cardinality, count
assertions, strategies and positional open-generic registration. For example:

```csharp
rules.AddClasses()
    .AssignableTo<ICatalog>()
    .WithAttribute<CatalogComponentAttribute>()
    .RequireNamespace("Catalog.Services")
    .AsSelfWithInterfaces()
    .WithScopedLifetime()
    .ExpectMany()
    .ExpectAtLeast(1);

rules.AddOpenGeneric(typeof(IRepository<>), typeof(Repository<>))
    .WithScopedLifetime()
    .ExpectOne();
```

Types and markers in these snippets belong to the application. Namespace guards fail on misplaced
matches; they do not filter them out. Counts apply to distinct eligible implementations before
projection. Open generics require equal arity, positional mapping and supported compatible constraints.
The generator reports proven singleton capture through known constructor paths; unknown dependencies
and uncertain constructor choices still require final-provider checks.

Self aliases forward to the self descriptor. Singleton/scoped identity follows provider/scope
boundaries; transient alias resolutions each create a fresh object. The container may capture and
dispose self and aliases repeatedly, so disposal must be idempotent. `AliasTargetType` exposes the
known target and `ImplementationType` exposes either a type descriptor's implementation or an alias
target without executing it. Explicit factory callers can supply `aliasTargetType` only when their
factory forwards to that self type; this metadata is a caller assertion, not runtime introspection.

Origins include normalized selector text. Rejections retain ordered reasons and a stable identity;
`CandidateType` is null when a rejected source type cannot be referenced by the generated factory.
The NuGet supplies `MSBuildProjectDirectory` through build-transitive props. When using development
project references, add `<CompilerVisibleProperty Include="MSBuildProjectDirectory" />` to an
`ItemGroup` for project-relative paths. Linked files outside the root and hosts without this property
use a filename fallback for absolute paths. No absolute checkout paths enter generated code.

The AOT sample executes generated aliases, open generics and synchronous/asynchronous disposal.
Generated source can be inspected with `EmitCompilerGeneratedFiles=true`; direct its output under
`obj` to avoid compiling persisted output twice. The package verifier checks isolated consumers,
bundled diagnostics, relative origins, dependency boundaries and optional native execution. See the [generator reference](../HostLoom.Composition.Generators/README.md) for
supported shapes, diagnostics and validation limits.


## Testing and optional ledger integration

`HostLoom.Composition.Testing` provides test-framework-independent, container-free assertions for
registration multisets, sequences, matched types, service/lifetime/cardinality, origins and rejection
reasons. It normalizes known aliases without comparing delegates; opaque activations need a caller's
reviewed semantic identity. See the [testing reference](../HostLoom.Composition.Testing/README.md).

An [application-owned ledger example](../../examples/HostLoom.Examples.CompositionDiagnostics/README.md)
aggregates each service's ordered implementation/lifetime list into one choice, preserving skips,
replacements and rejected candidates without false enumerable conflicts. Neither composition package
references diagnostics, and applying a plan never writes a ledger automatically.

Verify local packages without publishing:

```sh
python3 eng/verify-composition-package.py
# On a matching local host, also publish and execute runtime and testing AOT consumers:
python3 eng/verify-composition-package.py --runtime osx-arm64
```

The verifier retains packages, fresh consumer sources and command logs in an OS temporary directory.
`--packages <directory> --version <version>` verifies already packed artifacts, as the release
workflow does. No NuGet publication is performed by this check.
