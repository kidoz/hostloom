# Composition plans

`HostLoom.Composition` turns compile-time rules into explicit `IServiceCollection` registrations.
The package bundles its incremental generator and declaration-use analyzer. Its sole runtime
package dependency is `Microsoft.Extensions.DependencyInjection.Abstractions`; applications supply
their usual container implementation or host. `HostLoom.Composition.Testing` optionally adds
container-free assertions and depends only on composition. Neither package depends on diagnostics.

## Declare and apply

Reference `HostLoom.Composition` from the application. Types in the following example belong to the
application; `Catalog` implements `ICatalog` and has a public constructor.

```csharp
using HostLoom.Composition;
using Microsoft.Extensions.DependencyInjection;

internal static partial class CatalogComposition
{
    [CompositionRules(nameof(CreatePlan))]
    private static void Declare(CompositionRuleBuilder rules)
    {
        rules.Group("catalog", group =>
        {
            group.AddTypes(typeof(Catalog))
                .As<ICatalog>()
                .WithScopedLifetime()
                .ExpectOne()
                .ExpectExactly(1);
        });
    }

    public static partial CompositionPlan CreatePlan();
}
```

At the composition root:

```csharp
CompositionPlan plan = CatalogComposition.CreatePlan();
CompositionPlanProbe intended = plan.Probe();
CompositionApplicationReport applied = plan.ApplyTo(services);
// Fluent alternative: services.AddHostLoomComposition(plan);
```

Never call `Declare`. It is compiler input; runtime calls and delegate capture cause HLM0009.
DSL methods also throw if executed without the analyzer. `Probe()` returns an immutable view
without creating a provider, executing factories or constructing services. `ApplyTo` validates
registrations and returns the actions taken; it does not prove that services can be resolved.

Explicit plans are supported without declarations:

```csharp
var plan = new CompositionPlan("CatalogApplication.CatalogComposition.CreatePlan", [
    new CompositionRegistration(
        ServiceDescriptor.Scoped<ICatalog, Catalog>(),
        CompositionCardinality.One,
        new CompositionOrigin("DeclareCatalog", "catalog"))
]);
```

Use a stable assembly/type/factory identity. A successful application consumes that identity for
one collection, even if every entry is skipped. Fresh factories with the same identity cannot
apply twice to that collection. The same plan can be applied to another collection; failed
validation permits retry after the conflict is corrected. No tracking services are added to DI.

## Declaration syntax

A declaration is a non-generic `static void` method with a block body and exactly one
`CompositionRuleBuilder` parameter. Its attribute names one unimplemented parameterless static
partial method returning `CompositionPlan` in the same type. All containing types must be
non-generic, non-file-local partial classes. Nested classes and distinct factories work; records
and structs are unsupported. Multiple declarations claiming the same factory are errors.

Arguments are positional: explicit `typeof` expressions, generic type arguments and compile-time
scalar constants. Groups accept synchronous inline lambdas with one builder parameter. Locals,
control flow, helpers, runtime arrays, captured values, nested groups, predicates and repeated
lifetime/cardinality/projection/strategy clauses are rejected. Rules never execute at runtime.
`nameof` references are allowed; invoking a rule method or capturing its delegate is an error.

| Part | Methods |
| --- | --- |
| Grouping | `Group(constantName, inlineLambda)`; one level, unique names |
| Candidates | `AddTypes(typeof(...), ...)`, `AddClasses()` |
| Open pair | `AddOpenGeneric(typeof(IService<>), typeof(Implementation<>))` |
| Assignability | `AssignableTo<T>()`, `AssignableTo(typeof(...))`, `AssignableToAny(typeof(...), ...)` |
| Attribute filters | `WithAttribute<TAttribute>()`, `WithoutAttribute<TAttribute>()` |
| Namespace guard | `RequireNamespace(constantName)` |
| Projection | `AsSelf()`, `AsImplementedInterfaces()`, `AsAllImplementedInterfaces()`, `AsSelfWithInterfaces()`, `As<T>()`, `As(typeof(...), ...)` |
| Lifetime | `WithTransientLifetime()`, `WithScopedLifetime()`, `WithSingletonLifetime()`, `WithLifetime(constant)` |
| Cardinality | `ExpectOne()`, `ExpectMany()` |
| Counts | `ExpectExactly(nonnegativeConstant)`, `ExpectAtLeast(nonnegativeConstant)` |
| Absence | `AllowEmpty()` |
| Application policy | `Append()`, `Skip()`, `Throw()`, `Replace(CompositionReplacementBehavior.ServiceType / ImplementationType / All)` |

Every rule requires one lifetime and cardinality. Candidate rules also require one projection;
`AddOpenGeneric` supplies its service directly and accepts policies/counts, without candidate
filters or another projection. All count assertions remain binding, including with `AllowEmpty`.
They count distinct eligible implementations after filters/guards, before projection and strategies.
`ExpectOne` applies separately to each service, so one class implementing two services counts once
for `ExpectExactly(1)` and can satisfy `ExpectOne` for both services.

## Selection and projection

Discovery examines source-declared classes in the current compilation. Referenced base/interface
symbols participate in matching; referenced implementations need explicit `AddTypes`. Another
source generator's output is not a discovery input. Abstract, static, open and compiler-generated
classes are excluded from discovery, while remaining inheritance intermediates. Invalid explicit
candidates are errors. Eligible implementations must be accessible from the factory and have public
constructors; selected private or file-local types cause errors instead of silently disappearing.

Successive assignability selectors intersect; arguments within `AssignableToAny` form a union.
Closed selectors match exact constructed interface/base identities. Open selectors match the
generic definition. Attribute filters are conjunctive and match derived marker attributes too.
They honor the actual marker's `AttributeUsage.Inherited` through class bases, including inherited
attribute-usage settings. Interface attributes never become class attributes. Discovery requires
assignability or a positive marker. A negative marker alone cannot bound discovery.

`RequireNamespace("Catalog.Services")` accepts that namespace and its children, using ordinal,
case-sensitive segment boundaries. An otherwise eligible match outside the namespace causes a
build error with the rule and type locations. The guard cannot silently remove registrations.

`AsImplementedInterfaces` projects only interfaces matching the assignability selectors, including
inherited closed forms. Class-only or marker-only selectors need `AsSelf`, explicit `As`, or the
explicit all-interface opt-in `AsAllImplementedInterfaces`. An open interface passed to `As`
projects all its actual implemented closed forms; every requested service must match. Projection
does not guess argument positions or opt into incidental interfaces.

`AsSelfWithInterfaces` uses the narrowed interface projection plus one self descriptor. Generated
interface factories forward to self. Singleton aliases share one object per provider; scoped
aliases share one per scope; transient resolutions create fresh objects. A transient alias creates
its self object on each resolution. Only closed implementations support aliases. The probe exposes
`AliasTargetType` and `ImplementationType` without executing factories; known alias targets also
participate in duplicate detection across applied plans. Ordinary factories/instances stay opaque.

The built-in container can capture one disposable object through self and each resolved alias.
Scoped/singleton aliases can therefore dispose the same object repeatedly; a transient alias may
capture its newly created object twice. Implement disposal idempotently. Tests cover both disposal
interfaces and provider/scope ownership. This follows the container's
[scope capture and disposal implementation](https://github.com/dotnet/runtime/blob/v10.0.0/src/libraries/Microsoft.Extensions.DependencyInjection/src/ServiceLookup/ServiceProviderEngineScope.cs).

## Open generics and capture checks

Open registration supports top-level service/implementation definitions of equal arity. The
implementation must be a concrete accessible class with a public constructor, implementing the
service through its own type parameters in the same positions, including inherited mappings.
Permutations, fixed/nested argument mappings and nested generic definitions are diagnosed.

Implementation constraints cannot be stricter than service constraints. The supported conservative
subset covers matching class/struct/unmanaged/notnull/new constraints and exact constraint types
after positional substitution, including parameter-to-parameter constraints. It also checks generic
parameter trimming annotations. Unsupported implication between different constraint types is an
error. This validates registration shape; applications still resolve their known closed inventory
and dependencies, including in Native AOT. It does not prove every possible closed construction.

HLM0016 follows known registrations through implementations with exactly one public constructor,
including transient intermediates, aliases, enumerable services and compatible open registrations.
Direct closed registrations precede open registrations; explicit enumerable registrations precede
automatic collection construction. Open singleton implementations can expose known capture paths.
Keyed parameters, external registrations, factory bodies, `Skip` policies and uncertain constructor
choices are unverified. Traversal stops at cycles and after a plan-sized depth bound for expanding
generic paths. No diagnostic means no capture was proved within that scope, not that the provider
is valid. Run final provider/scope validation and known closed-service resolution tests after all
application registrations.

## Policies, diagnostics and provenance

Application policies use the [application policies](#application-policies).
They never hide internal duplicates, mixed lifetimes or ambiguous `ExpectOne` candidates. Replacement
by implementation applies to type-backed descriptors; it does not inspect forwarding factories.
Policies act on the current collection in emitted order, so review their application report,
including effects of skip/replace on self registrations used by aliases.

| ID | Error |
| --- | --- |
| HLM0009 | Unsupported declaration, invalid factory pairing, or runtime declaration use |
| HLM0010 | Unbounded/empty selection, namespace violation, invalid/inaccessible candidate |
| HLM0011 | Missing/repeated lifetime or cardinality, invalid constants or repeated strategy |
| HLM0012 | Missing/incompatible projection or no public constructor |
| HLM0013 | Duplicate activation, cardinality or lifetime conflict in the plan |
| HLM0014 | Invalid or unsatisfied implementation count assertion |
| HLM0015 | Unsupported open-generic shape, constraints or trimming requirements |
| HLM0016 | Proven singleton capture of a scoped service through known plan edges |

Invalid declarations emit no factory. Diagnostics point at authored rules and include type or
conflicting-rule locations when available. Origins include rule/group, normalized selector text,
source path and line. Rejected candidates appear once per rule with ordered reasons. Their inventory
is bounded to explicit lists or assignability matches before eligibility/attribute filtering;
marker-only discovery considers classes carrying any positive marker before applying all filters.
Unrelated types never become noise in the rejection report. Inaccessible rejected types have a
stable `CandidateIdentity` with null `CandidateType`, allowing reports without invalid `typeof` code.

Groups/rules retain declaration order. Within a rule implementations sort by namespace and full
metadata name, ordinal; projected services sort by fully qualified identity. Explicit lists use the
same sort. Separate plans retain application call order. This differs from reflection enumeration.

To supply the project directory during development, add this to the consuming project:

```xml
<ItemGroup>
  <CompilerVisibleProperty Include="MSBuildProjectDirectory" />
</ItemGroup>
```

Origins normalize separators and dot segments and remove the project root. Linked files outside
that root, or absolute paths from hosts without this property, use a filename fallback. Relative
paths stay relative. No absolute checkout path or timestamp is emitted. The NuGet's build-transitive props supply the compiler property automatically. Normalization is
lexical; differently spelled symlink roots can use the filename fallback.

## Incremental and release evidence

Attribute discovery uses `ForAttributeWithMetadataName`. Semantic matching may rerun after edits;
final emitted text compares by value to reuse unchanged source. Tracked-step tests cover unrelated
body edits, inherited attributes/interfaces, accessibility and rule changes, including reversion. Path tests cover different checkout
roots and Windows/POSIX separators. Seven reviewed snapshots include aliases, generics, strategies
and rejections. Failures write received files to the OS temporary directory and never update
verified snapshots automatically.

The full solution, seven snapshots and native example cover development consumers. The package
verifier additionally builds an isolated application with one HostLoom reference and a helper-only
consumer whose generator arrives transitively. It checks dependency graphs, negative HLM0009/HLM0014
builds and optional packed-runtime/helper AOT execution. `HostLoom.Composition.Testing` supplies
separate semantic, ordering and provenance assertions. Measured build/runtime budgets are published in the
[performance reference](composition-performance.md). External consumer migration evidence remains
outstanding.
Decoration, keyed declarations, runtime scanning and standalone registration attributes are deferred.

## Application policies

`One` requires exactly one unkeyed descriptor per service. `Many` permits distinct activations
with the same lifetime, preserving descriptor order. Duplicate implementation types, known alias
targets, identical factory references and identical instance references are rejected. Different
opaque factories cannot be assumed equivalent without executing them, which composition never does.

| Policy | Existing unkeyed registration |
| --- | --- |
| Default | Throw for One; append distinct activations for Many |
| Append | Append and enforce cardinality, lifetime and duplicate invariants |
| Skip | Retain the existing descriptors; record the incoming entry as skipped |
| Throw | Fail if the service is already registered |
| Replace(ServiceType) | Remove descriptors of the service, then append |
| Replace(ImplementationType) | Remove type descriptors of that implementation across services, then append |
| Replace(All) | Remove the union of those two sets, then append |

Implementation replacement cannot infer implementations from opaque factories or instances.
Existing keyed registrations remain untouched and do not count toward unkeyed cardinality.
Conflicts inside the incoming plan cannot be concealed with Skip or Replace. Cardinalities from
previously applied plans remain binding while their descriptors are present; replacing an
implementation cannot empty a known One service.

Validation runs against a temporary descriptor list before mutation. A validation error leaves
the original collection unchanged. `IServiceCollection` composition is single-threaded; exceptions
thrown by a custom collection while mutating it are outside this guarantee. Later external
mutations are outside the report's snapshot. `CompositionValidationException` includes phase,
plan identity, known origins and external descriptor index/lifetime/implementation details.

## Reports, provenance and tests

A `CompositionPlanProbe` contains ordered registrations and rejected candidates.
`CompositionApplicationReport` records additions, skips and replacements against that collection.
It is an application history, not a live provider inventory. Origins carry declaration/group,
normalized selector and authored source location; inaccessible rejected types have a stable
candidate identity with a null `CandidateType`. Rejection reasons retain their order.

The optional testing package exposes `CompositionRegistrationShape.Project(probe)` and
`FromDescriptor(descriptor, aliasTargetType, opaqueIdentity)`. Shapes compare service,
implementation, lifetime, activation kind, alias target and explicit opaque identity. They exclude
origin and application policy. Known aliases compare without delegate identity; ordinary factories
and instances require a reviewed test-owned semantic identity. Keyed descriptors have no unkeyed
projection.

| Assertion | Contract |
| --- | --- |
| `EquivalentRegistrations` | Unordered multiset, preserving duplicate multiplicity |
| `RegistrationSequence` | Complete ordered sequence |
| `MatchedTypes` | Distinct known implementation set |
| `Service` | One service's lifetime, cardinality and implementation multiset |
| `Origins` | Separate ordered provenance comparison |
| `Rejection` | Exactly one candidate/origin with the supplied ordered reasons |

Assertions throw `CompositionAssertionException` and do not build a provider. A matching multiset
alone does not establish ordering, constructor resolvability or service lifetime ownership.
See [migration](../how-to/migrate-composition.md) for the separate acceptance checks.

Applications can copy reports into an optional diagnostics ledger. Aggregate the ordered
implementation/lifetime list into one choice per plan/group/service; keep rejected candidates
under separate rule/type identities. The
[application-owned adapter example](https://github.com/kidoz/hostloom/tree/main/examples/HostLoom.Examples.CompositionDiagnostics)
retains skip/replacement detail and avoids false conflicts for enumerable services.
Applying a plan never records or logs automatically.

## Package and release verification

NuGet consumers receive the generator under `analyzers/dotnet/cs` and a build-transitive
`CompilerVisibleProperty` for `MSBuildProjectDirectory`. Repository project-reference consumers
must explicitly reference the generator as an analyzer and expose that property themselves.
Emitted paths are project-relative when lexically inside the root; outside linked files or absent
project metadata use a filename fallback. Windows and POSIX separators are normalized without
resolving filesystem symlinks. No absolute checkout paths are emitted.

Inspect output with `EmitCompilerGeneratedFiles=true` and keep output under `obj` to avoid
compiling persisted source twice. The generator has no separately published runtime package.
Scrutor and Roslyn driver dependencies belong only to the measurement executable.

From a repository checkout, verify local NuGet artifacts and optional native execution:

```sh
python3 eng/verify-composition-package.py
python3 eng/verify-composition-package.py --runtime osx-arm64
```

Use a runtime matching the host. The verifier checks isolated application-only and testing-only
consumers, exact dependencies, bundled diagnostics and relative origins. Native consumers execute
generated closed registrations, known closed open generics, alias identity and synchronous and
asynchronous disposal. These checks do not certify arbitrary future generic constructions or an
external application's inventory. No package is published by the verifier.

See [performance evidence](composition-performance.md) for measured costs and numeric budgets,
and [the composition model](../explanation/composition-plans.md) for the validation boundaries.
