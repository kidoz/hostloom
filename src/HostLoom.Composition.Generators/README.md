# HostLoom.Composition.Generators

The incremental generator turns methods marked with `CompositionRules` into explicit
`CompositionPlan` factories. Its analyzer rejects runtime invocation or delegate capture of rule
methods. This netstandard2.0 project references Roslyn only and is bundled under `analyzers/dotnet/cs`
in `HostLoom.Composition`; it is not a separately published runtime dependency. The application NuGet
also supplies the compiler-visible project directory through build-transitive props. Performance
budgets and complete migration/release evidence remain follow-up work.

See the [runtime README](../HostLoom.Composition/README.md#generated-plans) for the declaration/factory
example and references. The [AOT example](../../examples/HostLoom.Examples.CompositionAot/Program.cs)
executes generated inherited interfaces, self aliases, open generics and synchronous/asynchronous
disposal alongside explicit plans.

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

Application policies use the [runtime strategy table](../HostLoom.Composition/README.md).
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
body edits and inherited interface changes in another file. Path tests cover different checkout
roots and Windows/POSIX separators. Seven reviewed snapshots include aliases, generics, strategies
and rejections. Failures write received files to the OS temporary directory and never update
verified snapshots automatically.

The full solution, seven snapshots and native example cover development consumers. The package
verifier additionally builds an isolated application with one HostLoom reference and a helper-only
consumer whose generator arrives transitively. It checks dependency graphs, negative HLM0009/HLM0014
builds and optional packed-runtime/helper AOT execution. `HostLoom.Composition.Testing` supplies
separate semantic, ordering and provenance assertions. Measured build/runtime budgets and external
migration evidence remain outstanding.
Decoration, keyed declarations, runtime scanning and standalone registration attributes are deferred.
