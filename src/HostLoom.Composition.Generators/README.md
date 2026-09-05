# HostLoom.Composition.Generators

The initial incremental generator turns methods marked with `CompositionRules` into explicit
`CompositionPlan` factories. It also supplies an analyzer rejecting runtime invocation or delegate
capture of declaration methods. This project targets netstandard2.0, references Roslyn only, and
has no runtime reference to HostLoom.Composition or DI. Release packaging remains disabled.

See the [runtime README](../HostLoom.Composition/README.md#generated-plans) for the two development
project references and a complete declaration/factory example. The
[AOT example](../../examples/HostLoom.Examples.CompositionAot/GeneratedCatalogComposition.cs) executes
that same generated path with inherited closed generic interfaces and scoped dependencies.

## Supported declaration syntax

A declaration is a non-generic `static void` method with a block body and exactly one
`CompositionRuleBuilder` parameter. Its attribute names one unimplemented parameterless static
partial method returning `CompositionPlan` in the same type. All containing types must be
non-generic, non-file-local partial classes. Nested classes and multiple distinct factories work;
records and structs are not supported. Multiple declarations claiming the same factory are errors.

| Part | Current methods |
| --- | --- |
| Grouping | `Group(constantName, inlineLambda)`; one level with unique names |
| Candidate source | `AddTypes(typeof(...), ...)`, `AddClasses()` |
| Assignability | `AssignableTo<T>()`, `AssignableTo(typeof(...))`, `AssignableToAny(typeof(...), ...)` |
| Service projection | `AsSelf()`, `AsImplementedInterfaces()`, `As<T>()`, `As(typeof(...), ...)` |
| Lifetime | `WithTransientLifetime()`, `WithScopedLifetime()`, `WithSingletonLifetime()`, `WithLifetime(constant)` |
| Cardinality | `ExpectOne()`, `ExpectMany()` |
| Optional absence | `AllowEmpty()` |

Each rule specifies exactly one lifetime, cardinality and service projection. Multiple selectors
intersect candidate sets; `AssignableToAny` unions its arguments. Closed selectors match exact
constructed interfaces/bases; open selectors match the generic definition. `AsImplementedInterfaces`
projects only matching interfaces, including inherited ones. An explicit open interface in `As`
projects every matching implemented closed form, and every requested service must match. Incidental
interfaces do not enter the container. Empty eligible sets are errors unless `AllowEmpty` is present.

Discovery examines source-declared types in the current compilation. Referenced base/interface
symbols participate in matching, but referenced implementations require an explicit `AddTypes` entry.
Concrete public/internal classes with public constructors are supported when accessible from the
factory. Abstract, open, static and compiler-generated types are excluded from discovery results;
invalid explicit candidates are diagnosed. Another generator's output is not a discovery input.

Arguments are positional: explicit `typeof` expressions, generic type arguments and compile-time
scalar constants. Groups accept inline synchronous lambdas with one builder parameter. Locals,
control flow, arbitrary helper calls, runtime arrays, captured values, nested groups, arbitrary
predicates and repeated lifetime/cardinality/projection clauses are rejected. Declaration methods
are never executed to discover registrations. Referencing their names with `nameof` is allowed.

Groups/rules retain declaration order. Within each rule, implementations sort by namespace and full
metadata name, ordinal, then projected services sort by fully qualified identity. Explicit lists
use the same sort. This order is deterministic but differs from runtime reflection enumeration.

## Diagnostics

| ID | Error |
| --- | --- |
| HLM0009 | Unsupported declaration, invalid factory pairing, or runtime declaration use |
| HLM0010 | Unbounded/empty selection or invalid/inaccessible candidate |
| HLM0011 | Missing/repeated lifetime or cardinality, or invalid lifetime constant |
| HLM0012 | Missing/incompatible service projection or no public constructor |
| HLM0013 | Duplicate activation, cardinality or lifetime conflict in the plan |

Invalid declarations emit no factory. Error locations point to authored source; type failures and
conflicts include additional locations when available. The generator proves registration shape,
not completeness of external constructor dependencies. Use final-provider/scope validation and
known closed-type resolution tests after all application registrations.

## Incremental behavior and remaining work

Attribute-based entrypoint discovery uses `ForAttributeWithMetadataName`. Inherited candidate
matching may rerun after semantic edits; the final source-output step compares emitted text by
value and reuses unchanged results. Tests cover an unrelated body edit and invalidation after an
inherited interface changes in another file. No build-time performance budget is claimed yet.

Generated text uses LF line endings and explicit fully qualified descriptors, with rule/group/source
provenance. Relative syntax paths are preserved; absolute paths currently become filenames. Full
project-relative normalization, rejected-candidate emission, advanced filters/projections, explicit
open-generic registration, count assertions, configurable strategies, lifetime capture analysis,
and bundled NuGet consumption remain later milestones.

The test suite contains six reviewed output snapshots and executes emitted assemblies through DI.
Snapshots are ordinary `.verified.txt` files; test failures never update them automatically. A
received file is written to the operating system's temporary directory for comparison.
