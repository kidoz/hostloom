# Why composition is a plan

Registration discovery answers which services should exist. A running provider answers whether
those services can actually be created. HostLoom keeps the intermediate registration plan available
so applications can inspect and test intent before constructing anything.

The declaration is compiler input. The generator selects source symbols, validates the supported
registration shape and emits a factory containing explicit descriptors. Calling that factory
creates a plan with provenance. `Probe()` exposes its immutable snapshot. `ApplyTo()` validates
that intent against the collection as it exists at that point, changes it and returns an action
report. Building and using the final provider is the next, separate responsibility.

| Phase | Evidence it supplies |
| --- | --- |
| Compile declarations | Supported syntax, eligible types, projection, counts and known shape/capture checks |
| Create and probe a plan | Ordered intended registrations, origins and rejected candidates |
| Apply to a collection | Collision policy, cardinality and lifetime invariants at application time |
| Build and exercise the provider | Constructor dependencies, actual scope ownership and known generic/factory paths |

## Intent and application history

The same plan can encounter different preexisting services in different hosts. A probe therefore
cannot say which entries were added, skipped or replaced; that belongs to the application report.
Reports remain snapshots after later registrations change the collection. This distinction also
keeps diagnostics optional: an application can inspect or record the evidence without forcing a
logging, hosting or diagnostics dependency into the composition runtime.

A ledger adapter records one ordered implementation/lifetime choice per plan/group/service.
Recording each member of an enumerable as competing choices would create false conflicts.
Replacement actions must remove earlier additions from the retained choice, and rejected candidates
need separate identities so their explanations cannot masquerade as registered services.

## Determinism and explicit boundaries

A restricted DSL makes selection reviewable at build time. Runtime predicates and captured values
cannot participate. Stable symbol ordering and normalized source paths make emitted code and
provenance comparable across checkout roots. An unrelated source edit can require semantic work
while leaving the final emission cached; the benchmark reports both the work and the reused output.
Inherited attributes, interfaces, accessibility and rule changes must invalidate affected results.

Counts measure eligible implementations before service projection. Cardinality constrains each
service after projection. These checks answer different questions, and neither replaces an exact
consumer inventory. Namespace guards report unexpected placement rather than quietly deleting a
candidate from the plan.

## Identity and ownership

A scoped self descriptor and its forwarding aliases resolve to the same object within a scope;
a singleton follows the provider's lifetime. A transient alias resolves a fresh self object on each
call. The built-in container can capture an object through its self descriptor and aliases and
invoke disposal repeatedly. Idempotent disposal is part of adopting that registration shape.

Composition never executes factories to infer their implementation. Known generated aliases carry
explicit target metadata; opaque factories and prebuilt instances remain opaque. Test helpers
require reviewed semantic identities for these cases instead of treating delegate equality as
behavioral equivalence.

## Costs and limits

Generating explicit registrations removes runtime assembly scanning, but plan construction,
validation and provenance have their own costs. In the initial 100-registration measurement,
creation plus application is slower and allocates more than handwritten registration and the
pinned scanner comparison. A passive probe is cheap because it returns an existing snapshot;
it cannot represent total startup cost. Optional ledger formatting adds a separate cost.
The [performance reference](../reference/composition-performance.md) publishes every phase,
including first calls, warm runs and added compiler cost.

AOT evidence covers executed, known closed paths. Conservative open-generic checks and capture
diagnostics cannot prove arbitrary dependencies, future closed constructions or opaque factories.
The [migration guide](../how-to/migrate-composition.md) keeps those acceptance checks in the consumer.

Decoration, keyed declarations, runtime/plugin scanning and standalone type-level registration
attributes are deferred. Registering a handler CLR type also does not configure a messaging endpoint;
explicit messaging registration still owns addresses and dispatch metadata.
