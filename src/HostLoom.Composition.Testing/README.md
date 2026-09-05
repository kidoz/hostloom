# HostLoom.Composition.Testing

Optional, container-free assertions over generated plans and probes. The only project dependency
is `HostLoom.Composition`; there is no test framework, provider, Roslyn or diagnostics dependency.
Assertion failures throw `CompositionAssertionException`, which any test runner can report.

```csharp
CompositionAssert.EquivalentRegistrations(explicitPlan, discoveredPlan); // multiset
CompositionAssert.RegistrationSequence(explicitPlan, discoveredPlan);   // ordered
CompositionAssert.MatchedTypes(discoveredPlan.Probe(), typeof(Catalog), typeof(Inventory));
CompositionAssert.Service(discoveredPlan.Probe(), typeof(ICatalog), ServiceLifetime.Scoped,
    CompositionCardinality.Many, typeof(Catalog), typeof(Inventory));
CompositionAssert.Origins(explicitPlan.Probe(), expectedOrigin, expectedOrigin);
CompositionAssert.Rejection(discoveredPlan.Probe(), rejectedIdentity, rejectedOrigin, "Abstract class.");
```

Plans and types in the snippet belong to the application. Registration equivalence compares exact
service/implementation types, lifetime, activation kind and alias target. It preserves duplicate
multiplicity. It ignores origins, plan identity, cardinality and strategy; use separate probe/policy
assertions for those. The sequence assertion also checks order. Neither comparison constructs a
provider, executes factories, reflects over candidates, or interprets a rule declaration.

`CompositionRegistrationShape.Project(probe)` returns an immutable ordered projection. For migration
inventories, normalize legacy descriptors with `CompositionRegistrationShape.FromDescriptor` and
compare those shapes as a multiset, then pin old/new sequences separately. Ordering equivalence
alone does not prove application behavior or dependency resolvability.

Forwarding factories require an explicit `aliasTargetType` when projecting raw descriptors; generated
registrations already carry it. Ordinary factories and prebuilt instances are opaque. Supply a stable,
reviewed `opaqueIdentity` for each semantic contract, or projection fails. For a probe use
`Project(probe, entry => identityFor(entry))`. That test-owned callback is only invoked for opaque
entries and must not execute the activation. Helpers never use delegate/descriptor/instance identity
or guess an instance's implementation type. A supplied identity is an assertion by the test author,
not proof that two arbitrary functions behave alike. Keyed descriptor normalization is unsupported.

`MatchedTypes` compares distinct known implementation types, including alias targets; it rejects
unknown factory/instance matches. `Service` verifies the complete implementation multiset, lifetime
and cardinality for the named service. `Origins` checks registration origins in order. `Rejection`
requires exactly one candidate at the supplied origin and compares its reasons in order; it does not
assert the absence of other rejected candidates, so pin the rejection count separately when needed.
