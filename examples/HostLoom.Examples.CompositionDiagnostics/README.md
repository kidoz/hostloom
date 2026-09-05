# Application-owned composition ledger integration

Run `dotnet run --project examples/HostLoom.Examples.CompositionDiagnostics` from the repository root.
The example applies two enumerable implementations and records one ordered choice without a ledger
conflict. No provider, logger or external service is needed.

Copy/adapt [ApplicationCompositionLedger.cs](ApplicationCompositionLedger.cs) into the application
that opts into both composition and diagnostics. It is example code, not a bridge package or a
mandatory dependency of either composition package. Use the same plan instance that produced the
application report:

```csharp
CompositionPlan plan = CatalogComposition.CreatePlan();
CompositionApplicationReport applied = plan.ApplyTo(services);
ApplicationCompositionLedger.Record(services.CompositionLedger(), plan, applied);
// services.AddCompositionDiagnostics() may be called separately when the host should log the ledger.
```

The adapter groups by plan/group/service and records one ordered implementation/lifetime list.
It replays replacement actions to exclude additions removed later during the same application.
Reasons preserve all added/skipped/replaced actions and previous origins; a skip-only service says
“No retained additions.” It never claims that this is the complete collection, since existing
registrations retained by Skip and later external mutations are outside that report.

Rejected candidates get separate skipped components qualified by plan/group/rule/type identity.
Length-prefixed keys prevent names containing separators from colliding. Known generated aliases
are named from plan metadata; other factories and instances stay opaque and are never invoked or
inspected. Multiple legitimate implementations produce no ledger conflict. Recording different
choices under the same qualified component still exposes a real disagreement.

Regression tests compile this exact adapter source and verify enumerable aggregation, skips,
replacement origins, removal of earlier additions, rejection reasons and immutable report behavior.
