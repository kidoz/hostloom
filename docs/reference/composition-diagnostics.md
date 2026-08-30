# Composition diagnostics

The `HostLoom.Diagnostics` package: a ledger of registration decisions,
reported once at startup. Standalone — no other HostLoom package depends
on it. Namespace: `HostLoom.Diagnostics`.

```text
dotnet add package HostLoom.Diagnostics
```

## Recording API

Extensions on `IServiceCollection`:

```csharp
CompositionLedger CompositionLedger();   // get-or-create the singleton ledger

IServiceCollection RecordComposition(string component, string choice,
    string? reason = null, [CallerMemberName] string? origin = null);

IServiceCollection RecordSkippedComposition(string component, string reason,
    [CallerMemberName] string? origin = null);

IServiceCollection AddCompositionDiagnostics();   // enables the startup report
```

`origin` is captured automatically from the recording method's name.
Recording the literal choice `"(skipped)"` through `RecordComposition`
throws `ArgumentException` — use `RecordSkippedComposition`.

`AddCompositionDiagnostics()` registers the reporter as a hosted service;
without it nothing is written, so libraries can record unconditionally.

## Ledger and report types

| Type | Members |
| --- | --- |
| `CompositionLedger` | `Record(...)`, `RecordSkipped(...)` (same shapes as the extensions), `Snapshot()` → `CompositionReport` |
| `CompositionDecision` | `Component`, `Choice`, `Reason?`, `Origin?`, `IsSkipped`; `const string Skipped = "(skipped)"` |
| `CompositionConflict` | `Component`, `Choices` (distinct, recording order) — produced when one component is recorded with *differing* choices; the same choice twice is not a conflict |
| `CompositionReport` | `Decisions`, `Conflicts`, `Describe()` → `"Transport=Kafka \| Outbox=(skipped) \| Scheduler=Quartz"` |

## Reporting

| Member | Behavior |
| --- | --- |
| `CompositionDiagnostics.LogCategory` | `"HostLoom.Diagnostics.Composition"` |
| `CompositionDiagnostics.Report(IServiceProvider)` | what the hosted service calls; swallows all failures — diagnostics never take the host down |
| `CompositionDiagnostics.Report(ILogger, CompositionReport)` | transparent variant for tests and custom hosts |

The report writes one `Information` line with the whole manifest, one
`Debug` line per decision (with reason and origin), and one `Warning` per
conflict, naming both choices without guessing which one the container
resolved. An empty ledger writes nothing.

## Testing

`Snapshot()` makes registration decisions assertable:

```csharp
var report = services.CompositionLedger().Snapshot();
Assert.Contains(report.Decisions,
    d => d is { Component: "OrderPublisher", Choice: "Kafka" });
```

## Limitations

The ledger is a plan, not a validation: nothing enforces that a recorded
entry still matches the registration beside it, and pipeline topology is
not recorded here — registered pipelines log their own resolved topology
at startup.
