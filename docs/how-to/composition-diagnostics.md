# Record composition decisions

Make conditional service registration observable: record each decision in
a ledger as it happens, and get the whole plan reported once at startup
through the application's own logging stack.

## Before you begin

An application (or library) whose composition root registers different
implementations depending on configuration. The package stands alone —
no other HostLoom package depends on it.

```text
dotnet add package HostLoom.Diagnostics
```

## 1. Record decisions where they happen

```csharp
using HostLoom.Diagnostics;

public static IServiceCollection AddOrderPublishing(
    this IServiceCollection services, OrderOptions options)
{
    if (options.Kafka.Enabled)
    {
        services.AddSingleton<IOrderPublisher, KafkaOrderPublisher>();
        services.RecordComposition("OrderPublisher", "Kafka", "Orders:Kafka:Enabled=true");
    }
    else
    {
        services.AddSingleton<IOrderPublisher, InProcessOrderPublisher>();
        services.RecordComposition("OrderPublisher", "InProcess", "Orders:Kafka:Enabled=false");
    }

    if (options.Outbox is null)
    {
        services.RecordSkippedComposition("Outbox", "no Orders:Outbox section bound");
    }

    return services;
}
```

Record **skipped** components too — that is what lets the report answer
"what is missing". Keep each entry next to the registration it
describes; prefer no entry to a stale one.

## 2. Turn the report on

Anywhere in the application's composition root:

```csharp
builder.Services.AddCompositionDiagnostics();
```

Without this call nothing is written, so a library can record
unconditionally.

## 3. Verify

Start the host and look for one `Information` line under the
`HostLoom.Diagnostics.Composition` category:

```text
info: HostLoom.Diagnostics.Composition
      HostLoom composition: OrderPublisher=Kafka | Outbox=(skipped) | Scheduler=Quartz
```

Raise the category to `Debug` for one line per decision with its reason
and the recording call site. Standard `Logging` configuration raises,
lowers, or silences the category.

## 4. Assert on it in tests

`CompositionLedger` and its `Snapshot()` are public, so a test asserts on
what registration decided instead of parsing log output.

## Troubleshoot

- **No report at startup** — `AddCompositionDiagnostics()` was never
  called, or the `HostLoom.Diagnostics.Composition` category is filtered
  below `Information`.
- **A `Warning` naming one component twice** — two calls recorded
  disagreeing choices for the same component; the report refuses to guess
  which one the container resolved. Find and fix the double registration.
- **The report says one thing, the container does another** — the ledger
  is a plan, not a validation; a stale entry survived a code change. Keep
  `ValidateOnBuild`/`ValidateScopes` on in development and
  `ValidateOnStart` on options for the checks that stay correct on their
  own.

## Related

- When an entry earns its keep, and why the ledger reports at startup:
  [architecture](../explanation/architecture.md#explicit-over-convention).
- Full API: [composition diagnostics reference](../reference/composition-diagnostics.md).
