# Logging

The `HostLoom.Logging` package: a structured, allocation-conscious
`Microsoft.Extensions.Logging` provider with a bounded queue and a
dedicated background writer. Namespace: `HostLoom.Logging`.

```text
dotnet add package HostLoom.Logging
```

## Registration

```csharp
ILoggingBuilder AddHostLoomLogging(ILogSink sink,
    Action<HostLoomLoggerOptions>? configure = null, ILogFormatter? formatter = null);

ILoggingBuilder AddHostLoomLogging(ILogSink sink, IConfiguration configuration,
    Action<HostLoomLoggerOptions>? configure = null, ILogFormatter? formatter = null);
```

The provider registers as a singleton with alias `HostLoom`. The
configuration overload binds `HostLoomLoggerOptions` (conventionally from
the `HostLoom:Logging` section) with unknown keys treated as errors —
typos fail startup. A code callback applies *after* configuration. When
`formatter` is null, `JsonLogFormatter` is used.

Level filtering is standard MEL configuration (`Logging:LogLevel:*`) and
runs before the provider; HostLoom does no level filtering of its own.

## HostLoomLoggerOptions

| Option | Default | Meaning |
| --- | --- | --- |
| `QueueCapacity` | `8192` | Bounded queue size (records) |
| `QueueFullPolicy` | `DropBelowWarning` | `Block` \| `DropNewest` \| `DropBelowWarning` |
| `BatchSize` | `256` | Records per writer batch |
| `EnqueueTimeout` | null (block without limit) | Cap on how long a log call may block under `Block` |
| `ShutdownTimeout` | 5 s | Drain window on dispose |
| `MaxFieldNameLength` | `128` | — |
| `MaxFieldsPerRecord` | `64` | — |
| `AttachMachineName` | `true` | Adds the machine name as a static field |
| `ServiceName` | null | Adds a service name as a static field |
| `CaptureActivity` | `true` | Attach trace/span ids from `Activity.Current` |
| `Enrichers` | empty | `ILogEnricher` list |
| `Destructuring` | see below | `{@...}` destructuring limits |
| `TimeProvider` | `TimeProvider.System` | Testable timestamps |

`DestructuringOptions`: `MaxDepth` 5, `MaxCollectionItems` 32,
`MaxObjectMembers` 64, `MaxStringLength` 4096,
`MaxEncodedBytesPerRecord` 64 KiB, `MapLegacyAttributes` true; plus
programmatic redaction for types you cannot annotate —
`NotLogged<T>(params string[] members)` and
`Mask<T>(string member, string text = "***", int showFirst = 0, int showLast = 0)`.

## Sinks and formatters

| Type | Notes |
| --- | --- |
| `ILogSink` | `Write(ReadOnlySpan<byte>, CancellationToken)`, `FlushAsync`, `IAsyncDisposable` |
| `StreamLogSink(Stream, bool leaveOpen = false)` | `StreamLogSink.Console()` opens and owns standard output |
| `ILogFormatter` | `Format(in LogRecord, IBufferWriter<byte>)`, optional `OwnsFieldName` |
| `JsonLogFormatter(int maxExceptionLength = 32 * 1024)` | ECS-style compact JSON, one object per line; the hosted default |
| `ClefLogFormatter(int maxExceptionLength = 32 * 1024)` | CLEF (`@t`, `@mt`, `@l`, `@x`, `@tr`, `@sp`, …); the bootstrap default |

## Masking attributes

Fail-closed protection on destructured (`{@...}`) members:

| Attribute | Behavior |
| --- | --- |
| `[NotLogged]` | member never emitted; wins over `[LogMasked]` |
| `[LogMasked]` | `Text = "***"`, `ShowFirst = 0`, `ShowLast = 0` |

## LogFast

Allocation-free structured logging through an interpolated string
handler; field names come from the argument expressions:

```csharp
logger.LogFast(LogLevel.Information, $"processed {orderId} in {elapsed}");
```

Overloads: `(LogLevel, message)`, `(LogLevel, Exception?, message)`,
`(LogLevel, EventId, message)` — there is no combined
`(EventId, Exception)` overload. Zero-allocation applies with HostLoom's
own logger; other providers receive the rendered message and structured
state through the standard interface.

## Enrichers

`ILogEnricher.Enrich(ref LogEntryWriter writer)` runs per record;
`LogEntryWriter.Add(name, value)` has overloads for `string?`, `bool`,
`int`, `long`, `double`, `decimal`, `Guid`, `DateTimeOffset`, `TimeSpan`.

## Bootstrap logger

For the window before the host exists:

```csharp
using var bootstrap = new HostLoomBootstrapLogger(minimumLevel: LogLevel.Information);
```

Full ctor: `(HostLoomLoggerOptions?, ILogFormatter?, Stream?, LogLevel
minimumLevel = Information, string category = "Bootstrap", bool failFast
= false)`. Writes synchronously to stdout with the same event shape,
masking, and static fields; defaults to `ClefLogFormatter`. Dispose it
once the hosted provider is up — it retains nothing, so the hand-off
neither replays nor duplicates.

## Health

`HostLoomLoggerProvider` exposes `Dropped` and `WriterFault`, and the
`HostLoom.Logging` meter publishes seven instruments — see the
[observability reference](observability.md#logging-instruments-hostloomlogging).
