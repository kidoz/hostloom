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

ILoggingBuilder AddHostLoomLogging(Func<IServiceProvider, ILogSink> sink,
    Action<HostLoomLoggerOptions>? configure = null, ILogFormatter? formatter = null);

ILoggingBuilder AddHostLoomLogging(Func<IServiceProvider, ILogSink> sink,
    IConfiguration configuration, Action<HostLoomLoggerOptions>? configure = null,
    ILogFormatter? formatter = null);
```

The provider registers as a singleton with alias `HostLoom`. A container creates it when it
first resolves logging and disposes it, and the sink with it, when the container is disposed.
A sink instance passed at registration is shared by every container built from the service
collection, each running its own background writer against it. The factory overloads call the
factory once per container instead, so each gets a sink of its own, and CA2000 has no
undisposed sink to report at the call site. A factory that returns null fails when logging is
resolved.

The configuration overloads bind `HostLoomLoggerOptions` (conventionally from
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
| `ShutdownTimeout` | 5 s | Separate budgets for draining writes and disposing the sink |
| `MaxFieldNameLength` | `128` | UTF-8 bytes; a longer name drops the field, never the record |
| `MaxFieldsPerRecord` | `64` | Fields past the cap are dropped and counted; the record ships. Capture itself stops at four times the cap |
| `MaxMessageLength` | `16384` (16 KiB) | UTF-8 bytes of rendered message; see [Record size caps](#record-size-caps) |
| `MaxTextFieldLength` | `8192` (8 KiB) | UTF-8 bytes per plain text field, string hole, enricher value, or scope text |
| `AttachMachineName` | `true` | Adds the machine name as a static field |
| `ServiceName` | null | Adds a service name as a static field |
| `CaptureActivity` | `true` | Attach trace/span ids from `Activity.Current` |
| `Enrichers` | empty | `ILogEnricher` list |
| `Destructuring` | see below | `{@...}` destructuring limits |
| `TimeProvider` | `TimeProvider.System` | Testable timestamps |

Shutdown grants the writer `ShutdownTimeout`, then up to 250 ms for cancellation, and grants
sink disposal a separate `ShutdownTimeout` after the writer has stopped. Cancellation callbacks
and the entire sink disposal invocation run on dedicated background threads, so a synchronous
stall also respects these phase budgets. A sink whose writer or callbacks remain blocked is
abandoned without concurrent disposal. Bounded shutdown cannot force that external code to
release its resources.

None of the logging bounds depends on the thread pool. The shutdown deadlines are timed waits on
a thread of their own, `DisposeAsync` returns to its caller at once, a caller blocked on a full
queue waits on a monitor the writer signals, and the writer wakes as soon as a record arrives.
With every pool thread busy, `EnqueueTimeout` and `ShutdownTimeout` still hold, and records
still reach the sink.

`DestructuringOptions`: `MaxDepth` 5, `MaxCollectionItems` 32,
`MaxObjectMembers` 64, `MaxStringLength` 4096,
`MaxEncodedBytesPerRecord` 64 KiB, `MapLegacyAttributes` true, `TypeTags` false,
`IncludeFields` true; plus programmatic redaction for types you cannot annotate —
`NotLogged<T>(params string[] members)` and
`Mask<T>(string member, string text = "***", int showFirst = 0, int showLast = 0)`,
which follows the same reveal rule as [`[LogMasked]`](#masking-attributes).

`TypeTags` adds Serilog's `"$type"` member, the runtime type's short name, as the last member
of every destructured object except anonymous and other compiler-generated types. Turn it on
when existing queries expect Serilog-shaped output. `IncludeFields` destructures public
instance fields as well as properties; Serilog reads properties only, so turning it off keeps
fields that were never logged under Serilog out of the output. Value tuples are written as
sequences of their items either way.

`MaxStringLength` counts UTF-16 characters and applies to dictionary keys as well as string
values: a longer key or value keeps that many characters and ends in `…`.
`MaxEncodedBytesPerRecord` is a hard limit on the encoded JSON of all destructured values in
one record, event holes and scope values together. The walk checks it after every element. An
element that would take the record past the limit is removed and the cut is marked the same
way as the item and member caps: a `"…": "[Truncated]"` member in an object or dictionary, a
trailing `"…"` element in a collection, and the containers around the cut close without further
members. The walk keeps a few dozen bytes of the remaining budget free until it marks a cut, so
a value that only just fits may already be cut. A hole that does not fit even when cut, and any
hole after the budget is spent, is written as a `…` text field.

### Record size caps

Every cap is enforced on the producer thread, is measured in UTF-8 bytes, cuts on a character
boundary, and marks the cut with a trailing `…` — the same sentinel destructured strings use.
Every cap must be at least 1; the provider and the bootstrap logger reject other values at
construction, and the configuration overload rejects them at host startup.

- `MaxMessageLength` bounds the rendered message. Once the message is closed, the values of
  holes past the cut still ship as fields, and a hole whose bytes straddled the cut keeps its
  full value as a field, subject to the field cap. A template larger than this budget is
  discarded after safe rendering; CLEF emits the capped `@m` instead of `@mt`.
  Text buffers grow only for bytes within their remaining budgets, so large message and
  field inputs do not leave input-sized arrays in queued records.
- `MaxTextFieldLength` bounds each plain text field (`{Name}` and `{$Name}` holes, string
  holes on the `LogFast` path, enricher values, the static `MachineName`/`ServiceName` fields)
  and each entry of the `Scope` array. Strings and dictionary keys inside a destructured value
  are bounded by `Destructuring.MaxStringLength` instead, and the encoded size of all
  destructured values in a record by `Destructuring.MaxEncodedBytesPerRecord`.

## Sinks and formatters

| Type | Notes |
| --- | --- |
| `ILogSink` | `Write(ReadOnlySpan<byte>, CancellationToken)`, `FlushAsync`, `IAsyncDisposable` |
| `StreamLogSink(Stream, bool leaveOpen = false)` | `StreamLogSink.Console()` opens and owns standard output |
| `ILogFormatter` | `Format(in LogRecord, IBufferWriter<byte>)`, optional `OwnsFieldName` |
| `JsonLogFormatter(int maxExceptionLength = 32 * 1024)` | ECS-style compact JSON, one object per line; the hosted default |
| `ClefLogFormatter(int maxExceptionLength = 32 * 1024)` | CLEF (`@t`, `@mt`, `@l`, `@x`, `@tr`, `@sp`, …); the bootstrap default |

`ClefLogFormatter` writes the shape of Serilog's `CompactJsonFormatter`. `@t` is the UTC
round-trip format with all seven fractional digits (`2026-09-26T14:25:46.9209690Z`). `@r`
carries one rendering per template token that has a format (`{Amount:N2}`), in template
order; a token with only an alignment, or whose name Serilog would not accept as a hole
(`{order-id:N1}`), has none. A rendering uses the invariant culture, quotes a string unless its
format is `l`, and applies the token's alignment; a destructured (`{@Name:fmt}`) or collection
value renders as its captured, masked JSON. `SourceContext`, `ThreadId`, and `EventId` are
written unless the event captured a field of the same name, in which case the caller's value
is kept, as under Serilog. On the `LogFast`
path, `@r` holds the renderings of formatted holes instead.

A formatter instance can be shared. The one passed to `AddHostLoomLogging` is handed to the
provider of every container built from that service collection, and each provider formats on
its own writer thread, so `Format` can run concurrently. Both built-in formatters are safe for
that; a custom formatter must be too, and its `OwnsFieldName` must depend only on its input.

The background writer isolates formatter failures to a single record. If `Format` or
`OwnsFieldName` throws, the writer removes that record's partial output from the batch and
counts the record as dropped with reason `format_failed`. It then formats the rest of the batch
and later records with the same formatter instance, so a custom formatter must stay usable
after throwing. A formatter that throws on every record therefore loses every record without
stopping the writer, so watch the `format_failed` drop count. Only a sink failure faults the
pipeline (see [Health](#health)).

## Event holes

A standard `ILogger` call captures each hole of its message template as a field, following
Serilog's capture rules:

| Event hole | Scalar value | Collection or dictionary | Other object |
| --- | --- | --- | --- |
| `{Name}` | typed field | structure kept; an element that is neither a scalar nor a collection is its invariant `ToString()` | `ToString()` as a text field |
| `{@Name}` | typed field | destructured JSON | destructured JSON under the masking policy |
| `{$Name}` | text field | `ToString()` as a text field | `ToString()` as a text field |

A collection in a plain `{Name}` hole is bounded by the same destructuring caps and record
budget as a destructured one, and is enumerated once: the message is then rendered from the
captured fields. A byte array, `Memory<byte>`, or `ReadOnlyMemory<byte>` is a scalar in every
hole: uppercase hex, or beyond 1024 bytes its first 16 bytes followed by `... (N bytes)`.
Delegates, `Type` and other reflection objects, assemblies, and modules are written as their
`ToString()`, never walked.

Destructuring reads public properties with a public getter; a hidden (`new`) property is read
once, from the most derived type. Dictionary keys that render to the same text are written once,
the first entry winning, and the omission is marked with a `"…": "[Truncated]"` member.

A log call never throws because of the caller's values. A `ToString()` that throws, in a plain
hole or inside a collection, writes `"[DestructuringFailed]"` for that value only, as does a type
whose members cannot be reflected. A state that throws part-way, such as a template with fewer
arguments than holes, keeps the fields and template captured before the failure; the message is
then rendered from them, and a formatter that throws is replaced the same way, or by
`[MessageUnavailable]` when there is no template. These failures are counted under the
`destructurer` and `capture` components.

## Masking attributes

Fail-closed protection on destructured (`{@...}`) members:

| Attribute | Behavior |
| --- | --- |
| `[NotLogged]` | member never emitted; wins over `[LogMasked]` |
| `[LogMasked]` | `Text = "***"`, `ShowFirst = 0`, `ShowLast = 0` |

With `ShowFirst` and `ShowLast` at zero, the member is never read and only `Text` is written.
Otherwise the value's invariant string is shown as its first `ShowFirst` and last `ShowLast`
characters around `Text`, but only while at least as many characters stay hidden as are
shown. A value shorter than twice `ShowFirst + ShowLast` is written as `Text` alone:

| Value | `ShowLast = 4` | `ShowFirst = 2, ShowLast = 2` |
| --- | --- | --- |
| `4071` | `***` | `***` |
| `407152` | `***` | `***` |
| `40715236` | `***5236` | `40***36` |
| `4000123412341234` | `***1234` | `40***34` |

Of the other legacy attributes, `LogReplaced` is honored by masking the member whole with
`***`; its regular expression is not applied.

Both attributes are found through the inheritance chain: a member annotated on a base class
stays protected on a derived instance, including when the derived class overrides the
annotated virtual property. The same holds for the legacy attributes recognized by name under
`MapLegacyAttributes`.

The protection applies to members reached by destructuring. A plain `{Name}` hole in an
*event* template stringifies an object through its `ToString()`, including each object inside
a collection, and the message the caller's formatter renders does the same, so a record type's
generated `ToString()` prints every member there. Use `{@Name}` for any object carrying
protected members.

## Scopes

`BeginScope` state is captured on the producer thread at log time. Structured pairs (a
dictionary, or the template state produced by `BeginScope("order {OrderId}", id)`) flatten
into fields ranked below event holes: an event hole beats a scope value, an inner scope beats
an outer one. Scope values are captured as follows:

| Scope hole | Scalar value | Non-scalar value |
| --- | --- | --- |
| `{Name}` or `{@Name}` | typed field | destructured JSON under the masking policy and the record's `MaxEncodedBytesPerRecord` budget |
| `{$Name}` | text field | `ToString()` as a text field, bounded by `MaxTextFieldLength` |

A dictionary with string keys and a `(string, value)` tuple are structured scopes too, the
tuple naming one field.

No caller-side formatter renders a scope, so a non-scalar scope value is destructured even
without `@`; only `$` opts into `ToString()`. The destructuring budget is shared between the
event's `{@...}` holes and its scope values, outermost scope first, and a value past the budget
degrades to a `…` field.

A templated scope also contributes its rendered text to the `Scope` array, outermost first.
That text is rendered from the captured field representations of that scope — masked and
destructured values appear as they do in the fields, and a null hole renders as `null` — never
from the scope object's own `ToString()`. Format and alignment specifiers in the scope template
are ignored on this path. A non-structured scope (`BeginScope("checkout")`, or any object that
is not a sequence of key/value pairs) is rendered with its invariant `ToString()`, which the
masking policy does not reach; each `Scope` entry is bounded by `MaxTextFieldLength`.

## Security notes

- `AttachMachineName` defaults to `true`, so every record carries the host name as a static
  `MachineName` field. Turn it off where log output leaves a trust boundary and the host name
  is itself sensitive, or when an orchestrator already attaches it.
- An exception passed to a log call is captured on the record and rendered by the formatter
  in full — message, type, and stack trace, up to the formatter's `maxExceptionLength`
  (32 KiB by default). Exception text is not subject to `[NotLogged]`/`[LogMasked]`; an
  exception whose message embeds a secret leaks it. Keep credentials out of exception messages
  or wrap such exceptions before logging.
- The default `QueueFullPolicy.DropBelowWarning` drops Information and Debug records while the
  queue is full, and only Warning and above block. An audit trail logged at Information level
  can therefore lose events under sustained overload, silently apart from the `Dropped` counter
  and the `hostloom.logging.records.dropped` instrument. Log audit events at Warning or above, or use
  `QueueFullPolicy.Block` with an `EnqueueTimeout` so a stalled sink bounds the caller's wait
  instead of stalling the application.

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

`WriterFault` holds the sink failure that stopped the background writer. After a fault the
provider drops every record it receives. A formatter failure never sets `WriterFault`: it costs
only the record being formatted. `hostloom.logging.records.dropped` carries a `level` tag and one
of these `reason` values:

| Reason | Record dropped because |
| --- | --- |
| `queue_full` | the queue was full and the policy discards the record |
| `enqueue_timeout` | a blocking enqueue reached `EnqueueTimeout` |
| `format_failed` | the formatter threw while formatting this record |
| `writer_fault` | a sink failure faulted the writer while the record was queued or in its batch, or before it arrived |
| `provider_disposed` | it arrived after disposal started |
| `shutdown_timeout` | disposal reached its deadline before the record was written |

`hostloom.logging.failures` counts the underlying failures by `component`: `formatter`, `sink`,
`destructurer`, `enricher`, `scope`, or `capture`.
