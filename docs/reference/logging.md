# Logging

The `HostLoom.Logging` package: a structured, allocation-conscious
`Microsoft.Extensions.Logging` provider with a bounded queue and a
dedicated background writer. Namespace: `HostLoom.Logging`.

```text
dotnet add package HostLoom.Logging
```

## Registration

```csharp
ILoggingBuilder AddHostLoomLogging(IConfiguration configuration,
    Action<HostLoomLoggerOptions>? configure = null);

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
resolved. The overload taking only configuration writes to standard output, opening a
`StreamLogSink.Console()` per container.

The configuration overloads bind `HostLoomLoggerOptions` (conventionally from
the `HostLoom:Logging` section) with unknown keys treated as errors —
typos fail startup. A code callback applies *after* configuration. Options are validated when
`AddHostLoomLogging` runs, so an invalid value fails at the registering line. A `formatter`
passed in code takes precedence over the `Formatter` option; when neither names one,
`JsonLogFormatter` is used.

The provider is registered once per service collection. A second `AddHostLoomLogging` call
throws; call `ClearProviders()` before it to replace the first registration.

`Enrichers` and `TimeProvider` can be set only in code. Configuration that sets either — for
example, an enricher list under `HostLoom:Logging:Enrichers` — fails startup instead of being
ignored.

A wrapper that wants defaults configuration can still override calls
`HostLoomLoggerOptions.Bind(IConfiguration)` itself from the callback of an overload without
configuration. `Bind` applies configuration over the current values with the same strict rules
and returns the options:

```csharp
builder.Logging.AddHostLoomLogging(
    _ => StreamLogSink.Console(),
    options =>
    {
        // Defaults that configuration may override.
        options.Formatter = LogFormatterNames.Clef;
        options.Destructuring.TypeTags = true;
        options.Bind(builder.Configuration.GetSection("HostLoom:Logging"));
        options.Enrichers.Add(new RegionEnricher()); // code only
    });
```

Level filtering is standard MEL configuration (`Logging:LogLevel:*`) and
runs before the provider; HostLoom does no level filtering of its own.

## HostLoomLoggerOptions

| Option | Default | Meaning |
| --- | --- | --- |
| `QueueCapacity` | `8192` | Bounded queue size (records) |
| `QueueFullPolicy` | `DropBelowWarning` | `Block` \| `DropNewest` \| `DropBelowWarning` |
| `BatchSize` | `256` | Records per writer batch |
| `EnqueueTimeout` | 1 s | Cap on how long a log call may wait for room on a full queue before its record is dropped and counted; null waits without limit |
| `ShutdownTimeout` | 5 s | Separate budgets for draining writes and disposing the sink; also bounds the flush when the process ends without disposal |
| `MaxFieldNameLength` | `128` | UTF-8 bytes; a longer name is rejected before encoding, dropping the field, never the record |
| `MaxFieldsPerRecord` | `64` | Fields past the cap are dropped and counted; the record ships. Capture itself stops at four times the cap |
| `MaxMessageLength` | `16384` (16 KiB) | UTF-8 bytes of rendered message; see [Record size caps](#record-size-caps) |
| `MaxTextFieldLength` | `8192` (8 KiB) | UTF-8 bytes per plain text field, string hole, enricher value, or scope text |
| `Formatter` | null | `Json` \| `Clef`, case-insensitive; null uses JSON for the provider and CLEF for the bootstrap logger. A formatter passed in code wins; any other name fails validation |
| `MaxExceptionLength` | `32768` (32 KiB) | Characters of exception message and stack trace written by a formatter created from `Formatter` or by default |
| `AttachMachineName` | `true` | Adds the machine name as a static field |
| `ServiceName` | null | Adds a service name as a static field |
| `CaptureActivity` | `true` | Attach trace/span ids from `Activity.Current` |
| `Enrichers` | empty | `ILogEnricher` list; code only |
| `Destructuring` | see below | `{@...}` destructuring limits |
| `TimeProvider` | `TimeProvider.System` | Testable timestamps; code only |

Shutdown grants the writer `ShutdownTimeout`, then up to 250 ms for cancellation, and grants
sink disposal a separate `ShutdownTimeout` after the writer has stopped. Cancellation callbacks
and the entire sink disposal invocation run on dedicated background threads, so a synchronous
stall also respects these phase budgets. A sink whose writer or callbacks remain blocked is
abandoned without concurrent disposal. Bounded shutdown cannot force that external code to
release its resources.

A log call waits for room on a full queue only under `Block`, and for Warning and above under
`DropBelowWarning`. `EnqueueTimeout` caps that wait at one second by default. A sink that stops
draining, such as stdout that nobody reads, then costs each such call at most a second, and the
record is dropped and counted as `enqueue_timeout`. Setting it to null makes those calls wait as
long as the sink stalls, so a stuck stdout stalls every thread that logs a warning. Configuration
sets null with an empty value, `"EnqueueTimeout": ""`.

A process that ends without disposing the provider still attempts to drain its queued records. The
writer is a background thread, so the queue would otherwise die with the process, taking the
last lines before a crash with it. On `AppDomain.ProcessExit`, which `Environment.Exit` and `Main`
returning raise, and on `AppDomain.UnhandledException`, the provider waits up to
`ShutdownTimeout` for queued records to be processed and a sink flush attempt to complete. It keeps
accepting records meanwhile. An `UnhandledException` handler that logs the crash gets its record
into that flush only if it was registered before the provider was created; handlers run in
registration order. A handler registered later can flush its own records with
`HostLoomLoggerProvider.Flush(TimeSpan)`. It returns `true` once the writer has processed records
queued before the flush marker and completed the sink flush attempt. This does not guarantee
successful delivery: records may have been dropped and the sink flush may have failed. Sink
flush failures increment `hostloom.logging.failures` with `component=sink` and do not stop the
writer. The method returns `false` if its timeout passes first, the pipeline stops short of
completion, or it is called from the provider's writer thread. Records queued after the marker
are accepted as usual and not waited for:

```csharp
var provider = host.Services.GetServices<ILoggerProvider>().OfType<HostLoomLoggerProvider>().Single();
AppDomain.CurrentDomain.UnhandledException += (_, e) =>
{
    logger.LogCritical(e.ExceptionObject as Exception, "Unhandled exception");
    provider.Flush(TimeSpan.FromSeconds(5));
};
```

`Environment.FailFast`, SIGKILL, and an out-of-memory kill run no handlers, so whatever is still
queued then is lost.

Since .NET 10 the runtime no longer handles SIGTERM, so a SIGTERM ends a process at once and
raises no event. The Generic Host handles it and disposes the provider, which drains the queue.
An application without the host must handle the signal itself for anything queued to survive
it, for example with `PosixSignalRegistration.Create(PosixSignal.SIGTERM, _ =>
Environment.Exit(0))`, which raises `ProcessExit` and so runs the flush.

None of the logging bounds depends on the thread pool. The shutdown deadlines are timed waits on
a thread of their own, `DisposeAsync` returns to its caller at once, a caller blocked on a full
queue waits on a monitor the writer signals, and the writer wakes as soon as a record arrives.
With every pool thread busy, `EnqueueTimeout` and `ShutdownTimeout` still hold, and records
still reach the sink.

Concurrent calls to `DisposeAsync` await the same bounded shutdown operation, including drain
and sink disposal. Synchronous `Dispose` waits for that same completion.

`DestructuringOptions`: `MaxDepth` 5, `MaxCollectionItems` 32,
`MaxObjectMembers` 64, `MaxStringLength` 4096,
`MaxEncodedBytesPerRecord` 64 KiB, `MapLegacyAttributes` true, `TypeTags` false,
`IncludeFields` true; plus programmatic redaction for types you cannot annotate —
`NotLogged<T>(params string[] members)` and
`Mask<T>(string member, string text = "***", int showFirst = 0, int showLast = 0)`,
which follows the same reveal rule as [`[LogMasked]`](#masking-attributes); and `Preserve<T>()`,
which keeps a type's members for destructuring in a trimmed or Native AOT app (see
[Trimming and Native AOT](#trimming-and-native-aot)).

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

`LogFast` also bounds the buffers offered to UTF-8 formattables, with a small fixed allowance
for complete primitive tokens. A value or rendering that cannot fit becomes `…`; numeric
fields retain their complete canonical tokens. These bounds apply to HostLoom's buffers;
caller formatting methods can still allocate or block internally.

## Sinks and formatters

| Type | Notes |
| --- | --- |
| `ILogSink` | `Write(ReadOnlySpan<byte>, CancellationToken)`, `FlushAsync`, `IAsyncDisposable` |
| `StreamLogSink(Stream, bool leaveOpen = false)` | `StreamLogSink.Console()` opens and owns standard output; disposal attempts to release an owned stream even if flushing fails |
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
stopping the writer, so watch the `format_failed` drop count.

Sink failures cost a batch in the same way. If `Write` throws, the writer counts the batch's
records as dropped with reason `sink_failed` and goes on with the next batch on the same sink, so
a sink that recovers, such as a disk with space again, gets every record after the failure. The
failed batch is not retried, because the sink may already have written part of it. A sink that
throws on every write loses every record without stopping the writer or blocking a caller, so
watch the `sink_failed` drop count. A failed `FlushAsync` is counted under `failures` and costs
nothing further.

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
whose members cannot be reflected. An unreadable `Memory<byte>` or `ReadOnlyMemory<byte>`
also replaces only that value; sibling members and collection items remain intact within the
configured destructuring caps. A state that throws part-way, such as a template with fewer
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
  and the `hostloom.logging.records.dropped` instrument. Log audit events at Warning or above, or
  use `QueueFullPolicy.Block`. Either way a record still waiting for room after `EnqueueTimeout`,
  one second by default, is dropped too; only a null `EnqueueTimeout` loses nothing, and it lets a
  stalled sink stall the application.

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

The standard-interface handoff, including an injected `ILogger<T>`, boxes canonical numeric and
boolean values so their JSON types match the direct HostLoom path. Text fields remain text.
This changes the previous handoff behavior, which emitted every field as a string; update
downstream mappings that were configured for those string values.

Wrapped logging passes field names by reference during the synchronous handoff, so each HostLoom
provider applies its own `MaxFieldNameLength`, including limits above 128 bytes. Before enqueueing,
the provider removes rejected fields and releases those name references. Rejected names are never
UTF-8 encoded or copied to strip `@` or `$`; their captured values remain available for masked
message and scope rendering within the existing capture limits.

Exceptions raised by `ToString`, `IFormattable`, or UTF-8 formatting on this path produce
`[DestructuringFailed]` and increment `hostloom.logging.failures` with `component=capture`
when delivered to HostLoom. If only the display format fails (for example, `{count:Q}` for an
integer), the message uses the sentinel while the field keeps its canonical value, provided
canonical capture succeeds. Both direct and wrapped HostLoom logging preserve its numeric type.
Later holes and records still log normally. Exceptions from
evaluating the interpolation expressions themselves remain the caller's responsibility.

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
masking, and static fields; uses the formatter the options' `Formatter` names, and
`ClefLogFormatter` when it names none. Dispose it
once the hosted provider is up — it retains nothing, so the hand-off
neither replays nor duplicates.

## Trimming and Native AOT

The package is annotated for trimming and Native AOT, and
`examples/HostLoom.Examples.LoggingAot` publishes natively and checks its own output. Options
bind from configuration through compiled binding code, so the configuration overloads work in a
native app, the `Formatter` name included, and an empty `EnqueueTimeout` still lifts the limit.

Destructuring is the part that depends on reflection. `{@...}` reads a value's public properties
and fields at run time, and a trimmed or natively compiled app keeps them only when something
else uses them. Call `Preserve<T>()` for every type written with `{@...}`, nested types
included:

```csharp
logging.Destructuring.Preserve<Order>().Preserve<Shipment>();
```

`NotLogged<T>` and `Mask<T>` preserve their type the same way. Under Native AOT, a type nobody
preserved has no members to read. It is written as `{}`, or with only its `"$type"` when
`TypeTags` is on, and counted once in `hostloom.logging.failures` with `component=destructurer`.
Anonymous types cannot be preserved, so a native app always writes them as `{}`. A trimmed app
that still runs on the JIT keeps the members it saw used and can drop others without that count.

The masking attributes keep working natively on preserved types. A legacy `LogMasked` attribute
whose options cannot be read masks its member whole with `***` and counts a destructurer
failure, and the rest of the object is still written; Native AOT does this to the options when
nothing else reads them. Exception stack traces in a native app carry no file names or line
numbers, which is how the runtime formats them.

## Health

`HostLoomLoggerProvider` exposes `Dropped` and `WriterFault`, and the
`HostLoom.Logging` meter publishes seven instruments — see the
[observability reference](observability.md#logging-instruments-hostloomlogging).

`WriterFault` holds the defect that stopped the background writer. After a fault the provider
drops every record it receives. Formatter and sink failures never set `WriterFault`: a formatter
failure costs only the record being formatted, and a failed sink write only its batch. `hostloom.logging.records.dropped` carries a `level` tag and one
of these `reason` values:

| Reason | Record dropped because |
| --- | --- |
| `queue_full` | the queue was full and the policy discards the record |
| `enqueue_timeout` | a blocking enqueue reached `EnqueueTimeout` |
| `format_failed` | the formatter threw while formatting this record |
| `sink_failed` | the sink threw while writing the record's batch |
| `writer_fault` | a defect stopped the writer while the record was queued or in its batch, or before it arrived |
| `provider_disposed` | it arrived after disposal started |
| `shutdown_timeout` | disposal reached its deadline before the record was written |

`hostloom.logging.failures` counts the underlying failures by `component`: `formatter`, `sink`,
`destructurer`, `enricher`, `scope`, or `capture`.
