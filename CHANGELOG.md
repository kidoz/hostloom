# Changelog

All notable changes to HostLoom are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Package versions
are derived from release tags at publish time.

## [Unreleased]

### Added

- `HostLoom.Analyzers`, an opt-in Roslyn analyzer package that reports omitted available
  cancellation tokens on HostLoom async calls (`HLM0001`), synchronous blocking over HostLoom
  `Task` and `ValueTask` operations (`HLM0002`), and singleton dependency-injection registration
  of request handlers, event handlers, or request behaviors (`HLM0003`).
- Logging pipeline health metrics on the `HostLoom.Logging` meter: dropped records with reasons,
  queue depth, blocked enqueues and their duration, component failures, and writer state.
- A bounded logging shutdown deadline (`ShutdownTimeout`, default 5 seconds) and an optional
  bound on blocking enqueues (`EnqueueTimeout`), both counted rather than silent when they expire.
- A configurable `TimeProvider` for log timestamps, read per event on the calling thread.
- A deterministic field-name collision policy: duplicate names collapse by source precedence
  (event holes over scopes over enrichers over static fields, last occurrence wins within a
  source), user names beginning with `@` are escaped CLEF-style to `@@`, names a formatter
  reserves for its own schema are dropped rather than duplicated, and configurable name-length
  and per-record field caps apply — every dropped field counted in
  `hostloom.logging.fields.dropped`.

- Structured fields from the standard `ILogger` path: template holes from `LogInformation`-style
  calls, `LoggerMessage.Define`, source-generated logger methods, and custom key/value state are
  captured as typed fields with no call-site changes; `{OriginalFormat}` is preserved as the
  message template for template-aware formatters, and Serilog-style `@`/`$` operator prefixes
  are stripped from emitted names.
- Bounded `{@...}` destructuring: object graphs serialize into nested typed JSON on the calling
  thread, cycle-safe, with configurable caps on depth, collection items, object members, string
  length, and encoded bytes per record — every cap cut marked with an explicit sentinel, and any
  getter or serializer failure emitting `"[DestructuringFailed]"` instead of ever falling back
  to `ToString()`.
- Logging benchmarks against the deployed Serilog shape (`CompactJsonFormatter` on the calling
  thread): template with two scalar holes on the fast and interface paths, a destructured
  mid-size contract, an event enriched with eleven trace properties, a scoped event, a disabled
  level, and formatter-only throughput for the background half.
- Configuration binding for the logging provider: an `AddHostLoomLogging` overload binds
  `HostLoomLoggerOptions` from a configuration section (canonically `HostLoom:Logging`), with
  the code callback applying after configuration and unknown or invalid keys failing at host
  startup. Level filtering remains standard MEL `Logging` configuration.
- `HostLoomBootstrapLogger`, a synchronous pre-DI logger emitting the same event shape as the
  hosted provider — same formatter, typed fields, masking policy, timestamps, static fields,
  and enrichers — written to stdout on the calling thread with a construction-time minimum
  level, explicit flush/disposal, and failures swallowed unless fail-fast is opted into.
- A `ClefLogFormatter` emitting Compact Log Event Format shaped after Serilog's
  `CompactJsonFormatter`: `@t`, `@mt` when a template exists (`@m` only when none does), `@r`
  renderings for formatted fast-path holes, `@l` omitted for Information with Serilog level
  names otherwise, `@x` as the complete exception chain, `@tr`/`@sp`, `SourceContext`,
  `ThreadId`, `EventId` in the Serilog provider's property shape, and every captured field as a
  top-level typed property. `@i` is deliberately not emitted.
- Full exception chains in both formatters: `Exception.ToString()` semantics (inner exceptions
  and aggregate children included), bounded by a configurable cap with an explicit truncation
  marker, and guarded so an exception whose own `ToString` throws cannot fault the writer.
- MEL scope support: the provider implements `ISupportExternalScope` (with a standalone fallback
  so `BeginScope` works without a logger factory), scopes are snapshotted on the calling thread,
  structured scope pairs flatten into typed fields where inner scopes override outer ones and
  event holes override both, and templated or non-structured scopes keep their rendered text in
  a `Scope` array in outer-to-inner order. A throwing scope is counted, never thrown.
- Producer-side enrichment: `ILogEnricher` implementations registered on the options run on the
  calling thread before queueing — where `AsyncLocal` ambient context is still visible — writing
  typed fields through `LogEntryWriter` (no raw JSON injection possible). Enrichers run in
  registration order, a throwing enricher is counted and skipped without costing the event, and
  event holes outrank enricher fields.
- Static enrichment fields: `Environment.MachineName` attached by default (Serilog
  `WithMachineName` parity, opt-out via `AttachMachineName`) and a configurable `ServiceName`,
  both UTF-8-encoded once at provider start and carrying the lowest collision precedence.
- Fail-closed PII protection: `[NotLogged]` omits a property or field entirely at every nesting
  level (never read, including inherited members), `[LogMasked]` replaces or deterministically
  part-reveals values, a registration-time per-type policy covers unannotatable types, and
  legacy `Destructurama.Attributed` annotations are honored by name without the dependency.
  Events with `@` holes render their message from the protected representations, so a record
  type's generated `ToString()` cannot leak excluded members through the message text.

### Changed

- Structured log fields keep their JSON value kinds: numeric and boolean interpolation holes are
  emitted as JSON numbers and booleans, a formatted hole keeps its rendering in the message while
  the field stays typed, and `DateTimeOffset` holes default to ISO-8601.
- `LogFast` through a wrapped (dependency-injected) logger now hands structured key/value state
  to the standard interface, so captured hole names survive into any structured provider instead
  of being flattened away with the rendered string.
- `ILogSink.Write` now receives a `CancellationToken` so shutdown can abandon a stalled sink.
- An unexpected formatter or sink failure faults the logging pipeline instead of silently killing
  the writer: queued and later records are counted as dropped, and no caller can block on a dead
  writer.
- Log timestamps follow operating-system clock corrections instead of drifting from a Stopwatch
  anchor captured at startup.

### Fixed

- A stray `OperationCanceledException` from a formatter or sink faults the logging pipeline
  instead of silently stopping the writer while producers still see it as running.
- Sink disposal is bounded by the logging shutdown timeout, so a sink that hangs inside its own
  flush-on-dispose cannot hang application shutdown.
- The logging writer runs on a dedicated background thread, so a stalled synchronous sink write
  no longer occupies a thread-pool worker.
- Records formatted but not yet accepted by the sink are counted when the pipeline faults or
  shutdown abandons the writer.
- Logging options are validated at provider construction: queue capacity, batch size, queue-full
  policy, time provider, and both timeouts fail fast with actionable errors.

## [0.1.0] - 2026-08-25

### Added

- A transport-neutral request/response runtime with typed contracts, behaviors, scoped handlers,
  correlation, remote faults, JSON wire envelopes, host lifecycle integration, diagnostics, health
  checks, and an execution-free pipeline probe.
- Typed event publishing and named subscriptions, with fan-out semantics implemented by the
  in-memory, RabbitMQ, and Kafka transports.
- A composable asynchronous pipeline package with conditional branches, retry, circuit breaking,
  rate and concurrency limits, cooperative timeouts, short-circuits, immutable probes, metrics,
  and tracing.
- Dependency-injection pipeline registration with named stages, private transient filter
  registrations, per-run scopes and feature toggles, startup validation, topology reporting, and
  per-filter instrumentation.
- Deterministic pipeline testing helpers and harnesses, including asynchronous validation and
  disposal support, plus a runnable document-indexing example.
- An authenticated raw WebSocket RPC and live-subscription gateway with JSON, MessagePack, and
  Protocol Buffers subprotocols, bounded per-connection memory, and explicit subscription credit.
- Allocation-free UTF-8 logging compatible with `Microsoft.Extensions.Logging`, with bounded
  buffering, drop accounting, trace capture, and benchmarks.

### Compatibility

- Targets .NET 10 and C# 14. This is the first public release; the API is experimental and may
  change before 1.0.
- RabbitMQ and Kafka are optional transport packages. Core pipelines and the in-memory transport
  do not require an external broker.

[Unreleased]: https://github.com/kidoz/hostloom/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/kidoz/hostloom/releases/tag/v0.1.0
