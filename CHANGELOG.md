# Changelog

All notable changes to HostLoom are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Package versions
are derived from release tags at publish time.

## [Unreleased]

### Added

- `HLM0004` and `HLM0005`, completeness analysis for explicit maps, closing the one axis on which
  an explicit map is weaker than the convention mapping it replaces: a destination member that is
  simply never assigned compiles, passes review, and ships as silent data loss. `HLM0004` reports
  members a map never assigns; a member supplied through the destination's constructor counts as
  assigned, so positional records need nothing extra. `HLM0005` reports a map whose body the
  analysis cannot read, so that no diagnostic always means checked rather than skipped — every
  `Map` implementation lands in verified, not verifiable, or not applicable, the last being a
  destination with no settable public instance members or one that is itself a sequence. Two body
  shapes are verified: a destination constructed and returned directly, and one local constructed,
  assigned into across any number of statements and branches, then returned. Conditional assignment
  counts as assigned, because the rule targets forgotten members rather than conditional ones. A
  map whose destination is a type parameter cannot have its members enumerated and is skipped
  silently; that blind spot is documented in the analyzer README rather than left to be discovered.
- `HLM0006`, reporting a type that takes the scoped `IMapper` dispatcher through its constructor
  and is then registered as a singleton or a hosted service. The failure this replaces is the
  asymmetric one: a captured scoped service throws at host build where scope validation is enabled,
  the generic host's default in Development, and succeeds where it is not — so it fails in the
  environment least like production and hides in the one that matters. Only the non-generic
  dispatcher is reported; the closed `IMapper<TSource, TDestination>` the rule points at is
  transient and never flagged.
- `UnmappedMembersAttribute` in `HostLoom.Mapping`, naming the destination members a map leaves
  unset on purpose. Naming each one rather than marking the map incomplete is what keeps a member
  added to the contract later from being excused along with the deliberate omissions.

### Changed

- An inferred mapping pair is resolved once per map class and read from a static field afterwards,
  instead of walking `GetInterfaces()` on every registration. Registering four maps went from 111 ns
  and 680 B to 63 ns and 552 B — identical to restating the type triple, so choosing the shorter
  registration form no longer costs anything. Inference failures are stored rather than thrown from
  the static constructor, which would otherwise reach the caller as a `TypeInitializationException`
  wrapping the real diagnostic, and would cache that wrapping for every later attempt.
- Benchmark suites that settle an implementation choice rather than compare libraries:
  `MapManyStrategyBenchmarks` measured a span fast path over `T[]` and `List<T>` at 2% against the
  `IReadOnlyList<T>` indexing that ships, which does not pay for two extra type checks and a span
  over `List<T>`'s internals — the map calls are roughly 94% of the work at 1000 elements.
  `MappingLifetimeBenchmarks` established that the dispatcher's extra 24 B is the transient map
  class constructed per dispatch, and that registering a stateless map as a singleton returns its
  allocation to exactly that of an injected closed map.

### Fixed

- Kafka now skips only records classified as malformed HostLoom envelopes; an application handler
  that throws `InvalidDataException` follows the configured broker redelivery policy instead of
  being committed immediately as poison data.
- Usage analyzers identify framework assemblies through generated assembly metadata, preventing a
  consumer such as `HostLoom.OrderService` from being analyzed as framework code while ensuring
  future packages under `src/` are included automatically.

### Added

- `MappingBuilder.Add<TSource, TDestination>(Func<IServiceProvider, IMapper<TSource, TDestination>>)`
  registers a pair through a factory, which is how a generic map class is closed. A map generic in
  more than its source and destination — `EntityMapper<TEntity, TModel, TTranslation>` implementing
  `IMapper<TEntity, TModel>` — cannot be registered as an open generic, because the container
  requires the open service type and open implementation type to have equal arity and this shape
  never does. Closing the map at the call site instead keeps every type argument visible to the
  compiler, so the registration needs no `MakeGenericType` and both packages keep their trimming
  and Native AOT analyzers clean. Called from a generic helper, one map class registers many pairs
  in both directions; each registration remains a single closed descriptor, so the registered pairs
  stay enumerable and duplicate detection still spans it. A factory that returns null is reported
  as the factory's fault rather than surfacing as `MappingNotFoundException`, which would blame the
  registration for a pair that is in fact registered.

- Sequence and null-tolerant mapping in the core package, as extension methods on the closed
  mapper: `MapMany` and `MapManyOrEmpty` return `IReadOnlyList<TDestination>` sized in one
  allocation when the source reports a count, `MapManyDeferred` maps lazily for scans too large to
  materialize, and `MapOrNull` maps one value through null. The null policy is carried by the
  method name instead of by configuration, so unlike a convention mapper's global switch every
  call site that depends on null tolerance stays greppable. `MapManyDeferred` validates its
  arguments eagerly and only the mapping is deferred, so a null argument is reported at the call
  that passed it rather than at the first enumeration. The `HostLoom.Mapping` README documents the
  three ways AutoMapper's defaults differ, verified against AutoMapper 14 — including that
  `AllowNullCollections = false` rewrites null collection *members* to empty during ordinary object
  mapping, not only top-level collection maps, which is the case most likely to change behaviour
  silently during a migration.

- `MappingBuilder.Add<TMapper>()` registers a map class from the pair it already declares, read
  from the single closed `IMapper<TSource, TDestination>` it implements. A registration no longer
  restates a type triple the class carries on its interface, and the registering file needs no
  `using` for the contracts being mapped between — only for the map class. Inference reads type
  metadata once per registration and composes nothing, so both packages keep their trimming and
  Native AOT analyzers clean and the map dispatch path stays free of reflection. A class that
  implements no pair, or more than one, fails at registration with every candidate pair named and
  points at `Add<TSource, TDestination, TMapper>()`, which remains for choosing a pair explicitly
  and for closing an open generic map. Duplicate detection spans both overloads, since an inferred
  registration produces the same closed service type as an explicit one.

- Mapping benchmarks against AutoMapper across four suites: one map in steady state for a flat
  eight-scalar contract and a nested one with a child object and child collection; a batch of 100
  and 1000; a scope-resolve-map unit of work through the container; and cold start through the
  first mapped object, where a convention mapper's expression compilation lands. The flat shape is
  deliberately AutoMapper's best case — names match on both sides, so it is pure convention with no
  `ForMember` — and AutoMapper's execution plans are compiled during setup so the steady-state
  suites measure per-call cost rather than the cost of getting there. `GlobalSetup` asserts that
  both libraries produce equivalent destination values, so an incomplete map on either side fails
  the run instead of being reported as a faster one. AutoMapper is referenced by the benchmark
  project only; nothing under `src/` depends on it and the project is not packable.

## [0.2.0] - 2026-08-27

### Added

- `HostLoom.Diagnostics`, a composition ledger that records what each registration decided —
  which branch activated, why, and what was deliberately skipped — and reports the whole plan
  once at host start under the `HostLoom.Diagnostics.Composition` category: one `Information`
  manifest line for the composition, one `Debug` line per decision carrying its reason and the
  registration method that recorded it, and a `Warning` naming both choices whenever one
  component was recorded with choices that disagree. Collection is unconditional and passive, so
  a library can record without imposing anything: nothing is written until an application calls
  `AddCompositionDiagnostics`, and that opt-in may come after the decisions it reports. The
  package is a standalone leaf that nothing else depends on, so an application that does not
  reference it carries nothing.
- `HostLoom.Mapping` and `HostLoom.Mapping.DependencyInjection`, explicit object mapping that keeps
  renames, required members, constructor changes, conversions, and nullability visible to the C#
  compiler and to code review, instead of deferring them to startup or to production. A map is an
  ordinary class implementing the closed generic `IMapper<TSource, TDestination>` and is invoked
  directly: nothing scans assemblies, inspects members for mapping, compiles expressions, emits
  code, or dispatches from `object` to `Type`, so both projects opt into the .NET SDK trimming and
  Native AOT analyzers without reflection annotations on mapped members.
- `AddHostLoomMapping` registers one map class per source and destination pair, transient by
  default so a map can take scoped dependencies through its constructor, with an overload that
  registers a prebuilt stateless map as a singleton. The non-generic `IMapper` dispatcher is
  scoped, so a scoped dependency cannot be promoted through the normal registration path;
  orchestration code coordinating several pairs writes `mapper.From(source).To<Destination>()`
  through an allocation-free source wrapper. A duplicate pair fails at registration rather than
  resolving ambiguously, and a missing pair throws `MappingNotFoundException` naming both types.
  Mapping stays synchronous and performs no I/O, and this first release deliberately omits
  reverse-map inference, flattening conventions, lifecycle callbacks, runtime dictionaries,
  polymorphic `object` dispatch, and provider-specific projections — query projections remain
  explicit `IQueryable.Select` expressions so a provider still receives the whole expression tree.
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

- Destructuring a `{@...}` hole no longer allocates a fresh buffer and `Utf8JsonWriter` per
  event: the writer demands multi-kilobyte chunks from its buffer writer, which had cost about
  5 KB of garbage per destructured event. Thread-local pooled scratch (with a reentrancy guard
  and a retention cap) cuts that to the unavoidable reflection boxing — roughly 15× less
  allocation and measurably faster than Serilog's capture on the same contract.

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

[Unreleased]: https://github.com/kidoz/hostloom/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/kidoz/hostloom/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/kidoz/hostloom/releases/tag/v0.1.0
