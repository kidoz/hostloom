# Changelog

All notable changes to HostLoom are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Package versions
are derived from release tags at publish time.

## [Unreleased]

Upgrading adds two analyzer rules — `HLM0007` and `HLM0008` — which report as warnings by
default. A project building with `TreatWarningsAsErrors` will fail until each is addressed or
its severity is set in `.editorconfig`; that is the intended effect, but it is a build break on
upgrade rather than a silent change.

### Added

- The `hostloom.json.v1` WebSocket contract now matches its documented web defaults: camelCase
  frame kinds and omitted null optional fields. It ships a JSON Schema and exact frame fixtures,
  reads kinds case-insensitively, rejects numeric or unknown kinds, and retains Base64 opaque
  application payloads.
- Same-origin WebSocket handshake validation by default, exact-origin allowlists, an explicit
  missing-Origin policy for browser-only versus native clients, and a replaceable
  `IWebSocketOriginValidator`. Validation uses ASP.NET Core's effective scheme and host and rejects
  before accepting the upgrade.
- `HostLoom.AspNetCore.WebSockets.Testing`, a protocol-aware `WebSocketTestClient` over ASP.NET Core
  TestServer for driving frames and awaiting common gateway responses in consumer integration tests.
- Credential-bounded WebSocket sessions driven by `TimeProvider`, with a 12-hour maximum lifetime,
  1008 `session_expired` closure, read-only `IWebSocketSessionDirectory` snapshots, and
  `IWebSocketSessionControl` for logout or role-change disconnects by session id or subject. Host
  shutdown now waits for sessions to receive 1001 `server_shutdown` before broker listeners stop,
  and per-session control-frame rate limiting closes floods with 1008 `rate_limited`.
- Key-aware WebSocket topic authorization: `WebSocketTopicResource.Key` exposes the requested
  subscription key to ASP.NET Core policies, and `TopicKeyPolicy.SubjectOnly` provides an
  authenticated, exact subject-to-key policy that rejects foreign or missing keys before
  subscription registration.
- Scoped `IWebSocketTopicSnapshotProvider<TEvent>` registration with deterministic
  `subscribed` → snapshot → concurrent-live ordering. Snapshot events reuse the v1 `event` frame
  with sequence zero, consume credit, support credit and unsubscribe during asynchronous loading,
  filter keyed snapshots, and reserve concurrent live frames against the existing connection
  bounds. Provider failures fault only the subscription with `snapshot_failed`.
- A machine-guarded WebSocket fan-out regression baseline for one serialized 256-byte payload sent
  through JSON envelope encoding, subscription credit, and ready-writer bounded queue cycles at 1,
  100, and 500 sessions, with explicit separation from real-socket capacity evidence.
- Live subscription credit replenishment no longer signals a completed snapshot initializer, which
  removes one caught `SemaphoreFullException` allocation per zero-to-positive refill.

- `HLM0007` and `HLM0008` in `HostLoom.Analyzers`, and coverage of the cache and lock contracts by
  `HLM0001` and `HLM0002`. `HLM0007` reports a cache or lock key built from a parameter, local,
  field, or property whose name says it is a credential (`token`, `secret`, `password`,
  `refreshToken`, `apiKey`) through interpolation, concatenation, or `string.Format`, `Concat`,
  or `Join`, unless the value is wrapped in `CacheKey.FromSensitive` or `LockKey.FromSensitive`:
  a key reaches the store, the logs, and the spans, and the helper hashes the secret so the key
  stays unique without carrying it. `HLM0008` reports a factory passed to
  `ICache.GetOrCreateAsync` that names its `CancellationToken` parameter and never uses it, so
  the work it starts would outlive the caller while holding the per-key guard; a `_` parameter is
  the deliberate opt-out. `HLM0001` (omitted cancellation token) and `HLM0002` (blocking on an
  asynchronous call) now recognise `ICache`, `IDistributedLock`, and `ILockHandle` by name, so
  they apply to those contracts whether the call goes through the package or a test stub.

- `HostLoom.Caching`, a two-tier cache kernel: the consumer contract `ICache` with get-or-create,
  a state-carrying overload that captures nothing on an in-process hit, `TryGetAsync` returning a
  `CacheLookup<T>` that distinguishes a cached default value from a miss, tags, bulk reads,
  set-if-absent, and warmup; the backend contract `IDistributedCacheStore` over opaque keys and
  byte payloads with a `CacheStoreException` and `CacheFailureKind` as the only failure shape;
  `ICacheValueSerializer` with a `System.Text.Json` implementation that resolves contracts from
  the injected `TypeInfoResolver` and never calls the reflection overloads; the in-process tier
  `LocalCacheStore`, the in-process second tier `InMemoryDistributedCacheStore` that also acts as
  an invalidation channel, and `TieredCache`, the composition. A distributed-store failure never
  reaches a consumer as an exception from a read or a get-or-create: the cache degrades to the
  factory, keeps the in-process tier, records a metric, and logs one warning per key per interval.
  Every kind of key lives in its own domain under `{namespace}:cache:`, so a consumer key cannot
  collide with a stampede lease or a tag index. The kernel references only
  `Microsoft.Extensions.Logging.Abstractions`, is `IsAotCompatible`, and composes with `new`.
- `HostLoom.Caching.DependencyInjection`: `AddHostLoomCaching` with a `CachingBuilder` that
  chooses exactly one store (`UseInMemory()` or `UseStore<TStore>(name)`, a second choice throws
  naming the first), the serializer (`UseSystemTextJson`, which requires a type-info resolver,
  `UseSerializer<T>`, and the annotated `UseReflectionSerialization` opt-out), warmups that run
  in the background after startup with a readiness contributor governed by
  `Caching:Warmup:BlocksReadiness`, and a readiness check that asks the store's health probe.
  Options are validated at startup, every message naming the option key.
- `HostLoom.Locking`, a distributed lock kernel: `IDistributedLock` with a `ValueTask`,
  token-aware execute and `TryAcquireAsync` returning an `ILockHandle` that exposes `IsHeld`,
  `LeaseEnd`, a `LostToken` cancelled when the lease is lost, and `ExtendAsync`; the backend
  contract `ILockProvider` with owner tokens and compare-and-set release and extend;
  `LockRetryPolicy` with immediate, interval, linear, and exponential shapes plus additive jitter,
  defaulting to ten linear retries at a 50 ms step; typed outcomes `LockNotAcquiredException`,
  `LockProviderUnavailableException`, and `LockReentrancyException`; `Locking:Enabled = false`
  as a visible single-instance mode; and `InMemoryLockProvider` with real lease expiry on a
  `TimeProvider`. The lock is coordination, not correctness, for persisted state, and its
  documentation says so.
- `HostLoom.Locking.DependencyInjection`: `AddHostLoomLocking` with a `LockingBuilder`
  (`UseInMemory()`, `UseProvider<TProvider>(name)`, `AddHealthChecks()`), startup validation,
  and the same exactly-one rule as caching.
- Meters and activity sources named `HostLoom.Caching` and `HostLoom.Locking`, with
  `hostloom.cache.*` and `hostloom.lock.*` instruments tagged by namespace, and execution-free
  `CachingProbe` and `LockingProbe` descriptions in the spirit of `HostLoomProbe`.
- `tests/HostLoom.Conformance`, backend-neutral cache and lock scenarios with a manual clock and
  fault-injecting decorators, run by the unit suite on the in-process backends both composed with
  `new` and through the container, and reusable by the integration suite on a real backend.
- `examples/HostLoom.Examples.CachingAot`, a Native AOT sample that publishes without trim or AOT
  warnings and executes a serialized cache round trip through a source-generated
  `JsonSerializerContext`.
- `HostLoom.Redis`, the Redis backend for caching and locking over StackExchange.Redis 3.1.31.
  `UseRedis()` on either builder registers `RedisOptions`, validates them at startup, and creates
  one connection per process lazily; calling it on both builders shares that connection.
  `RedisCacheStore` uses `SET … PX`, `GET` with `PTTL`, `MGET`, `UNLINK`, and `SET … NX PX`, and
  keeps tag indexes as sets that expire with their longest member; `RedisCacheInvalidationChannel`
  fans invalidations out over `{namespace}:cache:invalidate` and keeps subscribing with backoff
  while Redis is unreachable; `RedisLockProvider` acquires with `SET … NX PX` and releases and
  extends through Lua compare-and-set. `Redis:FailFast` is false by default: an unreachable Redis
  lets the host start, readiness reports unhealthy, the cache serves from its in-process tier and
  factories, and the lock raises `LockProviderUnavailableException`, until the connection recovers.
  `UseHashTags` wraps the namespace for Redis Cluster slot affinity; a password never reaches a
  log or probe line. Meter `HostLoom.Redis` with `hostloom.redis.connection.state` and
  `hostloom.redis.reconnects`.
- The Redis conformance run: `tests/HostLoom.IntegrationTests` executes every shared cache and
  lock scenario against the `redis:7.4` service that `docker-compose.yml` now provides, on the
  wall clock, composed with `new` and through the container, plus backend tests for the key
  layout, hash tags, readiness, cross-connection invalidation, and re-subscription after the
  server kills the pub/sub connection. The suite skips honestly without a listener and fails
  under `HOSTLOOM_REQUIRE_BROKERS=1`.
- Server-side invalidation on Redis. `Caching:Invalidation:Mode` now does what it says:
  `Tracking` registers `CLIENT TRACKING ON REDIRECT … NOLOOP` to the process's subscriber
  connection, so an entry any other client modifies, deletes, expires, or evicts leaves the
  in-process tier without anyone publishing; `Broadcast` subscribes to keyspace notifications for
  the namespace's entries or the configured prefix filters; `Auto` picks tracking on Redis 6.0 or
  later from the server version and broadcast below that. Tracking is registered again after a
  reconnect, both re-establishments count on `hostloom.cache.invalidation.resubscribed`, a mode
  that cannot be enabled after `Redis:MaxClientCommandRetries` attempts falls back to the explicit
  channel with one warning, and `CachingProbe` reports the transport in effect. The package
  applies the client-side `allowAdmin` flag and RESP2 to its connection, because StackExchange.Redis
  gates `CLIENT` commands behind the former and only keeps a dedicated subscriber connection under
  the latter. Proven against Redis 7.4: automatic selection, another connection's write, the
  connection's own write ignored, server-side expiry, re-registration after the server kills the
  connections, and a second instance's in-process entry evicted by an overwrite.
- `CachingBuilder.AddDistributedCacheAdapter()`: `IDistributedCache` and `IBufferDistributedCache`
  over the chosen distributed store, so `HybridCache` and other asynchronous Microsoft consumers
  share a HostLoom backend. Entries live under `{namespace}:cache:external:` apart from the tiered
  cache's own; the synchronous members throw `NotSupportedException`; `RefreshAsync` is a no-op
  because the store has no touch operation, so sliding windows become absolute; store failures
  answer as misses and log rather than throw. Proven with `HybridCache` reading through a second
  provider what the first wrote.
- `HostLoom.Caching.Testing` and `HostLoom.Locking.Testing`: `TestCache.InMemory()` and
  `TestCache.Tiered(store, serializer)`, `TestLock.Create()`, the `FaultingCacheStore` and
  `FaultingLockProvider` decorators that fail the next `n` calls or every call with a chosen kind,
  `RecordingCacheStore` and `RecordingLockProvider` that record every call, and
  `ManualLockProvider` whose `Hold` and `Release` script the key another instance would own. Both
  reference their kernel only and are `IsAotCompatible`; the conformance suite now builds on them.
- `HostLoom.Caching.Pipelines` and `HostLoom.Locking.Pipelines`, filters for `HostLoom.Pipelines`
  over the caching and locking kernels, with `HostLoom.Pipelines` itself staying dependency-free.
  `UseCache<TContext, TPayload>` is get-or-create around the rest of the pipe: a hit puts the
  cached payload on the context and stops, a miss runs the pipe and caches the payload it leaves
  behind, and a `CacheFilterResult` records which happened. `UseDeduplication` claims an identity
  with an atomic set-if-absent before running and adds a `Deduplicated` payload instead of running
  again inside the window; when the store cannot answer it runs anyway with a
  `DeduplicationSkipped` payload, because processing twice is recoverable and dropping on an
  outage is not. `UseDistributedLock` runs the rest of the pipe under the lock for a key derived
  from the context and leaves a `HeldLock` payload whose token is cancelled on a lost lease when
  the options ask for it. Each filter also has a public constructor over the kernel service plus a
  small options object, so `HostLoom.Pipelines.DependencyInjection` resolves it through
  `AddFilter`, and each describes itself to `PipelineProbe`. Deduplication on the messaging
  receive pipeline is deliberately not wired.
- Documentation: `docs/reference/caching.md`, `docs/reference/locking.md`,
  `docs/how-to/cache-and-lock-fail-open.md`, and a caching and locking section in the README.
- Cache and lock benchmarks: the tracked HostLoom scenarios cover an in-process hit through the
  state-carrying overload, a distributed hit through the serializer, a miss under 100-way
  contention, a bulk read of 100 keys, and lock acquire/release and execute. Process-local cache
  comparisons add Microsoft `HybridCache` and FusionCache. A separate real-Redis project compares
  all three cache L2 paths and HostLoom locks against Medallion `DistributedLock.Redis`, failing
  setup when Redis is unavailable. A committed environment-checked baseline and command fail when
  deterministic HostLoom cache or lock mean time or allocations regress by more than 10%.

## [0.3.0] - 2026-08-29

Upgrading adds three analyzer rules — `HLM0004`, `HLM0005`, and `HLM0006` — which report
as warnings by default. A project building with `TreatWarningsAsErrors` will fail until each
is addressed or its severity is set in `.editorconfig`; that is the intended effect, but it
is a build break on upgrade rather than a silent change.

### Added

- `HostLoom.Mapping.Testing`, composing an `IMapper` dispatcher from explicit maps with no
  container, so a unit test does not have to build an `IServiceCollection` to obtain one. Maps are
  added as instances or as inline delegates. Duplicate pairs are rejected exactly as
  `MappingBuilder` rejects them, so a test cannot pass against a composition the container would
  refuse, and `Build` takes a snapshot so a dispatcher already handed to a test is unaffected by
  later additions. Substituting the dispatcher remains the worse option: it needs one substitute per
  pair, and each returns what the test told it to rather than what a map would.
- `MappedPairRegistry` and `IServiceCollection.GetMappedPairs()`, exposing the registered source and
  destination pairs so a service can assert its expectations while the container is still being
  composed, rather than discovering a missing pair on the first code path that needs it. One
  registry spans repeated `AddHostLoomMapping` calls and every registration overload.
- `MappingNotFoundException` now reports the destinations the requested source type *is* registered
  to map to, through a new constructor overload and a `RegisteredDestinations` property. The near
  miss is usually the diagnosis — a destination named one letter differently, or the pair registered
  in the other direction — so listing them turns reading the message into the fix.
- `AddHostLoomMapping` takes the dispatcher's `ServiceLifetime`. Scoped remains the default and the
  safe choice. Singleton lets an `IHostedService` take the dispatcher directly, and then every map
  must be registered singleton too — anything else is rejected at registration, because a singleton
  dispatcher resolves from the root provider and would retain each disposable map for the life of
  the process. Injecting a closed `IMapper<TSource, TDestination>` is still preferable, provided
  that map's own graph is singleton-safe. A singleton dispatcher does not silence `HLM0006`, which
  reports the shape rather than the lifetime it was registered with.
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

- A singleton mapping dispatcher now requires every map to be a singleton, and rejects anything
  else at registration. A singleton dispatcher resolves from the root provider, which never goes
  out of scope: a disposable map, or any disposable in its graph, was retained for the life of the
  process instead of released per unit of work, and a scoped dependency reached through a transient
  map was captured. Both were invisible at the call site. The documentation claiming a closed
  transient map "carries no restriction" in a singleton was wrong for the same reason — such a map
  is promoted to a singleton, so a scoped service inside it reproduces the Development-throws,
  Production-succeeds asymmetry `HLM0006` exists to prevent. `HLM0006`'s wording is corrected to
  say so.
- The factory registration overload is documented as the exception rather than the rule for closing
  a generic map. `Add<TEntity, TModel, EntityMapper<TEntity, TModel, TTranslation>>()` already
  closes a constructed generic from a generic helper, and being an implementation type it stays
  covered by `ValidateOnBuild`, where a factory body is opaque until the map is first resolved.
  Factories are for construction the container cannot perform.
- `MappedPairRegistry.Pairs` returns a read-only wrapper rather than the backing list, which could
  be downcast and appended to with pairs the container would never resolve.
- Inferring a mapping pair no longer re-reads `Type.GenericTypeArguments`, which allocates a fresh
  array on every access and was read twice per registration when recording the pair. Registering
  four maps by inference had become 3.6× the explicit form at 392 ns and 1128 B; the source and
  destination are now cached alongside the pair, and both forms cost 107 ns and 808 B. Inference
  being free is the only thing that justifies preferring the shorter registration.
- RabbitMQ no longer replaces a connection that automatic recovery owns. Observing `IsOpen: false`
  during a broker drop used to dispose the connection and build a new one, which cancelled the
  recovery that would have restored the channels, queues, and consumers created on it — leaving
  every listener and subscription permanently dead and silent while publishing carried on against
  the replacement. Only an application-initiated close is now treated as final; a peer or library
  shutdown is left to recover. The visible trade is deliberate: a publish attempted during the
  outage now fails loudly and transiently instead of succeeding at the cost of the consumers.
  `TopologyRecoveryEnabled` is stated explicitly rather than relied on as a default, because the
  listeners depend on it. Keeping the connection makes a second case reachable that replacing it
  had hidden: the reply queue is server-named and exclusive, so recovery re-declares it under a new
  name while the recovered channel reports itself open — nothing re-declared the reply path, and
  the cached name would have addressed a queue that no longer existed, timing out every later
  request on a connection that looked healthy. The broker now follows the rename.
- A WebSocket `Cancel` frame no longer escapes as `ObjectDisposedException` when it races the
  request it cancels. The request could complete and dispose its own cancellation source between
  the lookup and the cancel, and unlike the shutdown path — which already guarded this exact race —
  the frame path did not, so the exception passed the session's graceful-close handling and left
  the connection through ASP.NET. A client sending request/cancel pairs could provoke it.
- Kafka classifies both ways a required header can be absent as a malformed envelope. A record
  produced without headers carries a null collection, and `Headers.GetLastBytes` throws
  `KeyNotFoundException` rather than returning null for a missing key — so the existing null-check
  never fired and neither case reached the malformed path. Both were treated as transient faults
  and cost the record its full redelivery and backoff budget before being discarded, where a
  malformed envelope is committed and skipped immediately.
- Pipeline startup validation constructs only the filters a run would construct. A filter switched
  off for the environment through `EnabledWhen` was still built by the validator, so a host refused
  to start over a filter that would never execute — defeating the switch precisely when its
  dependencies were absent, which is the case it exists for. The validator also wraps any
  constructor failure in its guidance now, not only the container's `InvalidOperationException`.
- Kafka now skips only records classified as malformed HostLoom envelopes; an application handler
  that throws `InvalidDataException` follows the configured broker redelivery policy instead of
  being committed immediately as poison data.
- Usage analyzers identify framework assemblies through generated assembly metadata, preventing a
  consumer such as `HostLoom.OrderService` from being analyzed as framework code while ensuring
  future packages under `src/` are included automatically.

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
