# Packages

Runtime packages target .NET 10; the compiler-hosted analyzer targets
`netstandard2.0`. All packages are versioned together — the release
workflow derives the version from the git tag, and the latest release is
recorded in `CHANGELOG.md`.

| Package | Purpose |
| --- | --- |
| `HostLoom` | Typed request/response and event runtime |
| `HostLoom.Pipelines` | Transport-neutral asynchronous pipelines |
| `HostLoom.Pipelines.DependencyInjection` | Named stages, per-run resolution, and instrumentation |
| `HostLoom.Pipelines.Testing` | Deterministic pipeline test doubles and harnesses |
| `HostLoom.Transport.InMemory` | In-process request and event transport |
| `HostLoom.Transport.RabbitMq` | RabbitMQ request and fan-out event transport |
| `HostLoom.Transport.Kafka` | Kafka request and consumer-group event transport |
| `HostLoom.AspNetCore.WebSockets` | Authenticated WebSocket RPC and subscriptions |
| `HostLoom.AspNetCore.WebSockets.Testing` | In-process TestServer client for gateway tests |
| `HostLoom.Logging` | Allocation-free UTF-8 logging provider |
| `HostLoom.Composition` | Explicit DI plan runtime and passive application reports; generator under development |
| `HostLoom.Diagnostics` | Composition ledger and startup report of registration decisions |
| `HostLoom.Mapping` | Explicit, compile-time-safe, AOT-friendly object mapping |
| `HostLoom.Mapping.DependencyInjection` | Scoped mapper dispatch and explicit map registration |
| `HostLoom.Mapping.Testing` | Container-free mapper composition for tests |
| `HostLoom.Caching` | Two-tier cache contracts, in-process stores, single-flight, fail-open composition |
| `HostLoom.Caching.DependencyInjection` | Cache registration, options validation, warmup, health checks, and the `IDistributedCache` adapter |
| `HostLoom.Caching.Testing` | Container-free cache composition, recording and fault-injecting stores |
| `HostLoom.Caching.Pipelines` | Cache and deduplication filters for HostLoom pipelines |
| `HostLoom.Locking` | Distributed lock contracts, lease handles, retry policy, in-process provider |
| `HostLoom.Locking.DependencyInjection` | Lock registration, options validation, and health checks |
| `HostLoom.Locking.Testing` | Container-free lock composition, scripted, recording, and fault-injecting providers |
| `HostLoom.Locking.Pipelines` | Distributed-lock filter for HostLoom pipelines |
| `HostLoom.Redis` | Redis cache store, invalidation channel, lock provider, and health probes over one connection |
| `HostLoom.Analyzers` | Compile-time checks for asynchronous, DI, mapping, and caching usage |

Install only the runtime and transport the application needs:

```text
dotnet add package HostLoom.Transport.RabbitMq
```

The analyzer package is optional and has no runtime dependency:

```text
dotnet add package HostLoom.Analyzers
```

## Browser package

The repository contains the separately versioned ESM package
[`@hostloom/websocket-client`](https://github.com/kidoz/hostloom/tree/main/clients/hostloom-websocket-client).
It provides dependency-free TypeScript types, validation, and encoding for `hostloom.json.v1`, plus
Base64 UTF-8 JSON payload helpers. Its injectable connection core validates subprotocol and welcome
negotiation, exposes connection-state and server-frame observers, sends validated client frames,
and supports explicit close plus opt-in jittered exponential reconnect. Its request API allocates
stream identifiers, correlates responses and typed remote faults, enforces the welcome-advertised
concurrency limit, maps `AbortSignal` to a cancel frame, and never replays a request. Its
subscription API waits for gateway confirmation, automatically replenishes credit at a configurable
low watermark after an event listener attaches, maps cancellation to `unsubscribe`, and
resubscribes retained logical handles after a replacement welcome. Close code `1008` retries only
after the configured credential-refresh callback succeeds. The package uses an independent
`websocket-client-vX.Y.Z` release stream. Its GitHub workflow verifies and uploads the immutable
tarball before publishing it from the protected `npm` environment through npm trusted publishing;
only the first publication requires a temporary bootstrap token because npm cannot attach an OIDC
publisher to a package that does not yet exist.

The package's conformance tests consume the schema and exact fixtures from
`HostLoom.AspNetCore.WebSockets` instead of copying the wire contract.

## Dependency edges

- `HostLoom.Composition` currently provides the explicit plan runtime and depends only on
  `Microsoft.Extensions.DependencyInjection.Abstractions`. It does not reference diagnostics,
  hosting or the messaging core. The bundled compile-time generator is not implemented yet.

- `HostLoom` depends on `HostLoom.Pipelines`; each transport depends on
  `HostLoom`.
- `HostLoom.Pipelines.DependencyInjection` depends on `HostLoom.Pipelines`;
  `HostLoom.Pipelines.Testing` depends on both.
- `HostLoom.Diagnostics`, `HostLoom.Logging`, and the `HostLoom.Mapping.*`
  trio are independent of the messaging core and usable on their own.
- `HostLoom.Caching` and `HostLoom.Locking` are kernels that reference only
  `Microsoft.Extensions.Logging.Abstractions` and compose without a container;
  each `*.DependencyInjection` package depends on its kernel plus the
  Microsoft dependency-injection, options, hosting, and health-check
  abstractions. None of the four references the messaging core,
  `HostLoom.Pipelines`, or `HostLoom.Diagnostics`, and the two kernels never
  reference each other.
- `HostLoom.Caching.Testing` and `HostLoom.Locking.Testing` depend on their
  kernel only, as `HostLoom.Mapping.Testing` does.
- `HostLoom.Caching.Pipelines` depends on `HostLoom.Pipelines` and
  `HostLoom.Caching`; `HostLoom.Locking.Pipelines` on `HostLoom.Pipelines`
  and `HostLoom.Locking`. Neither references a `DependencyInjection` package
  or the messaging core.
- `HostLoom.Redis` depends on both `DependencyInjection` packages, for its
  `UseRedis` extensions, and on `StackExchange.Redis`; its store, channel,
  and provider keep public constructors over a multiplexer, so they compose
  without a container as the kernels do.

## AOT compatibility

`HostLoom.Diagnostics`, `HostLoom.Mapping`,
`HostLoom.Mapping.DependencyInjection`, `HostLoom.Mapping.Testing`,
`HostLoom.Caching`, `HostLoom.Caching.DependencyInjection`,
`HostLoom.Caching.Testing`, `HostLoom.Caching.Pipelines`, `HostLoom.Locking`,
`HostLoom.Locking.DependencyInjection`, `HostLoom.Locking.Testing`, and
`HostLoom.Locking.Pipelines` enable the
.NET SDK Native AOT and trimming analyzers (`IsAotCompatible=true`).
`examples/HostLoom.Examples.CachingAot` publishes with `PublishAot=true`
and exercises a serialized cache round trip through a source-generated
`JsonSerializerContext`, so the caching and locking packages are verified
under Native AOT rather than only analyzed. `HostLoom.Redis` does not claim
`IsAotCompatible`: StackExchange.Redis is not annotated for trimming, and the
package states that rather than asserting a compatibility it cannot verify.

## Toolchain

| Item | Value |
| --- | --- |
| Target framework | `net10.0` (analyzers: `netstandard2.0`) |
| Language version | C# 14 |
| SDK pin | 10.0.400 (`global.json`, `rollForward: latestPatch`) |
| License | MIT |
