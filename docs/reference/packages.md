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
| `HostLoom.Logging` | Allocation-free UTF-8 logging provider |
| `HostLoom.Diagnostics` | Composition ledger and startup report of registration decisions |
| `HostLoom.Mapping` | Explicit, compile-time-safe, AOT-friendly object mapping |
| `HostLoom.Mapping.DependencyInjection` | Scoped mapper dispatch and explicit map registration |
| `HostLoom.Mapping.Testing` | Container-free mapper composition for tests |
| `HostLoom.Analyzers` | Compile-time checks for asynchronous, DI, and mapping usage |

Install only the runtime and transport the application needs:

```text
dotnet add package HostLoom.Transport.RabbitMq
```

The analyzer package is optional and has no runtime dependency:

```text
dotnet add package HostLoom.Analyzers
```

## Dependency edges

- `HostLoom` depends on `HostLoom.Pipelines`; each transport depends on
  `HostLoom`.
- `HostLoom.Pipelines.DependencyInjection` depends on `HostLoom.Pipelines`;
  `HostLoom.Pipelines.Testing` depends on both.
- `HostLoom.Diagnostics`, `HostLoom.Logging`, and the `HostLoom.Mapping.*`
  trio are independent of the messaging core and usable on their own.

## AOT compatibility

`HostLoom.Diagnostics`, `HostLoom.Mapping`,
`HostLoom.Mapping.DependencyInjection`, and `HostLoom.Mapping.Testing`
enable the .NET SDK Native AOT and trimming analyzers
(`IsAotCompatible=true`).

## Toolchain

| Item | Value |
| --- | --- |
| Target framework | `net10.0` (analyzers: `netstandard2.0`) |
| Language version | C# 14 |
| SDK pin | 10.0.400 (`global.json`, `rollForward: latestPatch`) |
| License | MIT |
