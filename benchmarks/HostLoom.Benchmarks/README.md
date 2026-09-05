# HostLoom benchmarks

BenchmarkDotNet suites for caching, distributed locking, WebSocket envelope codecs, and logging.
Run them from the repository root on an otherwise idle machine.

## Caching and locking

The process-local comparison covers an L1 hit and a real 100-caller miss stampede for HostLoom,
Microsoft `HybridCache`, and FusionCache:

```text
dotnet run --project benchmarks/HostLoom.Benchmarks -c Release -- --filter "*CacheLibrary*"
```

The HostLoom-only tracked suite covers an L1 hit, an in-memory serialized L2 hit, a 100-caller
miss, a 100-key bulk read, and in-memory lock acquire/release and execute-with-lock:

```text
dotnet run --project benchmarks/HostLoom.Benchmarks -c Release -- --filter "HostLoom.Benchmarks.CachingBenchmarks.*" "HostLoom.Benchmarks.LockingBenchmarks.*"
```

The Redis project measures warmed L2 hits for HostLoom, `HybridCache`, and FusionCache, plus
uncontended acquire/release for HostLoom and Medallion `DistributedLock.Redis`. It defaults to
`localhost:6379`; set `HOSTLOOM_BENCHMARK_REDIS` to a StackExchange.Redis configuration string for
another endpoint. The project verifies Redis with `PING` and fails setup when it is unavailable.

```text
dotnet run --project benchmarks/HostLoom.Redis.Benchmarks -c Release -- --filter "*"
```

For cache comparisons, each library uses System.Text.Json and the corresponding local tier is
disabled in the Redis suite. The process-local suite leaves each library's shipped L1 and
single-flight implementation in place. For locking, both libraries share one multiplexer and the
same uncontended key lifecycle. HostLoom automatic extension is disabled; Medallion keeps its
normal lease-loss behavior, so the benchmark measures the public production paths rather than
internal Redis primitives.

`HybridCache`, FusionCache, and `DistributedLock.Redis` are benchmark-only dependencies. Both
benchmark projects are non-packable, and no project under `src/` references a comparison library.

### Regression gate

The committed cache/lock baseline is intentionally separate from cross-library and Redis results:
only deterministic in-process HostLoom scenarios gate changes. Run the fixed BenchmarkDotNet
`ShortRun` and enforce the 10% ceiling for both mean time and allocated bytes with:

```text
just benchmark-cache-lock-check
```

The checker rejects missing/added cases, a different BenchmarkDotNet job, or a different CPU,
architecture, runtime, or BenchmarkDotNet version. That prevents results from incomparable
machines from silently replacing a performance signal. After an intentional performance change,
rerun on the baseline machine and review the diff produced by:

```text
just benchmark-cache-lock-update
```

The JSON reports under `BenchmarkDotNet.Artifacts/` are transient. The compact reviewed baseline
under `benchmarks/baselines/` is the historical result committed to the repository.

## WebSocket protocols

```text
dotnet run --project benchmarks/HostLoom.Benchmarks -c Release -- --filter "*WebSocketProtocol*"
```

The encode and decode suites compare JSON, MessagePack, and protobuf-net for zero-byte, 256-byte,
and 4 KiB application payloads. `MemoryDiagnoser` reports managed allocations alongside throughput.

### Bounded fan-out regression gate

The fan-out suite publishes one already-serialized 256-byte application payload to 1, 100, and 500
ready topic subscribers using `hostloom.json.v1`. Each subscriber performs the stream-specific
envelope encode, consumes one credit, and writes to its own one-frame, 64 KiB bounded outbound
queue. The ready writer immediately drains the frame and restores credit. Each measured invocation
runs 256 publish-and-drain cycles and BenchmarkDotNet normalizes the result per published event,
which reduces noise without allowing the bounded queue to accumulate work.

Run the fixed `ShortRun` and enforce the 10% ceiling for mean time and allocated bytes with:

```text
just benchmark-websocket-fanout-check
```

The checker also rejects any measured exception rate; caught exceptions are still hot-path work and
must not disappear inside aggregate timing or allocation columns.

After an intentional performance change, rerun on the baseline machine and review the update:

```text
just benchmark-websocket-fanout-update
```

The environment guard rejects results from a different CPU, architecture, runtime, BenchmarkDotNet
version, or job. This is an in-process ready-writer regression signal: it includes bounded queue
enqueue, dequeue, and credit restoration but excludes socket I/O, browser processing, network
latency, reconnects, and multi-node broker delivery. It therefore does not establish the
real-socket p99 capacity target by itself.

## Mapping

Mapping comparisons and strategy suites have their own
[benchmark project and guide](../HostLoom.Mapping.Benchmarks/README.md).

## Logging

```text
dotnet run --project benchmarks/HostLoom.Benchmarks -c Release -- --filter "*Logging*"
```

## Dry runs

For a quick build-and-discovery smoke run rather than statistically meaningful results:

```text
dotnet run --project benchmarks/HostLoom.Benchmarks -c Release -- --job Dry --filter "*Logging*"
```

For the Redis smoke run, use the Redis benchmark project and the same `--job Dry` option. A
reachable real Redis endpoint is still required.

A dry job runs one cold-start iteration per benchmark, so its timings are dominated by JIT and are
not comparable. The allocation columns are still meaningful.

Do not treat a dry run, virtualized CI result, or one developer machine as a capacity claim. Run the
full job on deployment-like hardware and benchmark complete sessions separately before choosing
production connection and queue limits.

## Composition startup and compiler measurements

The separate [composition executable](../HostLoom.Composition.Benchmarks/README.md) measures first
calls, isolated runtime phases, optional ledger work and incremental/clean compiler costs. Its
reviewed baseline and gate are independent of these BenchmarkDotNet suites.
