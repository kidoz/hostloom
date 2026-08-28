# HostLoom benchmarks

BenchmarkDotNet suites for the WebSocket envelope codecs, the logging pipeline, and object
mapping. Run them from the repository root on an otherwise idle machine.

## WebSocket protocols

```text
dotnet run --project benchmarks/HostLoom.Benchmarks -c Release -- --filter "*WebSocketProtocol*"
```

The encode and decode suites compare JSON, MessagePack, and protobuf-net for zero-byte, 256-byte,
and 4 KiB application payloads. `MemoryDiagnoser` reports managed allocations alongside throughput.

## Mapping

```text
dotnet run --project benchmarks/HostLoom.Benchmarks -c Release -- --filter "*Mapping*"
```

Compares `HostLoom.Mapping` against AutoMapper across four suites:

| Suite                            | Question it answers                                          |
| -------------------------------- | ------------------------------------------------------------ |
| `MappingBenchmarks`              | What does one map cost once everything is warm?               |
| `MappingCollectionBenchmarks`    | What does a batch of 100 or 1000 cost?                        |
| `MappingResolutionBenchmarks`    | What does a scope-resolve-map unit of work cost?              |
| `MappingRegistrationBenchmarks`  | What does each way of declaring the same pairs cost?          |
| `MappingStartupBenchmarks`       | What does cold start through the first mapped object cost?    |

Two contract shapes run through the first suite: a flat record of eight scalars, and a nested
record with a child object and a three-element child collection. Both are grouped by category, so
each shape gets its own baseline and ratio column.

### Strategy suites

Three further suites exist to choose between implementations rather than to compare libraries, and
they are kept so the choices stay falsifiable:

| Suite                            | Choice it settles                                            |
| -------------------------------- | ------------------------------------------------------------ |
| `MapManyStrategyBenchmarks`      | How `MapMany` should reach each element                       |
| `MappingInferenceBenchmarks`     | Whether an inferred pair should be cached per map class       |
| `MappingLifetimeBenchmarks`      | What the dispatcher's extra allocation is, and how to remove it |

What they concluded, on an M4 Max:

- **`MapMany` indexes through `IReadOnlyList<T>`.** A span fast path over `T[]` and
  `List<T>` — via `CollectionsMarshal.AsSpan` — was 2% faster at both 100 and 1000 elements with
  identical allocations. That is real but small, because the map calls dominate: 1000 maps at
  7.5 ns is 7.5 µs of an 8.1 µs total, so the access strategy is about 6% of the work. Two extra
  type checks and a span over `List<T>`'s internals are not worth 2%. Enumerating into a pre-sized
  `List<T>` was 3–5% slower and allocated marginally more.
- **An inferred pair is cached per map class.** Walking `GetInterfaces()` costs 8 ns and 32 B every
  registration; a static field on a generic type costs neither. That took registration of four maps
  from 111 ns and 680 B to 63 ns and 552 B — identical to restating the triple, so the ergonomic
  form is now free rather than merely cheap.
- **A stateless map used through the dispatcher should be a singleton.** The dispatcher's 112 B
  against an injected closed map's 88 B is the transient map class being constructed on every
  dispatch. Registering it singleton returns the allocation to 88 B exactly. It saves only 0.7 ns,
  though — the dispatch cost is the container lookup, not the construction.

### How the comparison is set up

The suites are meant to be read as an honest comparison, which means being explicit about where
the two libraries are not doing the same thing:

- **The flat shape is deliberately AutoMapper's best case.** Member names match exactly on both
  sides, so it is pure convention mapping — no `ForMember`, no custom resolver, nothing to look up
  at map time beyond the compiled plan.
- **AutoMapper is fully warm in the steady-state suites.** `CompileMappings()` runs during
  `GlobalSetup`, so expression compilation never lands inside a measured iteration. It is measured
  on purpose, and only, in `MappingStartupBenchmarks`.
- **The collection suite carries three rows, not two.** `HostLoom_Loop` is the hand-written loop an
  application writes by hand, `HostLoom_MapMany` is the shipped helper that exists to replace it,
  and `AutoMapper_Collection` is AutoMapper's built-in array map. All three produce the same number
  of destination objects. The row that matters is `MapMany` against `Loop`: the helper is only
  worth adopting if deleting a consumer's own extension class costs nothing.
- **The nested destinations differ by one allocation.** AutoMapper materialises a `List<T>` for the
  `IReadOnlyList<T>` child collection; the hand-written map allocates an exact-sized array. That is
  AutoMapper's own choice of destination type, not a handicap imposed by the benchmark, but it is
  why the nested allocation columns are not directly subtractable.
- **The closed-map rows have no AutoMapper counterpart.** AutoMapper exposes no per-pair service
  type, so `IMapper<TSource, TDestination>` resolved from the container is a shape only HostLoom
  can express. It is included because it is what the package README tells consumers to inject.
- **The two dispatchers have different lifetimes.** HostLoom registers `IMapper` scoped and
  resolves the closed pair from the scope on every call — which constructs the transient map class
  each time, and that shows up in its allocation column. AutoMapper's DI extension registers its
  own `IMapper`. Each library is measured as it ships, not reconfigured to match the other.
- **AutoMapper needs an `ILoggerFactory`.** It resolves one during configuration; the benchmark
  supplies `NullLoggerFactory` so logging stays out of the measurement.

### AutoMapper as a dependency

AutoMapper is referenced by this benchmark project only. Nothing under `src/` depends on it and
the project is `IsPackable=false`, so it never reaches a shipped package.

Version 16.2.0 is pinned in `Directory.Packages.props` for two reasons that cannot both be
satisfied. AutoMapper 14.0.0 is the last MIT release, but every version below 15.1.1 carries
[GHSA-rvv3-g6hj-g44x](https://github.com/advisories/GHSA-rvv3-g6hj-g44x), a high-severity DoS
advisory that fails this repository's `TreatWarningsAsErrors` build as `NU1903`. Every patched
version is licensed RPL-1.5 or commercial. The pin takes the patched version and accepts the
license, rather than suppressing a security advisory.

## Logging

```text
dotnet run --project benchmarks/HostLoom.Benchmarks -c Release -- --filter "*Logging*"
```

## Dry runs

For a quick build-and-discovery smoke run rather than statistically meaningful results:

```text
dotnet run --project benchmarks/HostLoom.Benchmarks -c Release -- --job Dry --filter "*Mapping*"
```

A dry job runs one cold-start iteration per benchmark, so its timings are dominated by JIT and are
not comparable. The allocation columns are still meaningful.

Do not treat a dry run, virtualized CI result, or one developer machine as a capacity claim. Run the
full job on deployment-like hardware and benchmark complete sessions separately before choosing
production connection and queue limits.
