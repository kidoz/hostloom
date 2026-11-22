# Regional catalog capacity replay

Use a regional catalog refresh as the first staging candidate. Each region owns a snapshot that can be rebuilt from its source. The example already has two implementations of the same operation, one with a semaphore and one with an actor mailbox. That makes it possible to measure what bounded admission costs and whether a slow region delays its neighbors. The earlier inbox trial found a delivery risk after cancellation and retained its existing implementation. [Catalog pilot](CatalogRefreshPilot.md) · [Inbox pilot](ActorInboxPilot.md)

## Run the comparison

From the repository root, build once and start a separate process for each configuration.

```sh
project=benchmarks/HostLoom.Benchmarks
trace="$project/CatalogCapacityFixture.csv"
dotnet build "$project" -c Release
dotnet run --project "$project" -c Release --no-build -- --catalog-replay semaphore "$trace" eu 4 5
dotnet run --project "$project" -c Release --no-build -- --catalog-replay actor "$trace" eu 4 5
dotnet run --project "$project" -c Release --no-build -- --catalog-replay actor "$trace" eu 8 5
```

The final two arguments select the actor's queued-message capacity per region and the number of replay cycles. The semaphore accepts waiting requests without a queue limit and ignores the capacity argument. Each cycle creates and disposes its owners. Each process includes its first cycle and runs subsequent cycles without forcing garbage collection.

Supply a UTF-8 CSV with the exact header below, unquoted fields, and one refresh per row. All numeric fields are integers. The case-sensitive region names may be pseudonyms.

| Column | Meaning |
|---|---|
| `at_ms` | Milliseconds since the first arrival. Start at zero, use nondecreasing offsets, and finish at a positive offset. Equal offsets form a burst. |
| `region` | Owner of this refresh. Include the specified hot region and at least one other region. |
| `source_bytes` | Size of the synthetic ASCII item returned by the source, in bytes before conversion to a managed string. Zero is valid. |
| `io_ms` | Nonnegative duration of the simulated asynchronous source read. |
| `caller_timeout_ms` | Positive caller wait budget starting at dispatch. The worker may publish the requested version after this deadline. |

```csv
at_ms,region,source_bytes,io_ms,caller_timeout_ms
0,eu,4096,50,10
10,us,512,1,100
```

Rows receive distinct increasing versions in file order. The fixture exercises new-version refreshes. Read-only traffic, repeated versions that coalesce, and source failures need additional representation if they are material to an adopter. The driver schedules each arrival from the original start time, so a slow response does not postpone the next offer. Inspect the maximum dispatch lag to see whether the generator kept pace with the trace. [Grafana load-model guide](https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/open-vs-closed/)

## Local fixture results

The fixture schedules 65 arrivals over 490 ms across three regions. Each of five waves starts a 50 ms source read in `eu` with a 10 ms caller deadline. Ten milliseconds later, eight more refreshes target `eu` and two each target `us` and `ap`. This produces 45 hot and 20 cold offers per cycle. [Fixture](CatalogCapacityFixture.csv)

The table shows five cycles per configuration on an Apple M4 Max, macOS 27.2.1 arm64, 16 logical CPUs, .NET 10.0.15, workstation GC. Counts are per cycle. Latency ranges show the smallest and largest cycle p99, and memory shows the largest sampled resident set across the five cycles. The fixture SHA-256 was `52c67d47cf728265c6aa56ab27a3bb0f07d12ace1866f104ffa7a177496e2682`. [All 15 cycle records](CatalogCapacityReplay-2026-11-23.csv)

| Implementation | Executed / refused | Caller timeouts | Cold successful p99 (ms) | All successful p99 (ms) | Peak RSS (MiB) |
|---|---:|---:|---:|---:|---:|
| Semaphore | 65 / 0 | 5 | 2.37–3.76 | 59.26–60.22 | 56.95 |
| Actor, capacity 4 | 45 / 20 | 5 | 2.38–3.97 | 50.52–53.36 | 59.83 |
| Actor, capacity 8 | 65 / 0 | 5 | 2.40–4.03 | 58.91–62.61 | 60.27 |

In every cycle all 20 cold loads finished during hot-region I/O, and the five slow requests executed after their callers timed out. No admitted work was discarded. The lower all-success p99 at capacity four excludes 20 refused requests. Capacity eight is the first setting to compare with the semaphore on an adopter's trace. This fixture provides no evidence of an actor latency advantage or a production capacity limit.

## Interpreting a run

- The header records the trace hash, implementation, owner count, hot request count, scheduled window and runtime. Each cycle reports actual offers per second up to the last dispatch. Successful latency starts at dispatch and includes the queue wait. The cold p99 pools all regions except the specified hot region. It uses nearest rank and is empty if there were no successful callers. With 20 cold successes the p99 equals the maximum.
- After drain, `offered = admitted + refused` and `admitted = executed + discarded`. Refusal means the mailbox was full. A discarded admitted request terminated before execution. Caller timeout is counted independently. The driver verifies each successful reply and reconciles the number of physical source loads with the final outcome of each request. A source or operation error invalidates the cycle.
- `drain_and_dispose_ms` measures time from the end of dispatch until outstanding work settles and the owners are disposed. Crash recovery requires a separate exercise. The process working set is sampled about every 10 ms and again after disposal. It includes the driver, trace, runtime, and workers. Short peaks between samples can be missed. Use post-drain readings across longer runs to investigate memory growth. [Microsoft working-set measurement](https://learn.microsoft.com/en-us/dotnet/api/system.diagnostics.process.workingset64)

## Qualifying an application

The owner of a service that already refreshes rebuildable, independently keyed snapshots should supply its current implementation and a representative peak window. Export relative arrival offsets, consistent anonymous region IDs, source sizes, measured I/O durations, and caller deadlines. Record how often requests read existing versions or overlap for the same version. If dropping that traffic changes queue pressure, extend the replay before comparing implementations.

Before the staging run, record who can approve adoption, the deployment CPU and memory limits, the measurement window, the required cold and overall p99, acceptable refusal and timeout rates, the resident-memory ceiling, and shutdown and recovery deadlines. Replay the same input against the current implementation and a mailbox-capacity sweep in separate processes. Select the smallest capacity meeting the agreed budgets across repeated peak windows, or retain the existing implementation if it better serves the workload. [Google SRE Workbook on setting service-level objectives](https://sre.google/workbook/implementing-slos/)
