# Per-region catalog refresh pilot

**Decision. Use one actor per region in the opt-in catalog refresh example.** The actor met both latency budgets. Its practical advantage here is that the runtime keeps ownership of an admitted turn after the caller leaves, then drains it during shutdown. The semaphore also met the budgets and gave faster reads of an already-published snapshot.

## Criteria set before measurement

Each region owns one immutable catalog snapshot. A request for version V asks for V or a newer version. A read returns the current snapshot. Reads during an active refresh wait for that turn. The worker publishes version and contents together after a successful load. Expected source I/O failures return the previous snapshot and a failure outcome. The next refresh may retry.

Compare one HostLoom actor per region with one asynchronous `SemaphoreSlim` per region, using the same state transition, files and request trace. A caller's deadline bounds its wait. An admitted refresh can finish after its caller times out. Version-based reconciliation establishes whether the desired data is available. It makes no attribution to a particular caller.

| Criterion | Required observation |
|---|---|
| Duplicate refresh | Hold the first source read open and issue another refresh for the same version. Both callers receive the same snapshot. The source is read once. |
| Region independence | While one region's source is blocked, another region completes its refresh. |
| Source failure | Preserve version and contents, remain readable and allow a later successful retry. An older request must never roll the catalog back. |
| Timeout reconciliation | Expire an actual caller wait while its load is suspended. A read across the end of that load observes the newly published snapshot on success, and the previous snapshot on source failure. Reconciliation itself performs zero loads. |
| Latency | Median of five trial p99s at most 25 ms for refresh replies and 2 ms for current reads issued after the refresh burst. Every offered request must complete successfully in the measured trace. |

The latency budgets and workload are local pilot assumptions. Each wave requests one new version in four regions. Eight callers target the busiest region and two target each of the others. After all refresh replies, issue one current read per region. Run 12 warmup waves per implementation, then five alternating pairs of 60-wave trials with fresh workers. Use the same files backed by a temporary directory and add 8 ms per physical source read. Count source reads by region and version and verify each returned snapshot against independent expected contents. Declare adoption only if the actor passes the correctness and latency criteria and makes the worker's ownership guarantees easier to maintain.

## Observed behavior

- With the first EU load suspended, a second caller requested the same version. Both received the same immutable snapshot object from a single read. A US refresh completed before the EU source was released.
- When two callers overlapped a failing version-2 load, both received the same version-1 snapshot and the source-unavailable outcome. A subsequent retry loaded version 2. A later request for version 1 returned version 2 with zero additional I/O.
- A controlled clock expired a 25 ms caller wait after the source entered its read. The next current-version read waited for the turn. After a successful load it observed version 3 and its new contents. After a failed version-4 load it observed version 3 and its original contents. Each attempted version incurred one physical read.
- In a separate actor shutdown check, the original caller timed out, a current read entered the mailbox, and the system began stopping. New admission closed immediately. Once the source resumed, the pending read received the published version and shutdown completed.

Serial execution alone lets a queued duplicate retry immediately after a failure. During development the shared client was extended to keep one in-flight attempt per version until the operation replies. The final replay uses that client for both implementations and explicitly checks overlapping failures. Client deadlines bound individual waits. The shared operation has a separate five-second reply deadline. A missed operation deadline also leaves the underlying execution uncertain. The reconciliation contract answers whether the requested version is available when the read executes.

## Latency and work

Measured locally on 2026-11-22 with an Apple M4 Max, macOS 27.2.1 arm64, 16 logical CPUs, workstation GC, .NET SDK 10.0.404 and runtime 10.0.15, Release build. Both implementations read the same two-line files with `File.ReadAllLinesAsync` after the injected delay. The benchmark checks both items in every reply. The actor mailbox capacity was 32 per region.

| Across five measured trials | Actor | Semaphore |
|---|---:|---:|
| Successful refresh calls | 4,200 | 4,200 |
| Successful current reads | 1,200 | 1,200 |
| Physical source reads | 1,200 | 1,200 |
| Median trial p50 refresh | 9.23 ms | 9.21 ms |
| Median trial p99 refresh | 12.03 ms | 11.28 ms |
| Lowest–highest trial p99 refresh | 10.24–16.58 ms | 11.02–12.72 ms |
| Median trial p99 current read | 0.069 ms | 0.007 ms |
| Trials meeting both budgets | 5 of 5 | 5 of 5 |

Timings run from invocation through reply for each caller, including any wait for another caller's load. The next wave starts after the current reads finish. This closed-loop workload measures burst latency at the stated concurrency. Source delay dominated refresh time. The read budget covers reads after publication. A read queued behind a slow load can wait for that I/O.

## Repeat measurement

One later actor trial reached a refresh p99 of 33.3 ms. To check whether that recurred, we ran three more batches on the same Apple M4 Max on 2026-11-22 at revision `147f0497565c`. Each batch started a fresh Release process with the workload, alternating order, warmup and budgets described above. The batches ran sequentially on a shared workstation. One-second CPU samples during the runs showed 70–82% idle.

Each latency below is the median of five trial p99s, in milliseconds.

| Batch | Worker | Refresh p99 | Current-read p99 | Trials with refresh p99 > 25 ms |
|---|---|---:|---:|---:|
| 1 | Actor | 13.83 | 0.126 | 0/5 |
| 1 | Semaphore | 11.85 | 0.009 | 0/5 |
| 2 | Actor | 15.01 | 0.135 | 0/5 |
| 2 | Semaphore | 10.95 | 0.010 | 0/5 |
| 3 | Actor | 13.50 | 0.111 | 0/5 |
| 3 | Semaphore | 13.69 | 0.013 | 0/5 |

Across the 15 repeat trials per implementation, refresh p99 ranged from 10.50 to 21.29 ms for the actor and 9.69 to 22.63 ms for the semaphore. Every trial also met the 2 ms current-read budget. Each implementation completed 12,600 refresh calls and 3,600 current reads with exactly 3,600 physical loads. [Per-trial measurements](CatalogRefreshRepeat-2026-11-22.csv)

The earlier threshold exceedance did not recur. Its cause remains unknown. The pilot is closed for the regional catalog example under this workload.

## Adoption scope

Use the actor in this example because a caller may leave while work still owns the regional state. The actor provides a serialized asynchronous turn and a stop operation that closes admission and joins that turn. The comparison's semaphore implementation must hold and release its gate across I/O, separate the operation task from its caller's wait, and dispose gates only after the harness has joined outstanding work. Both implementations still need the same application logic for version checks, failure handling and sharing attempts. This decision applies to the local worker in an actor-enabled application. The measured budgets are pilot assumptions.

Run the example, its regression tests and the timing comparison from the repository root. The example creates an EU catalog file in a temporary directory, reads version 1, attempts to load an absent version 2, and verifies that the original items remain available.

```sh
dotnet run -c Release --project examples/HostLoom.Examples.Actors -- --refresh
dotnet test --project tests/HostLoom.Tests/HostLoom.Tests.csproj -c Release -- --filter-class '*CatalogRefreshTests'
dotnet run -c Release --project benchmarks/HostLoom.Benchmarks -- --catalog-refresh
```

[Application worker and client](../../examples/HostLoom.Examples.Actors/RegionalCatalog.cs) · [Runnable example](../../examples/HostLoom.Examples.Actors/CatalogRefreshExample.cs) · [Comparison harness](CatalogRefreshPilot.cs) · [Regression tests](../../tests/HostLoom.Tests/CatalogRefreshTests.cs)
