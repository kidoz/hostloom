# Actor pilot for the process-local message inbox

**Decision for this component. Keep the current in-memory inbox.** Both implementations returned correct decisions under the selected peak. The actor missed the latency criterion and introduced a cancellation outcome that can suppress a delivery whose handler never ran. The actor candidate lives in the benchmark project.

## Decision criteria recorded before measurement

The current `InMemoryInboxStore` owns a dictionary behind one lock. The candidate gives one actor exclusive ownership of the same kind of dictionary. Both implement `IInboxStore`. The receive pipeline records a claim before entering the event handler, suppresses a duplicate, and releases a failed delivery using its claim token. A store error lets the handler run with an `InboxSkipped` marker. [Store](../../src/HostLoom/Inbox/InMemoryInboxStore.cs) · [Receive filter](../../src/HostLoom/Inbox/InboxFilter.cs)

Use the current store as the baseline. Consider adoption only if the actor preserves claim ownership and gives the component a simpler lifetime or concurrency model. The following numerical limits are assumptions for a local experiment. No production traffic trace or service latency objective was supplied.

| Check | Pilot criterion |
|---|---|
| Load | Eight producers each offer 16 claims every 10 ms for 100 waves. Three out of four attempts revisit one message. Other attempts use distinct message IDs. Reuse the same keys and tokens for both implementations. |
| Correctness | One successful claim per distinct key, zero incorrect duplicates, and a matching final claim owner for every key. A stale release must preserve a replacement at the expiry boundary. |
| Response time and rejection | Median of five trial p99 store response times at most 1 ms. Zero mailbox refusals, timeouts or faults at the selected peak. A duplicate is a successful store response. Count admission failures separately. |
| Cancellation | Cancellation before admission leaves the key free. A cancelled wait during a claim must not leave a new record that suppresses an unhandled delivery without reporting ownership to its caller. |
| Shutdown and overload | With one active turn and queued work, stop closes new admission, settles all accepted calls and finishes within 2 seconds after unblocking. An intentionally full mailbox must report a store failure. Observe what the real receive filter does on that failure. |

Use one 20-wave warmup per implementation and alternate execution order across measured pairs. Use an actor mailbox capacity of 256 and a 5-second ask deadline. Measure elapsed time from the public store call to its reply and report launch lateness separately. Time the claim path only. Check expiry, release, cancellation and in-flight shutdown separately from the timed workload. A semantic failure is enough to rule out adoption at this boundary.

## Measured outcome

Measured on 2026-11-22 on an Apple M4 Max, macOS 27.2.1 arm64, 16 logical CPUs, .NET SDK 10.0.404, runtime 10.0.15, workstation GC, Release build. The pilot was added against HostLoom revision `26b2ac2`. Each trial used fresh state and the same deterministic sequence of message IDs and claim tokens. The deduplication window was one hour. Expiry tests advanced a separate controlled clock.

| Criterion | Current store | Actor store |
|---|---:|---:|
| Measured calls across five trials | 64,000 | 64,000 |
| Claims / duplicates per trial | 3,201 / 9,599 | 3,201 / 9,599 |
| Unavailable, timed out or faulted at selected peak | 0 | 0 |
| Median completed calls/s | 12,779 | 12,772 |
| Median trial p50 store response | 0.20 µs | 112.40 µs |
| Median trial p99 store response | 8.50 µs | 1,523.60 µs |
| Lowest to highest trial p99 | 7.80–11.50 µs | 640.30–2,929.80 µs |
| Trials meeting the 1 ms p99 screen | 5 of 5 | 2 of 5 |
| Median trial p99 delay in launching a scheduled call | 1.15 ms | 5.06 ms |

The local inbox starts empty on replacement and rebuilds its claim table from subsequent deliveries. Prior claims cannot be reconstructed after process loss from this store alone. Fresh instances accepted the same first deliveries in each trial.

Both stores completed every offered call. The runner probed every distinct key after each trial and checked that the winning token could release it. Throughput was bounded by the offered schedule. The p99 store response measures individual claims from invocation through reply, including actor mailbox waiting. Producer scheduling delay is reported separately. These are local store measurements on a synthetic burst trace. They exclude broker transit, serialization and handler time.

## Failure and ownership checks

- **Expiry and release passed for both stores.** At the expiry boundary, a new token acquired the key. A late release from the previous owner preserved the replacement, and releasing the current owner let another delivery through.
- **Pre-cancellation passed for both stores.** Each rejected a pre-cancelled claim and left the key available.
- **Cancellation during a claim changed what the caller knew.** The run paused each store inside its claim operation, cancelled the caller, then resumed the write. The current store returned success, allowing the receive filter to proceed with a known owner. The actor caller observed cancellation, yet the retry found the key occupied. At the filter boundary, cancellation during recording exits before handler execution and before the handler-failure release path. A redelivery can therefore skip an unhandled event until the record expires. The controlled run observed the unreported claim and refused retry at the store boundary. The consequence for handler execution follows from the receive filter.
- **Forced overload reached the real receive filter.** Using the in-memory transport, the runner published an event, captured its exact frame, and replayed it twice. The first replay was suppressed. For the second replay it held one actor turn open and filled a one-position mailbox. The store reported `Full`, the filter attached `InboxSkipped`, and the handler ran a second time. The smaller mailbox makes this failure deterministic and is separate from the timed capacity of 256.
- **Shutdown drained the actor.** With one active claim and one queued claim, shutdown rejected a new send and both admitted calls completed successfully. Joining the drain after unblocking took 0.16 ms against the 2-second pilot bound. The current store has synchronous local operations and no background consumer or shutdown API.

The current implementation serializes its dictionary update in one critical section. Giving this table an actor removes that lock but introduces a mailbox, a reply deadline, caller-wait cancellation, and an owner that must be drained. For this component the additional lifetime and failure handling outweigh the ownership benefit. Increasing mailbox capacity would leave the cancellation issue open.

## Reproduction

From the repository root, run the following on an otherwise idle machine. Output includes the directed observations and each measured trial. A failed invariant, timeout or unexpected failure terminates the run.

```sh
dotnet build benchmarks/HostLoom.Benchmarks -c Release
dotnet run --project benchmarks/HostLoom.Benchmarks -c Release --no-build -- --actor-inbox
```

[Replay runner](ActorInboxPilot.cs) · [Candidate store](ActorInboxStore.cs) · [Failure and shutdown checks](ActorInboxChecks.cs)
