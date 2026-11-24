# HostLoom local actors

Use an actor when several callers need to update the same process-local state across asynchronous calls. One actor executes one message at a time, including across awaits. The actor kernel and its generic host adapter are experimental, non-packable .NET 10 projects.

```csharp
await using var system = new ActorSystem(
    maxActors: 8, mailboxCapacity: 64, shutdownGracePeriod: TimeSpan.FromSeconds(2));
var owner = system.Spawn<int, int>(() => new CatalogCounter());
var count = await owner.Ref.AskAsync(3, TimeSpan.FromSeconds(1));
await owner.StopAsync();

sealed class CatalogCounter : IActor<int, int>
{
    private int _count;
    public ValueTask<int> ReceiveAsync(int observed, CancellationToken stopping)
    {
        _count += observed;
        return ValueTask.FromResult(_count);
    }
}
```

## Sending and receiving

| Operation or result | Meaning |
|---|---|
| `Spawn` or `SpawnAsync` | Reserve an actor position, then call the supplied factory. Supply a fresh, exclusively owned instance. Construction and cleanup count toward the limit. |
| `TryTell` returns `Accepted` | One immutable message entered this actor's FIFO. Each actor may hold the configured capacity plus one executing turn. |
| `Full` or `Stopped` | Admission failed. A tell returns the reason and an ask throws `ActorAdmissionException` with that reason. |
| `AskAsync` succeeds | The handler completed and returned this reply. Both request and response are passed by reference for reference types. |
| Ask timeout or caller cancellation | The caller stopped waiting. A previously admitted command keeps its place and may change state. Reconcile an uncertain mutation before retrying. |
| Unexpected handler failure | The executing ask gets `ActorFaultException`. The actor stops. Queued asks get `ActorTerminatedException`, and queued tells are discarded. Owner `Completion` reports the failure after cleanup. |
| `handle.StopAsync(token)` | Close admission and drain admitted turns. The token bounds this caller's wait. Existing references stay closed after replacement. |
| `system.StopAsync(token)` | Close the population and drain. At grace expiry, discard queued messages and signal active handlers. Resources remain owned until handlers exit and disposal completes. `DisposeAsync` joins that cleanup. |

FIFO follows successful admission order. Handlers must await work that touches private state. A handler waiting on a call chain back to itself prevents the awaited turn from running. Pass immutable data and grant callers only the references they may use. State lives in memory and disappears on process exit.

## Generic host

Register `AddLocalActorSystem` before services that start sending during host startup. Call `AddLocalActor<TMessage,TReply,TActor>()` for each distinct protocol. Consumers can inject `IActorRef<TMessage,TReply>` during construction and send after startup. After startup the owner can resolve its `ActorHandle<TMessage,TReply>` to observe terminal faults or initiate a drain. Each registration owns a DI scope until its actor terminates. Applications that need several actors with identical protocols can spawn them directly and keep the resulting references.

The runnable catalog example submits an observation, queries the resulting state through HostLoom messaging and checks the reply. From the repository root run `dotnet run -c Release --project examples/HostLoom.Examples.Actors`. [Example](../../examples/HostLoom.Examples.Actors/Program.cs)

For a worker that holds state across I/O, run `dotnet run -c Release --project examples/HostLoom.Examples.Actors -- --refresh`. This example loads a versioned catalog file, shares overlapping refresh attempts and preserves the last published snapshot when a newer file is unavailable. [Regional catalog worker](../../examples/HostLoom.Examples.Actors/RegionalCatalog.cs) · [Semaphore comparison and timeout checks](../../benchmarks/HostLoom.Benchmarks/CatalogRefreshPilot.md)

The catalog client returns `Overloaded` when a new read or refresh finds a full mailbox and `Unavailable` when admission has closed. Both replies carry an empty snapshot reference and the refused request never enters the handler. A refresh for a version already in flight shares the admitted attempt. A source read failure returns `SourceUnavailable` with the last published snapshot. Caller timeouts still leave the outcome of admitted work uncertain. [Catalog client and overload tests](../../tests/HostLoom.Tests/CatalogRefreshTests.cs)

After a refresh times out, call `catalog.CheckVersionAsync(minimumVersion, callerTimeout)` with a fresh wait budget. It reads the snapshot in mailbox order without loading the source. `Published` means the snapshot meets or exceeds the requested version, so the caller can use it. `BelowMinimum` includes the older snapshot observed and the caller may retry the same version if it still needs it. A later refresh for that version reuses an already-published snapshot or joins a matching in-flight load. If the check itself times out or returns `Overloaded` or `Unavailable`, the version remains unknown to that caller. A version check establishes which data is available at the time of the read, regardless of which caller published it.

## Recovering a regional catalog

The example gives each region an owner that watches its worker’s completion. On a handler fault, the owner reads the authoritative version and its items, then opens ingress on a fresh actor in the same system. Old references stay closed. Requests to that region receive `Unavailable` during reconstruction. If the source read fails, the owner reports the error and keeps ingress closed until the application runs the owner again. A graceful stop ends the owner loop. [Regional catalog owner](../../examples/HostLoom.Examples.Actors/RegionalCatalogOwner.cs) · [Fault and recovery tests](../../tests/HostLoom.Tests/CatalogRecoveryTests.cs)

Run the process-restart check from the repository root:

```sh
dotnet build -c Release examples/HostLoom.Examples.Actors
uv run --locked python -B scripts/test-catalog-recovery.py
```

The fixture writes each immutable version file before atomically replacing `eu.current`. It waits for the first process to serve version 2, leaves an unpublished version 3 on disk, and kills that process. The fresh process must read version 2 from the manifest and return both items. The fixture uses a temporary directory on the same filesystem throughout the restart. [Process check](../../scripts/test-catalog-recovery.py)

## Observability and sizing

`ActorSystem.Snapshot` reads per-system counters without entering any mailbox. Population includes construction and disposal. Under concurrent activity each field is sampled independently. The `HostLoom.Actors` meter exposes population, queued and executing work, refused sends, discarded items, faults, caller timeouts, queue delay and turn duration. Histogram units are seconds. The instrument names and dimensions carry no message data or actor identifiers.

Limits bound the number of owned messages. Choose payload and state limits for the application. For a workload comparison against a private channel consumer, run `dotnet run -c Release --project benchmarks/HostLoom.Benchmarks -- --actor-catalog`. [Comparison harness](../../benchmarks/HostLoom.Benchmarks/ActorCatalogComparison.cs)
