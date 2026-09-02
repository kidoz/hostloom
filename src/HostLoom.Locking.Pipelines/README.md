# HostLoom.Locking.Pipelines

A distributed-lock filter for `HostLoom.Pipelines`, built on the `HostLoom.Locking` kernel. The
package references those two and nothing else; `HostLoom.Pipelines` stays dependency-free.

```csharp
var pipeline = Pipe.Create<RefreshContext>(pipe =>
{
    pipe.UseDistributedLock(
        distributedLock,
        context => $"refresh:{context.Region}",
        new LockOptions { Lease = TimeSpan.FromSeconds(30), OnLost = LostLeaseBehavior.Cancel });
    pipe.UseExecute(context => RefreshAsync(context));
});
```

`UseDistributedLock` runs the rest of the pipe inside `IDistributedLock.ExecuteWithLockAsync`,
so two contexts with the same key never run downstream at once, across every instance sharing
the lock's provider. Different keys run concurrently. `LockNotAcquiredException` and
`LockProviderUnavailableException` propagate unchanged; a retry filter ahead of this one is the
place to decide whether to try again.

Before the rest of the pipe runs, a `HeldLock` payload is put on the context with the key and the
cancellation token the lock handed to the run. The context's own token cannot be replaced, so a
downstream filter that must stop when the lease is lost watches `HeldLock.CancellationToken`,
which the lock cancels when the options ask for `LostLeaseBehavior.Cancel`.

The lock is coordination, not correctness, for persisted state: database transactions, row
locks, unique constraints, and idempotency records own correctness.

## From the container

The filter has a public constructor taking `IDistributedLock` and
`DistributedLockFilterOptions<TContext>`, so `HostLoom.Pipelines.DependencyInjection` resolves it
per run:

```csharp
services.AddSingleton(new DistributedLockFilterOptions<RefreshContext>
{
    KeySelector = context => $"refresh:{context.Region}",
    Lock = new LockOptions { Lease = TimeSpan.FromSeconds(30) },
});
services.AddPipeline<RefreshContext>("refresh", pipeline =>
    pipeline.Stage("run", stage =>
        stage.AddFilter<DistributedLockFilter<RefreshContext>>().AddFilter<RefreshFilter>()));
```

The filter describes itself to `PipelineProbe.Inspect` as a `distributedLock` scope with the
lease, wait bound, retry policy, and lost-lease behaviour.
