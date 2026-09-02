# HostLoom.Caching.Pipelines

Cache and deduplication filters for `HostLoom.Pipelines`, built on the `HostLoom.Caching`
kernel. The package references those two and nothing else; `HostLoom.Pipelines` stays
dependency-free.

```csharp
var pipeline = Pipe.Create<CatalogContext>(pipe =>
{
    pipe.UseDeduplication(cache, context => context.MessageId, TimeSpan.FromMinutes(10));
    pipe.UseCache<CatalogContext, Catalog>(
        cache,
        context => $"catalog:{context.Region}",
        new CacheEntryOptions(TimeSpan.FromMinutes(5)) { Tags = ["catalog"] });
    pipe.UseExecute(async context =>
    {
        var catalog = await LoadCatalogAsync(context.Region, context.CancellationToken);
        context.GetOrAddPayload(() => catalog);
    });
});
```

## Cache

`UseCache<TContext, TPayload>` is get-or-create around the rest of the pipe. On a hit the cached
`TPayload` is put on the context and the rest of the pipe does not run; on a miss the rest of
the pipe runs, and whatever `TPayload` the context holds afterwards is written to the cache with
the entry options. A `CacheFilterResult` payload records the key, whether it was a hit, the tier
that answered, and whether the distributed tier was unavailable. The cache is fail-open, so a
store failure never surfaces from this filter.

The lookup and the write are two calls rather than one get-or-create, because the "factory" is
the downstream pipe and has to run with the context. Two concurrent misses for one key therefore
both run downstream; put `UseDistributedLock` from `HostLoom.Locking.Pipelines` ahead of the
cache filter when one computation per key matters.

## Deduplication

`UseDeduplication` runs the rest of the pipe at most once per identity inside a window. It claims
the identity with an atomic set-if-absent before running: a claim that succeeds runs the pipe, a
claim that finds the identity present adds a `Deduplicated` payload and stops. The claim happens
before processing, so a run that fails after claiming is not repeated inside the window; put a
retry filter after this one when a failed run should be retried.

When the store cannot answer, the pipe runs anyway and a `DeduplicationSkipped` payload says why.
Processing a message twice is recoverable; dropping it on an outage is not, so an unavailable
store always leads to processing. The marker written is the identity string itself, which every
serializer can write; a source-generated `JsonSerializerContext` used as the cache serializer
must be able to serialize `string`.

This filter is offered for generic pipelines. HostLoom does not wire it into the messaging
receive pipeline: the messaging kernel defers idempotent consumer storage, and a database inbox
remains the answer where a platform's concurrency rules require one.

## From the container

Each filter has a public constructor taking `ICache` and a small options object, so
`HostLoom.Pipelines.DependencyInjection` resolves it per run:

```csharp
services.AddSingleton(new CacheFilterOptions<CatalogContext, Catalog>
{
    KeySelector = context => $"catalog:{context.Region}",
    Entry = new CacheEntryOptions(TimeSpan.FromMinutes(5)),
});
services.AddPipeline<CatalogContext>("catalog", pipeline =>
    pipeline.Stage("load", stage =>
        stage.AddFilter<CacheFilter<CatalogContext, Catalog>>().AddFilter<LoadCatalogFilter>()));
```

`DeduplicationFilterOptions<TContext>` plays the same role for `DeduplicationFilter<TContext>`.
Both filters describe themselves to `PipelineProbe.Inspect` as `cache` and `deduplication`
scopes with their settings.
