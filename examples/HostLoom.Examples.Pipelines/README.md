# HostLoom.Examples.Pipelines

A runnable tour of HostLoom pipelines, modeled on a batch processing service: one run takes a
batch of documents through analyze → summarize → store stages.

```text
dotnet run --project examples/HostLoom.Examples.Pipelines
```

The program demonstrates three ways to build a pipe from several filters:

1. **Registered pipeline (dependency injection)** — `AddPipeline<IndexingContext>("document-indexing", …)`
   declares named stages holding filter types. Filters resolve transient from a per-run scope, so
   `WordCountFilter`, `ReadingTimeFilter`, and `StoreDocumentFilter` take loggers and the
   `IDocumentStore` repository through constructors. Host startup validates the pipeline and logs
   its topology; the run is wrapped in a timeout and a retry policy; `sentence_count` is behind a
   feature toggle evaluated on every run; each filter is automatically instrumented (meter and
   activity source `HostLoom.Pipelines`).
2. **Manual composition from container-resolved filters** — open a scope, resolve each filter
   type with `GetRequiredService`, and hand the instances to `Pipe.Create`. Full control over
   composition, constructor injection intact.
3. **Standalone composition** — no container: delegate filters and directly constructed filter
   instances compose the same way, which is also how filters are unit-tested.

The filters show the patterns that matter in production pipelines: bounded intra-filter fan-out
(`Parallel.ForEachAsync` capped by `IndexingContext.MaxAnalysisParallelism`), per-item error
isolation (one failing document never fails the batch), and missing-input tolerance (an insight
over an absent metric becomes `null`, not an exception).
