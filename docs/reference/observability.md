# Observability surface

Every name an OpenTelemetry (or plain `System.Diagnostics.Metrics`)
configuration needs.

## Sources and meters

| Name | Kind | Published by |
| --- | --- | --- |
| `HostLoom` | `ActivitySource` and `Meter` | messaging runtime |
| `HostLoom.Pipelines` | `ActivitySource` and `Meter` | registered pipelines |
| `HostLoom.Logging` | `Meter` | logging provider health |

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics.AddMeter("HostLoom", "HostLoom.Pipelines", "HostLoom.Logging"))
    .WithTracing(tracing => tracing.AddSource("HostLoom", "HostLoom.Pipelines"));
```

Requires the `OpenTelemetry.Extensions.Hosting` package.

## Messaging instruments (`HostLoom`)

Tagged by destination and message type.

| Instrument | Type | Meaning |
| --- | --- | --- |
| `hostloom.request.duration` | histogram (s) | Request handling duration |
| `hostloom.request.active` | up-down counter | In-flight requests |
| `hostloom.request.faults` | counter | Failed requests |
| `hostloom.request.retries` | counter | Receive-pipeline retry attempts |

## Pipeline instruments (`HostLoom.Pipelines`)

| Instrument | Type | Meaning |
| --- | --- | --- |
| `hostloom.pipeline.filter.duration` | histogram | A filter's own work, downstream time subtracted |
| `hostloom.pipeline.filter.failures` | counter | Filter failures |
| `hostloom.pipeline.run.duration` | histogram | Whole pipeline run duration |
| `hostloom.pipeline.run.active` | up-down counter | In-flight pipeline runs |

The per-filter duration subtracts downstream time on purpose: the slow
filter is visible wherever it sits in the chain, instead of every upstream
filter inheriting its latency. `WithoutInstrumentation()` opts a registered
pipeline out.

## Logging instruments (`HostLoom.Logging`)

Health of the logging provider itself — the bounded queue and its
background writer.

| Instrument | Type | Meaning |
| --- | --- | --- |
| `hostloom.logging.records.dropped` | counter | Log records dropped instead of written |
| `hostloom.logging.fields.dropped` | counter | Structured fields dropped from otherwise-shipped records |
| `hostloom.logging.enqueue.blocked` | counter | Log calls that blocked because the queue was full |
| `hostloom.logging.enqueue.blocked.duration` | histogram (s) | Time log calls spent blocked on a full queue |
| `hostloom.logging.failures` | counter | Unexpected component failures inside the logging pipeline |
| `hostloom.logging.queue.depth` | observable gauge | Records waiting in the bounded queue |
| `hostloom.logging.writer.state` | observable gauge | 1 while the background writer is healthy, 0 once faulted or disposed |

## Health checks

`AddHealthChecks()` on the HostLoom builder registers:

| Check | Default name | Tag | Contacts broker? |
| --- | --- | --- | --- |
| Liveness | `hostloom-live` | `live` | Never |
| Readiness | `hostloom-ready` | `ready` | Via `IBrokerHealthProbe`, when the transport implements it |

A transport that does not implement `IBrokerHealthProbe` is treated as
reachable — "cannot tell" must not read as "broken".

## Execution-free probes

- `HostLoomProbe.ReceivePipeline()` — the receive pipeline's structure as a
  `ProbeResult` tree, without executing it.
- `PipelineProbe.Inspect(pipe)` — the same for any standalone pipe.
- `IPipelineRunner<TContext>.Topology` — a registered pipeline's resolved
  topology; `Describe()` renders it, marking conditional filters with a
  trailing `?`.
