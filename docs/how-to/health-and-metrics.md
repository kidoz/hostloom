# Expose health checks and metrics

Wire HostLoom into Kubernetes-style probes and an OpenTelemetry metrics
pipeline.

## Before you begin

- A HostLoom application with a transport registered.
- ASP.NET Core (or another host that can serve HTTP) for the probe
  endpoints.
- For metrics export, the `OpenTelemetry.Extensions.Hosting` package plus
  an exporter such as `OpenTelemetry.Exporter.OpenTelemetryProtocol`:

```text
dotnet add package OpenTelemetry.Extensions.Hosting
```

## 1. Register the health checks

`AddHealthChecks()` registers two checks, tagged `live` and `ready`, so
they map to separate probe endpoints:

```csharp
builder.Services.AddHostLoom().UseRabbitMq().AddHealthChecks();

app.MapHealthChecks("/health/live", new() { Predicate = r => r.Tags.Contains("live") });
app.MapHealthChecks("/health/ready", new() { Predicate = r => r.Tags.Contains("ready") });
```

The check names default to `hostloom-live` and `hostloom-ready`; both are
parameters of `AddHealthChecks` if you need different ones.

!!! danger "Liveness never contacts the broker"
    Liveness answers "should this process be restarted". If a broker
    outage answered *yes*, one outage would become a restart storm across
    every pod that talks to it. Broker reachability belongs in
    **readiness**, which reports unhealthy when endpoints are not
    listening or the transport says the broker is unreachable.

## 2. Subscribe to the metrics

Metrics are published on a `Meter` named `HostLoom`, tagged by
destination and message type — `hostloom.request.duration`, `.active`,
`.faults`, and `.retries` (full list in the
[observability reference](../reference/observability.md)):

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics.AddMeter("HostLoom", "HostLoom.Pipelines"))
    .WithTracing(tracing => tracing.AddSource("HostLoom", "HostLoom.Pipelines"));
```

## 3. Expose the pipeline structure

`HostLoomProbe` returns the receive pipeline's structure without
executing it, which suits a debug endpoint. The topology reveals internal
structure, so don't expose it anonymously — require a policy:

```csharp
app.MapGet("/diagnostics/pipeline", (HostLoomProbe probe) => probe.ReceivePipeline())
    .RequireAuthorization("diagnostics");
```

Alternatively, map it only in development
(`if (app.Environment.IsDevelopment()) { ... }`).

## 4. Verify

```text
curl -i http://localhost:5000/health/live    # 200 Healthy
curl -i http://localhost:5000/health/ready   # 200 while endpoints listen
```

Send a few requests and confirm `hostloom.request.duration` appears in
your metrics backend, tagged with the destination you used.

## Troubleshoot

- **Readiness stays healthy during a RabbitMQ/Kafka outage** — expected
  for now: a transport reports reachability by implementing
  `IBrokerHealthProbe`, a transport that does not is treated as reachable
  ("cannot tell" must not read as "broken"), and today only the in-memory
  transport implements it. Readiness for the broker transports reports on
  listening endpoints only and cannot detect a post-start outage; pair it
  with broker-side monitoring.
- **No HostLoom metrics in the backend** — the meter names are
  case-sensitive (`HostLoom`, `HostLoom.Pipelines`), and an exporter must
  be configured; `AddMeter` alone only subscribes.
- **Probe endpoints return 404** — `MapHealthChecks` was not called, or
  the predicate filters out both tags.

## Related

- Every meter, instrument, and check name:
  [observability reference](../reference/observability.md).
- Why liveness and readiness are split this way:
  [transport semantics](../explanation/transports.md).
- Per-filter pipeline metrics: [Register pipelines with stages](register-pipelines.md).
