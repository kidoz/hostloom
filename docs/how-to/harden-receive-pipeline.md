# Harden the receive pipeline

Wrap every inbound delivery — requests and events alike, on every
transport — with retry, circuit breaking, or your own filters, using
`ConfigureReceivePipeline`.

## Before you begin

A HostLoom application with at least one handler or subscriber registered
(any transport).

## 1. Add resilience filters

```csharp
builder.Services
    .AddHostLoom()
    .UseRabbitMq()
    .ConfigureReceivePipeline(pipe =>
    {
        pipe.UseRetry(
            RetryPolicy.Exponential(3, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(5)));
        pipe.UseCircuitBreaker(failureThreshold: 5, resetInterval: TimeSpan.FromSeconds(30));
    })
    .AddHandler<GetGreeting, Greeting, GetGreetingHandler>("greetings");
```

Size the retry policy for **transient in-process failures** — a
deadlocked row, a momentary connection blip — not for broker outages.
This pipeline never moves a broker offset or acknowledgement;
what happens to a message after this process gives up is the transport's
redelivery, configured on the broker
([why the boundary exists](../explanation/faults-and-retries.md)).

## 2. Add your own filter if needed

A receive filter sees a `ReceiveContext`, which is a
`RequestReceiveContext` or an `EventReceiveContext`. Both carry
`Destination`, `MessageId`, `MessageType`, and `Message`; the event form
adds `Subscription`. Inline delegates suit logic with no dependencies;
implement `IFilter<ReceiveContext>` when the filter needs
constructor-injected services:

```csharp
pipe.Use(async (context, next) =>
{
    var stopwatch = Stopwatch.StartNew();
    await next.SendAsync(context);
    Console.WriteLine($"{context.MessageType} at {context.Destination}: {stopwatch.Elapsed}");
}, "receive-timing");
```

## 3. Verify

Make a handler throw and watch the retry: with the policy above, a
request that keeps failing is attempted four times — the initial attempt
plus three retries — and then reaches the caller as a single
`RemoteRequestException`. The `hostloom.request.retries` metric counts
retries, not attempts, so it increments by three. The composed structure is
visible without executing anything — behind an authorization policy,
since it reveals internal topology:

```csharp
app.MapGet("/diagnostics/pipeline", (HostLoomProbe probe) => probe.ReceivePipeline())
    .RequireAuthorization("diagnostics");
```

## Troubleshoot

- **Retries seem to share state** — they don't: each attempt runs in its
  own dependency-injection scope. If a handler *observes* leftover state,
  it is caching outside the scope (a singleton, a static).
- **The breaker rejects events after request failures** — by design: one
  pipeline serves both, one verdict on whether this process should take
  work. See [faults, retries, and delivery](../explanation/faults-and-retries.md).
- **A breaker or rate limiter appears to reset per message** — it
  doesn't: the pipeline is composed once and shares filter state across
  deliveries. Per-message behavior means the filter was registered
  somewhere re-composed per run.

## Related

- Filter semantics in depth: [the pipeline model](../explanation/pipeline-model.md).
- The full fault path from handler to caller:
  [faults, retries, and delivery](../explanation/faults-and-retries.md).
- Retry metrics: [observability reference](../reference/observability.md).
