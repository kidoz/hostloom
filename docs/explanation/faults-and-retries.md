# Faults, retries, and delivery

What happens when a handler throws? This page traces a failure from the
handler to the caller, and draws the boundary HostLoom deliberately keeps
between in-process retry and broker redelivery.

## The life of a fault

1. A handler throws inside its delivery scope.
2. **Receive-pipeline filters see the raw exception.** Retry and circuit
   breaking run here, *before* any encoding — this ordering is what makes
   them apply to handler failures at all.
3. If the pipeline gives up (retries exhausted, breaker open), the
   exception is encoded into the wire envelope as a fault: the error type
   name and message, **no stack trace**. Implementation internals stay on
   the server; the caller receives what it can act on.
4. On the caller's side the fault surfaces as `RemoteRequestException`,
   with `ErrorType` naming the remote exception type. A reply that never
   arrives ends as `RequestTimeoutException` when the request timeout
   elapses; an undecodable envelope raises `MalformedEnvelopeException`.

The wire contract is unchanged by resilience configuration: an exhausted
retry looks to the caller exactly like an immediate failure — one fault
envelope. Resilience changes *whether* and *when* a fault is produced,
never its shape.

## Each attempt gets a fresh scope

A retry re-invokes the rest of the receive pipeline, and each attempt runs
in its own dependency-injection scope. The failed attempt's scoped state —
a poisoned `DbContext`, a half-mutated unit of work — is disposed with the
attempt that failed. Retrying into leaked state is one of the classic
distributed-systems bugs; HostLoom removes it structurally rather than by
convention. (The `HLM0003` analyzer guards the other door: a handler
registered as a singleton would smuggle state across attempts anyway.)

## In-process retry is not redelivery

The receive pipeline never moves a broker offset or acknowledgement.

- **In-process retry** answers: "this attempt failed with something
  transient — try again *here*, now, in a fresh scope."
- **Redelivery** answers: "this *process* failed to handle the message —
  what happens to it next?" That is the transport's concern, governed by
  the broker's own machinery.

Keeping these separate keeps both honest. A retry policy sized for broker
outages would hold deliveries hostage in one process; broker redelivery
used for transient blips would churn acknowledgements and, on Kafka,
consumer offsets. Size the in-process policy for transient failures, and
let the broker do what it already does well with the rest.

## Why the breaker is process-wide

The receive pipeline is composed once, so a circuit breaker's state spans
every delivery — requests and events alike. That is deliberate: the
breaker answers one question, "should this process be taking work at
all?" — and requests and events flow through the same handler-execution
path, so a process failing one is likely to fail the other. A per-message
or per-contract breaker would answer a narrower question; if you need
per-destination isolation, that is a topology decision (separate
processes), not a breaker setting.

## Where the guarantees stop

HostLoom's current slice does not yet include dead-letter behaviors,
outbox/inbox, or delivery policies beyond the receive pipeline — they are
on the roadmap. Until then, plan poison-message handling around the
broker's own redelivery and dead-letter configuration rather than
assuming framework support that does not yet exist — this page states the
boundary so that plan can be made deliberately.
