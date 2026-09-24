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
   failure is encoded into the wire envelope as a fault. By default the
   fault is anonymous: type `HandlerFault`, message "The request handler
   failed." The real exception type and message are logged on the
   handling side and never cross the transport, because an exception
   message can carry a connection string, a file path, or a fragment of
   somebody else's data. Two things change that: throwing a
   `RemoteFaultException` (or a subclass), which is the handler saying
   "this message is written for the caller" and travels with its type and
   message intact; or setting `HostLoomOptions.IncludeFaultDetails`, which
   forwards every exception in full and belongs only in deployments where
   every caller is trusted. In no case does a stack trace cross the wire.
4. On the caller's side the fault surfaces as `RemoteRequestException`,
   with `ErrorType` carrying the fault type: `HandlerFault`,
   `HandlerNotFound` (no handler for that message type on the endpoint),
   `ResponseTypeMismatch`, `HandlerNotRun` (a receive filter completed the
   request without running its handler), or the exception type name when details were
   allowed. A reply that never arrives ends as `RequestTimeoutException`
   when the request timeout elapses; an undecodable envelope raises
   `MalformedEnvelopeException`.

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

Registered pipelines follow the same rule. `WithRetry` re-runs the whole
pipeline, and every attempt opens its own scope, resolves fresh filter
instances, and evaluates `EnabledWhen` again; the failed attempt's scope
is disposed before the next attempt starts. The one thing every attempt
shares is the context object, including any payloads filters added to it.

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

HostLoom does not provide a general delivery or dead-letter policy for
inbound messages beyond the receive pipeline. What each transport does
with a delivery this process could not handle is stated, not unified:

- **RabbitMQ** rejects a failed or malformed delivery without requeue. Set
  `RabbitMqOptions.DeadLetterExchange` to have the request and
  subscription queues declared with `x-dead-letter-exchange`, so those
  rejections are routed rather than dropped. A delivery that was
  cancelled — the listener is stopping, or the client library cancelled
  it — is nacked with requeue, because the process, not the message, is
  the reason it was not handled.
- **Kafka** rewinds a record whose handling failed and consumes it again
  after a short backoff. An event is retried until it is handled or the
  subscription stops, and holds up the rest of its partition meanwhile. A
  request is retried until its fifth failed attempt, then logged and
  committed past. A malformed record, or a request whose reply could not be
  produced after the handler ran, is committed past at once without
  re-running the handler. Nothing is dead-lettered.

Plan poison-message handling around those statements and the broker's own
configuration; this page states the boundary so that plan can be made
deliberately.

What the framework does include is the pair that makes at-least-once
delivery safe to build on: the transactional outbox, which stores an event
with the business change and relays it afterwards, and the inbox, which
skips a redelivered event once its handlers have completed for that
subscription inside a window, and releases the key of a run that failed so
the redelivery runs again.
See [Outbox](../reference/messaging.md#outbox) and
[Inbox](../reference/messaging.md#inbox). The outbox relay retries a
message the transport refuses with an exponential backoff and, after
`Outbox:MaxAttempts`, moves it to a dead-letter state in the store that no
claim returns; an operator requeues it from there. Neither changes what a
broker guarantees: the outbox relay itself delivers at least once, and the
inbox is what absorbs the duplicate.
