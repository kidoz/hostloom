# Messaging runtime

The `HostLoom` package: contracts, registration, options, exceptions, and
the serialization boundary. Namespace: `HostLoom`.

```text
dotnet add package HostLoom
```

A transport package is also required — see the
[transports reference](transports.md).

## Contracts

| Contract | Member |
| --- | --- |
| `IRequest<out TResponse>` | marker; one expected response type |
| `IRequestHandler<in TRequest, TResponse>` | `ValueTask<TResponse> HandleAsync(TRequest request, CancellationToken cancellationToken)` |
| `IRequestBehavior<in TRequest, TResponse>` | `ValueTask<TResponse> HandleAsync(TRequest request, RequestHandlerDelegate<TResponse> next, CancellationToken cancellationToken)` |
| `IRequestClient<in TRequest, TResponse>` | `ValueTask<TResponse> GetResponseAsync(RequestAddress destination, TRequest request, TimeSpan? timeout = null, CancellationToken cancellationToken = default)` |
| `IEvent` | marker |
| `IEventHandler<in TEvent>` | `ValueTask HandleAsync(TEvent @event, CancellationToken cancellationToken)` |
| `IPublishEndpoint` | `ValueTask PublishAsync<TEvent>(RequestAddress topic, TEvent @event, CancellationToken cancellationToken = default) where TEvent : class, IEvent` |

`RequestHandlerDelegate<TResponse>` is
`delegate ValueTask<TResponse> (CancellationToken cancellationToken)`.

`RequestAddress` is a `readonly record struct` over a `string Value`, with
an implicit conversion from `string` and `RequestAddress.FromString`.

## Registration

`AddHostLoom(this IServiceCollection, Action<HostLoomOptions>? configure = null)`
returns a `HostLoomBuilder`:

| Method | Effect and lifetimes |
| --- | --- |
| `AddHandler<TRequest, TResponse, THandler>(RequestAddress endpoint)` | handler **scoped**; makes `IRequestClient<TRequest, TResponse>` resolvable (**transient**) |
| `AddSubscriber<TEvent, THandler>(RequestAddress topic, string subscription = "default")` | handler **scoped** |
| `AddBehavior<TRequest, TResponse, TBehavior>()` | behavior **scoped**; additive — multiple behaviors run for one request |
| `AddRequestClient<TRequest, TResponse>()` | client **transient**; for applications with no local handler |
| `ConfigureReceivePipeline(Action<PipeBuilder<ReceiveContext>> configure)` | appends receive filters; callable repeatedly |
| `AddHealthChecks(string livenessName = "hostloom-live", string readinessName = "hostloom-ready")` | two checks tagged `live` / `ready` |
| `UseTransport<TBroker>()` | broker **singleton**; throws `InvalidOperationException` if a transport is already registered |
| `Services` | the underlying `IServiceCollection` |

Handlers, subscribers, and behaviors are scoped on purpose: every delivery
attempt runs in its own dependency-injection scope. Registering them as
singletons defeats that isolation — the `HLM0003`
[analyzer rule](analyzer-rules.md) reports it.

## Options

`HostLoomOptions`:

| Option | Default | Meaning |
| --- | --- | --- |
| `RequestTimeout` | 30 seconds | How long a client waits for a reply before failing; overridable per call via `GetResponseAsync`'s `timeout` parameter |
| `IncludeFaultDetails` | `false` | Forward every handler exception's type name and message in the fault envelope. Off, a caller sees `HandlerFault` / "The request handler failed." for everything except a `RemoteFaultException`; the full exception is logged on the handling side either way |

## Exceptions

| Type | Raised when | Members |
| --- | --- | --- |
| `RemoteRequestException` | the remote handler failed; carries the fault from the wire | `string ErrorType` — `HandlerFault`, `HandlerNotFound`, `ResponseTypeMismatch`, or the remote exception's type name when details were forwarded |
| `RemoteFaultException` (thrown by handlers, not raised by HostLoom) | a handler wants the caller to read its message; its type and message cross the wire verbatim, unlike any other exception | standard constructors; subclass it for typed faults |
| `RequestTimeoutException` (`: TimeoutException`) | no reply within the timeout | `RequestAddress Address`, `TimeSpan Timeout` |
| `MessagingTransportException` | the transport could not carry a request or event: the broker refused or lost the connection, rejected the publication, or its client library failed; whether the broker accepted the message is unknown, so a retry can deliver twice | `RequestAddress Address`; the client library's exception as `InnerException` |
| `MalformedEnvelopeException` | an envelope cannot be decoded, has no message id, or has an unusable correlation id | message only |
| `NotSupportedException` | publishing through a transport without `IEventBroker` | — |

## Serialization boundary

`IMessageSerializer` serializes message bodies (the envelope itself is
encoded separately — see the [wire envelope](wire-envelope.md)):

```csharp
byte[] Serialize(object? value, Type type);
object? Deserialize(ReadOnlySpan<byte> payload, Type type);
// plus generic default-interface overloads
```

The default is `SystemTextJsonMessageSerializer`, registered with
`TryAddSingleton` — register your own `IMessageSerializer` before
`AddHostLoom` to replace it.

## Receive context

Filters registered with `ConfigureReceivePipeline` observe:

| Type | Properties |
| --- | --- |
| `ReceiveContext` (abstract, `: PipeContext`) | `Destination`, `MessageId`, `MessageType`, `Message` |
| `RequestReceiveContext` | — |
| `EventReceiveContext` | adds `string Subscription` |

## Outbox

`UseOutbox<TStore>()` routes every `IPublishEndpoint.PublishAsync` through an
`IOutboxStore` instead of the transport, and starts a relay with the host that
publishes what the store holds. The endpoint encodes the same envelope a direct
publish would send, appends it from the caller's dependency-injection scope, and
wakes the relay. Publishing completes when the append does.

| `IOutboxStore` member | Contract |
| --- | --- |
| `AppendAsync(message, ct)` | store as pending, inside the caller's unit of work when the store has one |
| `ClaimAsync(batchSize, lease, ct)` | the oldest pending messages not under an unexpired lease, leased and in enqueue order; atomic against concurrent relays |
| `MarkPublishedAsync(id, ct)` | never claimed again |
| `MarkFailedAsync(id, error, nextAttemptAt, ct)` | record the error (the exception's type name, never its message), increment `Attempts`, release the lease, and hold the message back until `nextAttemptAt` |
| `MarkDeadLetteredAsync(id, error, ct)` | move the message to a dead-letter state that no claim returns; keep it for an operator to requeue |

`OutboxMessage` carries `MessageId`, `Topic`, `MessageType`, the encoded
`Frame`, `EnqueuedAt`, `Attempts`, and `NextAttemptAt` (`null` when due at
once). `ClaimAsync` must skip a message whose `NextAttemptAt` has not passed.
Register the store scoped (the default)
when it writes through the same unit of work as the handlers, which is what
makes the outbox transactional; the relay resolves it from a scope of its own
per call. `UseInMemoryOutbox()` supplies a per-process store that joins no
transaction, for tests and single-process deployments.

The relay (`OutboxRelay`, also constructible with `new` for a manual relay)
drains when woken and every `Outbox:PollInterval` regardless, so a message
committed after the wake or appended by another process is still relayed.
Within a drain it claims `Outbox:BatchSize` messages under an
`Outbox:ClaimLease`, publishes each frame unchanged through the transport's
`IEventBroker`, and marks it. A publish that fails leaves the message pending
with the failure's type name and a `NextAttemptAt` computed from
`Outbox:RetryDelay` grown by `Outbox:RetryBackoffFactor` per attempt and
clamped to `Outbox:MaxRetryDelay` (the same arithmetic as
`RetryPolicy.Exponential`); the rest of the batch still goes out, and the drain
stops after that batch. A message that fails `Outbox:MaxAttempts` times is
dead-lettered: logged at error, counted, and never claimed again until an
operator requeues it in the store (`InMemoryOutboxStore.Requeue` does this for
the in-memory store). Delivery is at-least-once: a relay that dies between
publishing and marking lets the next claim publish again after the lease.

| Key | Default |
| --- | --- |
| `Outbox:PollInterval` | 5 seconds |
| `Outbox:BatchSize` | 100 |
| `Outbox:ClaimLease` | 1 minute |
| `Outbox:MaxAttempts` | 10 |
| `Outbox:RetryDelay` | 5 seconds |
| `Outbox:MaxRetryDelay` | 5 minutes |
| `Outbox:RetryBackoffFactor` | 2 |

A transport without publish/subscribe fails the host at startup, as it does for
a subscription. Metrics: `hostloom.outbox.published`, `hostloom.outbox.failed`,
`hostloom.outbox.dead_lettered`, and `hostloom.outbox.lag`; log events in
`OutboxEvents` (3300 to 3304).

## Inbox

`UseInbox<TStore>(window)`, `UseInbox(provider => store, window)`, and
`UseInMemoryInbox(window)` append the `InboxFilter` to the receive pipeline at
that point in registration order. Before an event's handlers run, the filter
records `{topic.Length}:{topic}:{subscription.Length}:{subscription}:{messageId}`
(`InboxFilter.KeyFor`) with the `IInboxStore` for the window; a key already
present marks the delivery with an `InboxDuplicate` payload and skips the
handlers. The names are length-prefixed because either may contain `:`, and a
bare join would let two subscriptions share a key. The message id is the
sender's; the codec rejects an empty one before the filter sees it. Requests pass through untouched, because a
request that is not answered leaves its caller waiting for a timeout.

`IInboxStore` has one member, `TryRecordAsync(key, window, ct)`, the atomic
set-if-absent every idempotency store reduces to. `InboxStore.FromClaim`
wraps a delegate, so a cache's set-if-absent with the key as its own value is
a store:

```csharp
.UseInbox(
    provider => InboxStore.FromClaim((key, window, token) =>
        provider.GetRequiredService<ICache>().SetIfAbsentAsync(
            key, key, new CacheEntryOptions(window) { OnUnavailable = UnavailableBehavior.Throw }, token)),
    TimeSpan.FromDays(1))
```

A store that throws lets the handlers run with an `InboxSkipped` payload on the
context and a warning in the log: processing twice is recoverable, dropping a
delivery on an outage is not. The key is recorded before processing, so a run
that fails after recording is not repeated by a later redelivery inside the
window; register `UseInbox` before a `ConfigureReceivePipeline` that adds
`UseRetry` when a failed run should be retried in process. The filter reports
itself in the receive-pipeline probe as `inbox` with its window and store.
Metric: `hostloom.inbox.duplicates`; log events in `InboxEvents` (3310, 3311).

## Cancellation and concurrency

- Every handler, behavior, and client method takes a `CancellationToken`;
  the API is `ValueTask`-based end to end. The `HLM0001`/`HLM0002`
  analyzers flag dropped tokens and sync-over-async.
- One dependency-injection scope per delivery attempt; the receive
  pipeline is composed once, so its stateful filters (breakers, rate
  limits) span deliveries.
- Handlers under the same subscription name share one delivery and one
  scope; distinct subscription names receive independent deliveries.

## Example

```csharp
builder.Services
    .AddHostLoom(options => options.RequestTimeout = TimeSpan.FromSeconds(10))
    .UseInMemory()
    .AddHandler<GetGreeting, Greeting, GetGreetingHandler>("greetings")
    .AddBehavior<GetGreeting, Greeting, LoggingBehavior>();
```
