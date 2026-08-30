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

`HostLoomOptions` has a single property:

| Option | Default | Meaning |
| --- | --- | --- |
| `RequestTimeout` | 30 seconds | How long a client waits for a reply before failing; overridable per call via `GetResponseAsync`'s `timeout` parameter |

## Exceptions

| Type | Raised when | Members |
| --- | --- | --- |
| `RemoteRequestException` | the remote handler failed; carries the fault from the wire | `string ErrorType` — the remote exception's type name |
| `RequestTimeoutException` (`: TimeoutException`) | no reply within the timeout | `RequestAddress Address`, `TimeSpan Timeout` |
| `MalformedEnvelopeException` | an envelope cannot be decoded | message only |
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
