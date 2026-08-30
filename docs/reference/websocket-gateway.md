# WebSocket gateway

The `HostLoom.AspNetCore.WebSockets` package: an authenticated raw
WebSocket RPC and live-subscription gateway exposing an explicit subset
of request operations and event topics. Namespace:
`HostLoom.AspNetCore.WebSockets`.

```text
dotnet add package HostLoom.AspNetCore.WebSockets
```

## Registration

```csharp
HostLoomWebSocketBuilder AddWebSocketGateway(this HostLoomBuilder hostLoom,
    Action<HostLoomWebSocketOptions>? configure = null);
```

Calling it a second time *with* a configure callback throws. The builder:

```csharp
HostLoomWebSocketBuilder AddRequest<TRequest, TResponse>(
    string operation, RequestAddress destination, string? authorizationPolicy = null);

HostLoomWebSocketBuilder AddTopic<TEvent>(
    string topic, RequestAddress source,
    string subscription = "hostloom-websocket", string? authorizationPolicy = null);

HostLoomWebSocketBuilder AddTopic<TEvent>(
    string topic, RequestAddress source, Func<TEvent, string?> keySelector,
    string subscription = "hostloom-websocket", string? authorizationPolicy = null);
```

`AddRequest` also registers the typed request client; `AddTopic` also
registers a broker subscription under `subscription`. One event type maps
to one public topic — a second mapping throws.

## Middleware and endpoint

```csharp
app.UseHostLoomWebSockets();            // UseWebSockets, keep-alive 20 s / timeout 10 s
app.MapHostLoomWebSocketHub("/hostloom");  // default pattern "/hostloom"
```

Authentication happens before upgrade; when `RequireAuthenticatedUser` is
on, the endpoint requires authorization and unauthenticated requests get
401. Non-upgrade requests get 400, as does a client offering no
acceptable subprotocol. Named policies are evaluated again per operation
(`WebSocketOperationResource`) and per topic (`WebSocketTopicResource`).

## Options (`HostLoomWebSocketOptions`)

| Option | Default |
| --- | --- |
| `MaximumMessageSize` | 64 KiB |
| `ReceiveBufferSize` | 4 KiB |
| `MaximumQueuedBytesPerConnection` | 256 KiB |
| `MaximumQueuedFramesPerConnection` | 512 |
| `MaximumConcurrentRequestsPerConnection` | 8 |
| `MaximumSubscriptionsPerConnection` | 32 |
| `MaximumCreditPerSubscription` | 1024 |
| `DefaultRequestTimeout` | 10 s |
| `MaximumRequestTimeout` | 30 s |
| `RequireAuthenticatedUser` | `true` |
| `IncludeRemoteFaultMessages` | `false` |
| `ProtocolPreference` | msgpack, protobuf, json |

All limits are validated at registration; the byte and frame bounds plus
per-subscription credit are what keep a slow client from creating
unbounded per-connection memory or work.

## Subprotocols

| Constant | Value | Frame type |
| --- | --- | --- |
| `MessagePackWebSocketHubProtocol.ProtocolName` | `hostloom.msgpack.v1` | binary |
| `ProtobufWebSocketHubProtocol.ProtocolName` | `hostloom.protobuf.v1` | binary |
| `JsonWebSocketHubProtocol.ProtocolName` | `hostloom.json.v1` | text |

Custom protocols implement `IWebSocketHubProtocol`
(`SubProtocol`, `MessageType`, `Decode`, `Encode`).

## Frames and fault codes

`HubFrame` carries `Kind` (`Welcome`, `Request`, `Response`, `Fault`,
`Cancel`, `Subscribe`, `Subscribed`, `Event`, `Credit`, `Ack`,
`Unsubscribe`, `Complete`), a `StreamId`, and kind-dependent fields
(`Operation`, `Topic`, `Key`, `TimeoutMilliseconds`, `Credit`,
`Sequence`, `EventId`, `Code`, `Message`, `Payload`, …).

`HubFaultCodes` string constants: `invalid_frame`, `invalid_payload`,
`operation_not_found`, `topic_not_found`, `forbidden`, `request_timeout`,
`request_failed`, `canceled`, `duplicate_stream`, `capacity_exceeded`.

Remote fault *messages* are withheld from clients unless
`IncludeRemoteFaultMessages` is enabled; the code `request_failed` is
returned either way.

## Limitations

- Subscriptions are live and process-local: acknowledgements record
  progress but provide no replay, and gateway event ids are not broker
  offsets.
- Multi-node services must choose distinct broker subscription names per
  node — or add a backplane per the broker's queue/consumer-group
  semantics ([transport semantics](../explanation/transports.md)).
- The full wire protocol and operating notes live in
  `src/HostLoom.AspNetCore.WebSockets/README.md` in the repository.
