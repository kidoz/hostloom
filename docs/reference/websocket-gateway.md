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
The topic resource includes the exact client-selected `Key`.

`TopicKeyPolicy.SubjectOnly` is the built-in own-channel policy. Pass it as a topic's
`authorizationPolicy` to require an authenticated principal and an ordinal match between a
nonempty key and the first claim named by `SubjectClaimType`. A mismatch returns `forbidden` before
the subscription is registered. Applications that also require a scope or role define a normal
composite ASP.NET Core policy and inspect `WebSocketTopicResource.Key` in that policy.

A supplied Origin is validated before the upgrade. `SameOrigin` is the default;
`AllowList` accepts configured exact origins and `Disabled` opts out. Native clients
may omit Origin by default; set `AllowMissingOrigin` to `false` for browser-only endpoints.
The gateway uses the effective ASP.NET Core request scheme and host, so trusted forwarded
headers must be configured earlier in the middleware pipeline.

## Options (`HostLoomWebSocketOptions`)

| Option | Default |
| --- | --- |
| `OriginMode` | `SameOrigin` |
| `AllowMissingOrigin` | `true` |
| `AllowedOrigins` | empty |
| `MaximumMessageSize` | 64 KiB |
| `ReceiveBufferSize` | 4 KiB |
| `MaximumQueuedBytesPerConnection` | 256 KiB |
| `MaximumQueuedFramesPerConnection` | 512 |
| `MaximumConcurrentRequestsPerConnection` | 8 |
| `MaximumSubscriptionsPerConnection` | 32 |
| `MaximumCreditPerSubscription` | 1024 |
| `MaximumControlFramesPerSecond` | 50 |
| `MaximumSessionLifetime` | 12 h |
| `SubjectClaimType` | `ClaimTypes.NameIdentifier` |
| `DefaultRequestTimeout` | 10 s |
| `MaximumRequestTimeout` | 30 s |
| `RequireAuthenticatedUser` | `true` |
| `IncludeRemoteFaultMessages` | `false` |
| `ProtocolPreference` | msgpack, protobuf, json |

All limits are validated at registration; the byte and frame bounds plus
per-subscription credit are what keep a slow client from creating
unbounded per-connection memory or work.

## Session lifetime and control

`IWebSocketSessionLifetimeResolver.ResolveExpirationAsync` resolves credential expiry during the
upgrade. The default prefers `AuthenticationProperties.ExpiresUtc`, then the earliest valid `exp`
claim. The session is capped by `MaximumSessionLifetime` in either case and closes with 1008
`session_expired` when the injected `TimeProvider` reaches that boundary. Register a replacement
resolver before `AddWebSocketGateway` when the authentication system stores expiry elsewhere.

`IWebSocketSessionDirectory` provides `Count`, `GetSessions()`, and
`GetSessionsBySubject(subject)`. Each `WebSocketSessionInfo` snapshot contains session id, subject,
negotiated protocol, connection and expiry times, and the current subscription count; no
`ClaimsPrincipal` is exposed.

`IWebSocketSessionControl.DisconnectAsync(sessionId, reason)` and
`DisconnectSubjectAsync(subject, reason)` close matched sessions with 1008 and wait for their
lifecycle to finish. Use them when logout or a role change must revoke an already upgraded socket.
The reason must fit the WebSocket 123-byte UTF-8 close-description limit. During host shutdown the
gateway closes all sessions with 1001 `server_shutdown` before HostLoom's broker listeners stop.

Client control frames are bounded independently from request concurrency. More than
`MaximumControlFramesPerSecond` `cancel`, `subscribe`, `credit`, `ack`, or `unsubscribe` frames in
one fixed one-second window closes the session with 1008 `rate_limited`.

## Subprotocols

| Constant | Value | Frame type |
| --- | --- | --- |
| `MessagePackWebSocketHubProtocol.ProtocolName` | `hostloom.msgpack.v1` | binary |
| `ProtobufWebSocketHubProtocol.ProtocolName` | `hostloom.protobuf.v1` | binary |
| `JsonWebSocketHubProtocol.ProtocolName` | `hostloom.json.v1` | text |

Custom protocols implement `IWebSocketHubProtocol`
(`SubProtocol`, `MessageType`, `Decode`, `Encode`).

JSON uses camelCase kind values, omits null optional fields, and keeps application payloads as
Base64 bytes. The NuGet package includes its schema and conformance fixtures under `protocol/`.

## Testing

`HostLoom.AspNetCore.WebSockets.Testing.WebSocketTestClient` connects to an ASP.NET Core
`TestServer`, configures handshake headers, drives frames, and awaits common server frame kinds.
It uses JSON v1 by default and accepts any `IWebSocketHubProtocol` in its constructor.

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
