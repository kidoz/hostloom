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

HostLoomWebSocketBuilder AddTopicSnapshot<TEvent, TProvider>(string topic)
    where TProvider : class, IWebSocketTopicSnapshotProvider<TEvent>;
```

`AddRequest` also registers the typed request client; `AddTopic` also
registers a broker subscription under `subscription`. One event type maps
to one public topic — a second mapping throws.

`AddTopicSnapshot` must follow the matching `AddTopic` call. It registers one scoped snapshot
provider for that topic; a provider already registered for
`IWebSocketTopicSnapshotProvider<TEvent>` is respected. `WebSocketTopicSnapshotContext` supplies
the authorized topic, optional key, and session principal for the duration of enumeration. The
provider must honor cancellation and must not retain the principal after enumeration.

Snapshot delivery order is `subscribed`, zero or more snapshot `event` frames with `sequence = 0`,
then live events with positive process-local sequences. Snapshot values consume credit, and the
client may send `credit` or `unsubscribe` while the asynchronous provider is running. Live events
arriving during initialization reserve capacity in the same connection-wide byte/frame budget and
are released afterward. Keyed subscriptions receive only values whose event key matches; keyless
subscriptions receive all provider values. A provider failure removes only that subscription and
returns `snapshot_failed`; cancellation is silent.

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
`request_failed`, `snapshot_failed`, `canceled`, `duplicate_stream`, `capacity_exceeded`.
`snapshot_failed` reports a sanitized topic-snapshot provider failure.

Remote fault *messages* are withheld from clients unless
`IncludeRemoteFaultMessages` is enabled; the code `request_failed` is
returned either way.

## Composition probe

`HostLoomWebSocketBuilder.Probe()` describes the gateway during registration, and the singleton
`WebSocketGatewayProbe.Describe()` provides the same immutable shape after the container is built.
Both are execution-free: they do not resolve application services, start the host, or contact the
configured transport.

The result lists gateway and origin settings, preferred protocols, public request routes, and
public topics with their source, subscription, policy, keyed-selection, and snapshot metadata.
Configured allowlisted origins are represented only by their count. Its `Decisions` collection has
stable `WebSockets:Gateway`, `WebSockets:Origins`, and `WebSockets:Topic:<name>` component names that
applications may explicitly copy into an optional HostLoom composition ledger. Protect any HTTP
endpoint exposing the result because destinations and contract type names describe application
topology.

## Tracing

Enable the `HostLoom.AspNetCore.WebSockets` activity source through
`WebSocketDiagnostics.ActivitySourceName`. Each registered request creates a
`hostloom.websocket.request` Server activity tagged with registered operation, negotiated protocol,
bounded outcome, and public fault code when applicable. Unregistered client operation names do not
create activities or tags.

The existing `HostLoom` request activity is a direct child through ambient context. This provides
the complete gateway-to-handler chain for the in-memory transport and gateway-to-broker-send
correlation for external transports. Cross-process consumer correlation still requires future W3C
trace-context propagation in broker headers.

## Metrics

Enable the `HostLoom.AspNetCore.WebSockets` meter (also exposed as
`WebSocketDiagnostics.MeterName`) to collect these `System.Diagnostics.Metrics` instruments:

| Instrument | Type | Tags |
| --- | --- | --- |
| `hostloom.websocket.sessions` | up-down counter | `hostloom.websocket.protocol` |
| `hostloom.websocket.subscriptions` | up-down counter | `hostloom.websocket.topic` |
| `hostloom.websocket.events.sent` | counter | `hostloom.websocket.topic` |
| `hostloom.websocket.events.dropped` | counter | `hostloom.websocket.topic`, `hostloom.websocket.reason` |
| `hostloom.websocket.queue.bytes` | byte histogram | `hostloom.websocket.topic` |
| `hostloom.websocket.session.duration` | seconds histogram | `hostloom.websocket.close_reason` |
| `hostloom.websocket.faults` | counter | `hostloom.websocket.fault.code` |
| `hostloom.websocket.handshake.rejected` | counter | `hostloom.websocket.reason` |

Queue bytes are encoded event-frame sizes accepted into the bounded outbound budget, not a queue
occupancy gauge. Sent events are counted only after the socket write succeeds; faults are counted
when generated. Reason values are a bounded library-controlled vocabulary. Metric tags never carry
session ids, subjects, subscription keys, payloads, credentials, or application-supplied close
text. ASP.NET Core authorization middleware may reject a request before the gateway handler; use
the framework's authorization metrics for that path.

## Structured logs

`WebSocketEvents` publishes stable event ids `4100`–`4106` for session open and close,
subscription denial, slow-client abort, handshake rejection, operation failure, and snapshot
failure. Session close logs use the same normalized reason vocabulary as the duration metric.

Lifecycle entries carry a session id, protocol, optional configured subject, and bounded reason;
subscription entries carry only a registered topic and never echo an unknown client topic. The
gateway does not add subscription keys, payloads, credentials, handshake headers, caller-supplied
close text, or remote fault messages to structured properties. The complete event-id, level, and
property table is in the package README.

## Limitations

- Subscriptions are live and process-local: acknowledgements record
  progress but provide no replay, and gateway event ids are not broker
  offsets.
- Multi-node services must choose distinct broker subscription names per
  node — or add a backplane per the broker's queue/consumer-group
  semantics ([transport semantics](../explanation/transports.md)).
- The full wire protocol and operating notes live in
  `src/HostLoom.AspNetCore.WebSockets/README.md` in the repository.
