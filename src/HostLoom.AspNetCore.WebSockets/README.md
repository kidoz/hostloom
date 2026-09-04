# HostLoom.AspNetCore.WebSockets

An authenticated raw-WebSocket gateway for HostLoom request/response and live event
subscriptions. It runs on ASP.NET Core/Kestrel and does not use SignalR.

## Registration

```csharp
using HostLoom;
using HostLoom.AspNetCore.WebSockets;
using HostLoom.Transport.RabbitMq;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddAuthentication().AddJwtBearer();
builder.Services.AddAuthorization(options =>
{
    options.AddPolicy("orders.read", policy => policy.RequireClaim("scope", "orders.read"));
});

builder.Services
    .AddHostLoom()
    .UseRabbitMq()
    .AddHandler<GetOrder, OrderView, GetOrderHandler>("orders-api")
    .AddWebSocketGateway(options =>
    {
        options.MaximumConcurrentRequestsPerConnection = 16;
        options.MaximumQueuedBytesPerConnection = 512 * 1024;
        options.OriginMode = WebSocketOriginMode.AllowList;
        options.AllowedOrigins.Add("https://admin.example.com");
        options.AllowMissingOrigin = false;
    })
    .AddRequest<GetOrder, OrderView>("orders.get", "orders-api", "orders.read")
    .AddTopic<OrderChanged>(
        "orders.changed",
        "orders",
        changed => changed.CustomerId,
        subscription: "realtime-node-a",
        authorizationPolicy: "orders.read");

var app = builder.Build();
app.UseAuthentication();
app.UseAuthorization();
app.UseHostLoomWebSockets();
app.MapHostLoomWebSocketHub("/realtime");
app.Run();
```

The parameterless `UseHostLoomWebSockets()` uses a 20-second keep-alive interval and a 10-second
Pong timeout. Pass ASP.NET Core `WebSocketOptions` when the host needs different middleware
settings:

```csharp
app.UseHostLoomWebSockets(new WebSocketOptions
{
    KeepAliveInterval = TimeSpan.FromSeconds(30),
    KeepAliveTimeout = TimeSpan.FromSeconds(15),
});
```

This helper delegates to ASP.NET Core `UseWebSockets`. If the application already called
`UseWebSockets`, omit `UseHostLoomWebSockets`; registering both would add the middleware twice, and
the gateway does not attempt unreliable pipeline-presence detection. ASP.NET Core
`WebSocketOptions.AllowedOrigins` and the gateway's `HostLoomWebSocketOptions.OriginMode` are
independent checks, so configure them consistently when both are enabled.

### Reverse proxies and load balancing

For HTTP/1.1 WebSocket connections, the reverse proxy must support the upgrade and preserve the
`Upgrade: websocket`, `Connection: Upgrade`, and `Sec-WebSocket-Protocol` request headers. It must
also preserve the application's selected authentication material and `Origin` when those values
participate in authentication or origin validation.

When TLS terminates at a proxy, have that proxy set the forwarded scheme and host and configure
ASP.NET Core forwarded-header middleware to trust and apply those values before authentication and
the gateway endpoint. The gateway evaluates ASP.NET Core's effective `Request.Scheme` and
`Request.Host`; it does not interpret forwarding headers itself.

With the default 20-second keep-alive interval, configure the proxy's idle timeout above 60 seconds.
For a custom interval, leave equivalent operational margin above the chosen interval. The 10-second
Pong timeout detects an unresponsive peer; it is not a substitute for the proxy idle timeout.

Sticky sessions are not required when every gateway replica has a distinct HostLoom broker
subscription name, or when a backplane otherwise delivers every event to every replica. Each open
socket remains on the replica that accepted it, while a reconnect may select another replica. The
version-one protocol is live and has no replay, so a reconnecting client must refresh its current
state. Sharing one RabbitMQ queue or Kafka consumer group load-balances events between replicas;
session affinity does not turn that topology into fan-out.

The endpoint requires an authenticated ASP.NET Core principal by default. Authentication is
completed before the upgrade; each request operation and subscription can additionally name an
ASP.NET Core policy. Policy handlers receive `WebSocketOperationResource` or
`WebSocketTopicResource`. Browser applications should exchange their normal credential for a
short-lived, single-use WebSocket ticket in application code instead of putting a long-lived
bearer token in a query string.

## Key-aware subscription authorization

Topic policy handlers receive `WebSocketTopicResource.Topic` and the exact client-selected
`WebSocketTopicResource.Key`. This lets an application combine key ownership with its ordinary
scope or role requirements. For the common "own channel" case, use the built-in policy:

```csharp
.AddTopic<OrderChanged>(
    "orders.changed",
    "orders",
    changed => changed.CustomerId,
    authorizationPolicy: TopicKeyPolicy.SubjectOnly)
```

`TopicKeyPolicy.SubjectOnly` requires an authenticated principal, a nonempty subscription key, and
an ordinal match between that key and the first `SubjectClaimType` claim (by default
`ClaimTypes.NameIdentifier`). Missing subjects, missing keys, and differently cased values are
denied with a `forbidden` fault before the subscription enters session state. Define an ordinary
ASP.NET Core policy over `WebSocketTopicResource` when subject ownership must be combined with
additional requirements.

## Snapshot on subscribe

A topic can load its current application state from a scoped provider before live delivery begins:

```csharp
.AddTopic<OrderChanged>("orders.changed", "orders", changed => changed.CustomerId)
.AddTopicSnapshot<OrderChanged, OrderSnapshotProvider>("orders.changed");

sealed class OrderSnapshotProvider(OrderStore store)
    : IWebSocketTopicSnapshotProvider<OrderChanged>
{
    public IAsyncEnumerable<OrderChanged> GetSnapshotAsync(
        WebSocketTopicSnapshotContext context,
        CancellationToken cancellationToken = default) =>
        store.ReadCurrentAsync(context.Key, cancellationToken);
}
```

`AddTopicSnapshot` follows `AddTopic`, requires the same event type, and allows one provider per
topic. The provider is scoped by default and receives the authorized topic, optional key, and
session principal. It must honor the cancellation token and must not retain the principal after
enumeration. Applications remain the source of truth; the gateway does not add a retained-value
store.

The server queues `subscribed` first, then emits snapshot values as ordinary `event` frames with
`sequence = 0`, then releases live events that arrived while the provider was running. Positive
sequences remain live, process-local events. Snapshot values consume subscription credit; the
receive loop remains active, so the client can add credit or unsubscribe while initialization is
waiting. A keyed subscription receives only provider values whose configured event key selector
matches exactly. A keyless subscription receives every provider value.

Live frames held during initialization reserve bytes and frames from the existing connection-wide
outbound limits. Overflow keeps the existing slow-client behavior and aborts the connection rather
than allocating an unbounded snapshot side buffer. Provider cancellation ends silently;
unsubscribe cancels it. Other provider failures remove that subscription and return a sanitized
`snapshot_failed` fault without closing unrelated streams.

## Origin validation

Browser-supplied Origin headers are checked before the upgrade. `OriginMode` defaults to
`SameOrigin`; `AllowList` accepts exact normalized scheme, host, and effective-port matches, while
`Disabled` is an explicit opt-out. Missing Origin is allowed by default because native WebSocket
clients may omit it; set `AllowMissingOrigin = false` for a browser-only endpoint.

Configure ASP.NET Core forwarded-header middleware before the WebSocket endpoint when a trusted
proxy supplies the effective scheme or host. The gateway uses the resulting `Request.Scheme` and
`Request.Host` and never interprets forwarding headers itself. Register a custom
`IWebSocketOriginValidator` to replace the built-in policy.

## Session lifetime and revocation

Every accepted session has a fixed expiry. The built-in
`IWebSocketSessionLifetimeResolver` uses `AuthenticationProperties.ExpiresUtc` when authorization
middleware exposes the authentication ticket, otherwise it reads the earliest valid JWT `exp`
claim. `MaximumSessionLifetime` (12 hours by default) is always an upper bound, including for
credentials without an expiry. At expiry the server closes with 1008 `session_expired`.

`IWebSocketSessionDirectory` exposes read-only point-in-time session metadata and can filter by the
configured `SubjectClaimType`. It does not expose credentials. Application logout and role-change
handlers can use `IWebSocketSessionControl`:

```csharp
var sessions = services.GetRequiredService<IWebSocketSessionControl>();
await sessions.DisconnectSubjectAsync(userId, "roles_changed", cancellationToken);
```

Administrative disconnects close with 1008 and the supplied close reason. Reasons must be nonempty
and at most 123 UTF-8 bytes. Host shutdown closes every registered session with 1001
`server_shutdown` and waits for session teardown before HostLoom broker subscriptions stop.
Client `cancel`, `subscribe`, `credit`, `ack`, `unsubscribe`, and `ping` frames share a per-session
fixed one-second rate window; exceeding `MaximumControlFramesPerSecond` closes with 1008
`rate_limited`.
Register a custom `IWebSocketSessionLifetimeResolver` before `AddWebSocketGateway` when credential
expiry lives elsewhere.

Only registered operations and topics are reachable. Client-supplied CLR type names, broker
addresses, and arbitrary handler names are never resolved.

## Protocol negotiation

The client must offer at least one versioned `Sec-WebSocket-Protocol` value:

- `hostloom.msgpack.v1` — binary MessagePack envelope, preferred by default;
- `hostloom.protobuf.v1` — binary Protocol Buffers envelope implemented with protobuf-net;
- `hostloom.json.v1` — compact UTF-8 JSON with camelCase frame-kind values and omitted null fields.

All codecs carry the same `HubFrame`. `Payload` contains bytes produced by HostLoom's configured
`IMessageSerializer`; JSON therefore represents it as Base64 while MessagePack and Protocol Buffers
represent it as binary. This keeps the WebSocket framing protocol independent from application
contract serialization. The cross-language Protocol Buffers schema is shipped as
`protocol/hostloom-websocket-v1.proto` in the NuGet package.

JSON accepts kind names case-insensitively and rejects numeric, `None`, and unknown kinds. Its JSON
Schema and exact `welcome`, `subscribed`, `event`, `fault`, `ping`, and `pong` fixtures are shipped
under the package's `protocol/` directory.

`streamId`, `sessionId`, and `eventId` are `Guid` identifiers. JSON spells each as 32 lowercase
hexadecimal digits with no separators, and the binary codecs carry the 16 big-endian bytes of
RFC 4122, so all three subprotocols name the same identifier. The all-zero identifier addresses the
session rather than a stream; it is valid only on `welcome`.

The schema is the interoperability contract, so it never admits a frame one conformant
implementation could not read. `welcome` is pinned to the all-zero stream; every other kind must use
a different identifier; `sequence` stops at 9007199254740991, the largest integer JSON carries
without loss; `credit`, `timeoutMilliseconds`, `maximumMessageSize`, and
`maximumConcurrentRequests` stop at 2147483647. `payload` is constrained by a Base64 pattern,
because Draft 2020-12 treats `contentEncoding` as an annotation that most validators do not
enforce.

The repository's dependency-free ESM
[`@hostloom/websocket-client`](../../clients/hostloom-websocket-client/README.md) package consumes
that schema and those fixtures in its conformance tests. It provides TypeScript frame types,
direction-aware JSON-v1 encoding and decoding, runtime validation, and Base64 JSON payload helpers.
Its injectable connection core validates the selected protocol and welcome frame, observes state
and later server frames, sends validated client frames, and supports explicit close and manual
reconnect, including calls made during close teardown, plus an opt-in jittered exponential retry
policy. Its request API allocates stream identifiers, correlates responses and typed faults,
enforces the advertised concurrency and encoded-message limits, passes gateway timeouts, maps
`AbortSignal` to a cancel frame, and never replays requests. Its subscription API shares stream
allocation with requests, waits for `subscribed`, buffers within initial credit until a listener
exists, replenishes credit at a configurable low watermark, acknowledges current-session progress,
cleans up unowned subscription streams, maps cancellation to `unsubscribe`, and resubscribes
retained logical handles after a replacement welcome. Close code `1008` retries only after the
configured credential-refresh callback succeeds.

The server rejects an upgrade when no supported subprotocol was offered. Changing a frame's
WebSocket message type after negotiation closes the connection.

## Version-one frames

Every client stream picks its own `streamId`, which must not be the all-zero session identifier. A
request stream lives until `response` or `fault`; a subscription stream lives until `unsubscribe`
and `complete`. Identifiers are never reused within a session.

| Direction | `kind` | Required fields | Meaning |
|---|---|---|---|
| server → client | `welcome` | `sessionId` | Advertises message, concurrency, and credit limits. |
| client → server | `request` | `streamId`, `operation`, `payload` | Starts one registered HostLoom request. |
| server → client | `response` | `streamId`, `payload` | Successful typed response. |
| server → client | `fault` | `streamId`, `code`, `message` | Stable machine code plus sanitized detail. |
| client → server | `cancel` | `streamId` | Cancels an active request. |
| client → server | `subscribe` | `streamId`, `topic`, `credit` | Starts a topic subscription; `key` is optional. |
| server → client | `subscribed` | `streamId`, `topic`, `credit` | Confirms the subscription. |
| server → client | `event` | `streamId`, `eventId`, `sequence`, `payload` | Delivers a snapshot (`sequence = 0`) or live event. |
| client → server | `credit` | `streamId`, `credit` | Adds bounded delivery credit. |
| client → server | `ack` | `streamId`, `sequence` | Records progress in session state; it does not enable replay. |
| client → server | `unsubscribe` | `streamId` | Stops a subscription. |
| server → client | `complete` | `streamId` | Confirms termination. |
| client → server | `ping` | `streamId` | Requests an application-level liveness reply. |
| server → client | `pong` | `streamId` | Echoes the ping `streamId`. |

For JSON, enum names are camelCase (for example `"request"`). Malformed frames close the
connection. A decodable frame kind that is not valid from a client returns an `invalid_frame`
fault. Route errors are returned as `fault` frames with codes from `HubFaultCodes`.

### Application-level ping

`ping` and `pong` exist because browsers cannot observe RFC 6455 Ping and Pong control frames.
Those transport-level frames remain the gateway's keep-alive and still detect an unresponsive peer;
a client that can observe them does not need these application frames.

The client picks a `streamId` and the gateway echoes it, so several pings may be in flight
and each round-trip time is unambiguous. The session answers directly, without a dependency-injection
scope, a registered operation, or a transport hop, which makes a `pong` evidence about the socket
and the gateway loops rather than about broker health. The gateway retains no ping state, and a
`ping` never reserves a stream, a request slot, or a subscription. A `ping` addressed to the
all-zero session identifier returns `invalid_frame`, and a client-sent `pong` is rejected the same
way. The reply is queued like
any other outbound frame, so a connection too slow to drain its queue is aborted rather than served.

## Load and delivery semantics

Each connection has exactly one receive loop and one socket writer. Request work may run
concurrently up to `MaximumConcurrentRequestsPerConnection`; duplicate active stream IDs are
rejected. Outbound traffic uses a bounded channel with a separate byte budget. A connection whose
writer cannot keep up is aborted instead of growing memory without limit.

Events consume one unit of subscription credit before entering the output queue. No event is sent
when credit is zero. Version one is deliberately a **live, process-local subscription protocol**:
event IDs and sequences are generated by the gateway process, acknowledgements are not persisted,
and reconnecting does not replay missed events.

A session may hold several streams for the same topic and key. Registry membership is
reference-counted per session and `(topic, key)`, so completing one stream does not stop delivery to
its siblings; membership ends after the last matching stream is removed.

HostLoom broker subscriptions still keep their broker-specific meaning. In a multi-node deployment,
use a distinct HostLoom subscription name per gateway node if every node must see every event, or
put a dedicated fan-out/backplane service in front of the nodes. Reusing one RabbitMQ queue or Kafka
consumer group across gateway nodes intentionally load-balances events, which means a client on a
different node will not see every event. The package does not claim cross-node presence, replay, or
exactly-once delivery.

For a broker-free topology where every replica produces events for its own connected clients, see
[Stream process-local events to WebSocket clients](../../docs/how-to/stream-process-local-events-to-websockets.md).

Important limits are `MaximumMessageSize`, `MaximumQueuedBytesPerConnection`,
`MaximumQueuedFramesPerConnection`, `MaximumConcurrentRequestsPerConnection`,
`MaximumSubscriptionsPerConnection`, `MaximumCreditPerSubscription`,
`MaximumControlFramesPerSecond`, `MaximumSessionLifetime`, and `MaximumRequestTimeout`. Defaults are
conservative and should be load-tested with the actual event size distribution and client
population.

## Composition probe

`HostLoomWebSocketBuilder.Probe()` returns an immutable description while the service collection is
still being composed. The same execution-free snapshot is available later from the registered
`WebSocketGatewayProbe`; neither entry point resolves handlers or snapshot providers, starts the
host, or contacts a transport:

```csharp
var gateway = builder.Services
    .AddHostLoom()
    .UseInMemory()
    .AddWebSocketGateway()
    .AddRequest<GetOrder, OrderView>("orders.get", "orders-api");

var registration = gateway.Probe();

app.MapGet(
        "/diagnostics/websockets",
        (WebSocketGatewayProbe probe) => probe.Describe())
    .RequireAuthorization("operations.read");
```

The description contains authentication and remote-fault settings, origin mode and allowlist
count, protocol preference, request routes, and topic routes including source, subscription,
authorization, keyed selection, and snapshot-provider metadata. Allowlisted origin values are not
returned. Protect a runtime endpoint because route destinations and application type names describe
the service topology.

`Decisions` contains `WebSockets:Gateway`, `WebSockets:Origins`, and one
`WebSockets:Topic:<name>` value per topic. The WebSocket package does not reference
`HostLoom.Diagnostics`; an application that already references that optional leaf can record the
values explicitly during composition:

```csharp
using HostLoom.Diagnostics;

foreach (var decision in gateway.Probe().Decisions)
{
    gateway.Services.RecordComposition(
        decision.Component,
        decision.Choice,
        decision.Reason);
}
```

## Tracing

The `HostLoom.AspNetCore.WebSockets` activity source, exposed as
`WebSocketDiagnostics.ActivitySourceName`, creates one `hostloom.websocket.request` Server activity
for each registered request operation. Enable both gateway and core sources to retain the complete
same-process chain:

```csharp
builder.Services.AddOpenTelemetry().WithTracing(tracing => tracing.AddSource(
    WebSocketDiagnostics.ActivitySourceName,
    HostLoomDiagnostics.ActivitySourceName));
```

The gateway activity becomes the ambient parent of the existing core `hostloom request` activity;
the transport and in-process handler activities continue below it. Tags are
`hostloom.websocket.operation`, `hostloom.websocket.protocol`, `hostloom.websocket.outcome`
(`success`, `fault`, `canceled`, or `exception`), and `hostloom.websocket.fault.code` on a fault.
Only registered operation names become trace identity. Unregistered client input creates no
gateway activity, and activities never contain stream or session ids, subjects, payloads, keys,
credentials, caller text, or remote fault messages.

This is parent/child correlation inside the current process. HostLoom transports do not yet
propagate W3C trace context in broker headers, so a handler running in another process cannot join
this trace until that separate transport feature is implemented.

## Metrics

The gateway publishes `System.Diagnostics.Metrics` instruments from the
`HostLoom.AspNetCore.WebSockets` meter. `WebSocketDiagnostics.MeterName` and its public tag-name
constants can be used when configuring a collector. No OpenTelemetry package dependency is
required.

| Instrument | Type / unit | Tags | Meaning |
| --- | --- | --- | --- |
| `hostloom.websocket.sessions` | up-down counter / `{session}` | `hostloom.websocket.protocol` | Active accepted sessions. |
| `hostloom.websocket.subscriptions` | up-down counter / `{subscription}` | `hostloom.websocket.topic` | Active authorized subscriptions. |
| `hostloom.websocket.events.sent` | counter / `{event}` | `hostloom.websocket.topic` | Event frames successfully written to a socket. |
| `hostloom.websocket.events.dropped` | counter / `{event}` | `hostloom.websocket.topic`, `hostloom.websocket.reason` | Event frames not delivered. |
| `hostloom.websocket.queue.bytes` | histogram / `By` | `hostloom.websocket.topic` | Encoded event-frame sizes accepted into the connection's bounded outbound budget; this is not current queue occupancy. |
| `hostloom.websocket.session.duration` | histogram / `s` | `hostloom.websocket.close_reason` | Completed session lifetimes. |
| `hostloom.websocket.faults` | counter / `{fault}` | `hostloom.websocket.fault.code` | Fault frames generated, whether or not the socket remains writable. |
| `hostloom.websocket.handshake.rejected` | counter / `{rejection}` | `hostloom.websocket.reason` | Upgrade requests rejected inside the gateway handler. |

Drop reasons are bounded to `no_credit`, `message_too_large`, `queue_capacity`,
`queue_unavailable`, and `subscription_stopped`. Handshake reasons are `unauthenticated`,
`not_websocket`, `origin`, and `subprotocol`. Close reasons are normalized to `aborted`,
`session_expired`, `server_shutdown`, `rate_limited`, `message_too_large`,
`invalid_message_type`, `invalid_payload`, `peer_closed`, `completed`, `policy_violation`,
`endpoint_unavailable`, or `other`; an application-supplied administrative close description is
never used as a tag.

Tags contain only negotiated protocol, registered public topic, and library-controlled reason or
fault values. They never contain session ids, subjects, subscription keys, payloads, credentials,
or caller-supplied text. Authorization middleware can reject a request before the gateway handler
runs; observe ASP.NET Core authorization metrics for those rejections rather than expecting a
gateway handshake measurement.

## Structured logging

`WebSocketEvents` exposes stable `EventId` values for every gateway log. The hot lifecycle paths
use cached `LoggerMessage` delegates.

| Id / name | Level | Structured properties |
| --- | --- | --- |
| `4100` / `WebSocketSessionOpened` | Information | `SessionId`, `Protocol`, `Subject` |
| `4101` / `WebSocketSessionClosed` | Information | `SessionId`, `Protocol`, `Subject`, `CloseReason`, `CloseStatus`, `DurationMilliseconds` |
| `4102` / `WebSocketSubscriptionDenied` | Warning | `SessionId`, `Topic`, `Reason` |
| `4103` / `WebSocketSlowClientAborted` | Warning | `SessionId`, `FrameKind`, `Topic`, `MaximumQueuedFrames`, `MaximumQueuedBytes` |
| `4104` / `WebSocketHandshakeRejected` | Warning | `Reason` |
| `4105` / `WebSocketOperationFailed` | Error | `Operation`, exception |
| `4106` / `WebSocketSnapshotFailed` | Error | `Topic`, `SessionId`, exception |

Session close reasons use the same normalized vocabulary as
`hostloom.websocket.session.duration`. Subscription-denial reasons are `topic_not_found`,
`key_too_long`, `invalid_credit`, `capacity`, `forbidden`, and `duplicate_stream`. Only a registered
topic is logged; an unknown client-supplied topic is represented as null. Subscription keys,
payloads, credentials, handshake headers, caller-supplied close text, and remote fault messages are
never added as structured properties. `Subject` is the configured subject claim and should remain
a non-secret identifier. Exceptions on operation and snapshot-provider failures originate from
application code and remain subject to the application's exception-message policy.

Codec throughput and allocations can be measured with the repository's BenchmarkDotNet project:

```text
dotnet run --project benchmarks/HostLoom.Benchmarks -c Release -- --filter "*WebSocketProtocol*"
```

## Integration testing

`HostLoom.AspNetCore.WebSockets.Testing` wraps ASP.NET Core `TestServer` with a protocol-aware
`WebSocketTestClient`. It configures upgrade headers, sends and receives `HubFrame` values, and has
helpers for awaiting `welcome`, `subscribed`, `event`, and `fault` frames without a real browser or
network listener.
