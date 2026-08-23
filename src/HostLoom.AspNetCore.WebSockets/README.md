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

The endpoint requires an authenticated ASP.NET Core principal by default. Authentication is
completed before the upgrade; each request operation and subscription can additionally name an
ASP.NET Core policy. Policy handlers receive `WebSocketOperationResource` or
`WebSocketTopicResource`. Browser applications should exchange their normal credential for a
short-lived, single-use WebSocket ticket in application code instead of putting a long-lived
bearer token in a query string.

Only registered operations and topics are reachable. Client-supplied CLR type names, broker
addresses, and arbitrary handler names are never resolved.

## Protocol negotiation

The client must offer at least one versioned `Sec-WebSocket-Protocol` value:

- `hostloom.msgpack.v1` — binary MessagePack envelope, preferred by default;
- `hostloom.protobuf.v1` — binary Protocol Buffers envelope implemented with protobuf-net;
- `hostloom.json.v1` — UTF-8 JSON envelope.

All codecs carry the same `HubFrame`. `Payload` contains bytes produced by HostLoom's configured
`IMessageSerializer`; JSON therefore represents it as Base64 while MessagePack and Protocol Buffers
represent it as binary. This keeps the WebSocket framing protocol independent from application
contract serialization. The cross-language Protocol Buffers schema is shipped as
`protocol/hostloom-websocket-v1.proto` in the NuGet package.

The server rejects an upgrade when no supported subprotocol was offered. Changing a frame's
WebSocket message type after negotiation closes the connection.

## Version-one frames

Every client stream uses a non-zero `streamId`. A request stream lives until `response` or `fault`;
a subscription stream lives until `unsubscribe` and `complete`.

| Direction | `kind` | Required fields | Meaning |
|---|---|---|---|
| server → client | `welcome` | `sessionId` | Advertises message, concurrency, and credit limits. |
| client → server | `request` | `streamId`, `operation`, `payload` | Starts one registered HostLoom request. |
| server → client | `response` | `streamId`, `payload` | Successful typed response. |
| server → client | `fault` | `streamId`, `code`, `message` | Stable machine code plus sanitized detail. |
| client → server | `cancel` | `streamId` | Cancels an active request. |
| client → server | `subscribe` | `streamId`, `topic`, `credit` | Starts a topic subscription; `key` is optional. |
| server → client | `subscribed` | `streamId`, `topic`, `credit` | Confirms the subscription. |
| server → client | `event` | `streamId`, `eventId`, `sequence`, `payload` | Delivers one live event. |
| client → server | `credit` | `streamId`, `credit` | Adds bounded delivery credit. |
| client → server | `ack` | `streamId`, `sequence` | Records progress in session state; it does not enable replay. |
| client → server | `unsubscribe` | `streamId` | Stops a subscription. |
| server → client | `complete` | `streamId` | Confirms termination. |

For JSON, enum names use the serializer's web defaults (for example `"request"`). Unknown or
malformed connection-level frames close the connection. Route errors are returned as `fault`
frames with codes from `HubFaultCodes`.

## Load and delivery semantics

Each connection has exactly one receive loop and one socket writer. Request work may run
concurrently up to `MaximumConcurrentRequestsPerConnection`; duplicate active stream IDs are
rejected. Outbound traffic uses a bounded channel with a separate byte budget. A connection whose
writer cannot keep up is aborted instead of growing memory without limit.

Events consume one unit of subscription credit before entering the output queue. No event is sent
when credit is zero. Version one is deliberately a **live, process-local subscription protocol**:
event IDs and sequences are generated by the gateway process, acknowledgements are not persisted,
and reconnecting does not replay missed events.

HostLoom broker subscriptions still keep their broker-specific meaning. In a multi-node deployment,
use a distinct HostLoom subscription name per gateway node if every node must see every event, or
put a dedicated fan-out/backplane service in front of the nodes. Reusing one RabbitMQ queue or Kafka
consumer group across gateway nodes intentionally load-balances events, which means a client on a
different node will not see every event. The package does not claim cross-node presence, replay, or
exactly-once delivery.

Important limits are `MaximumMessageSize`, `MaximumQueuedBytesPerConnection`,
`MaximumQueuedFramesPerConnection`, `MaximumConcurrentRequestsPerConnection`,
`MaximumSubscriptionsPerConnection`, `MaximumCreditPerSubscription`, and
`MaximumRequestTimeout`. Defaults are conservative and should be load-tested with the actual event
size distribution and client population.

Codec throughput and allocations can be measured with the repository's BenchmarkDotNet project:

```text
dotnet run --project benchmarks/HostLoom.Benchmarks -c Release -- --filter "*WebSocketProtocol*"
```
