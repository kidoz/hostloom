# Stream process-local events to WebSocket clients

Use the in-memory transport when each ASP.NET Core replica produces the live events needed by the
WebSocket clients connected to that same replica. This topology needs no external broker and keeps
the full HostLoom event envelope, receive pipeline, gateway authorization, bounded queues, and
subscription credit.

It is deliberately process-local. It is not sufficient when an event produced in one process must
reach clients connected to other replicas.

## Before you begin

- Install `HostLoom.Transport.InMemory` and `HostLoom.AspNetCore.WebSockets`.
- Configure the application's normal ASP.NET Core authentication scheme.
- Decide which local state changes should be public gateway topics. Register only those topics;
  client input never selects a broker address or CLR type.

```text
dotnet add package HostLoom.Transport.InMemory
dotnet add package HostLoom.AspNetCore.WebSockets
```

This guide uses an inventory update produced by an HTTP endpoint in the same process as the
gateway. Replace the endpoint with a local background producer or application service when that is
where the state changes originate.

## 1. Register the local topology

Register one in-memory transport and expose the event under a separate public topic name:

```csharp
using HostLoom;
using HostLoom.AspNetCore.WebSockets;
using HostLoom.Transport.InMemory;
using Microsoft.AspNetCore.Authentication.Cookies;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
    .AddCookie();
builder.Services.AddAuthorization(options =>
{
    options.AddPolicy("inventory.read", policy =>
        policy.RequireClaim("scope", "inventory.read"));
    options.AddPolicy("inventory.write", policy =>
        policy.RequireClaim("scope", "inventory.write"));
});

builder.Services
    .AddHostLoom()
    .UseInMemory()
    .AddWebSocketGateway(options => options.AllowMissingOrigin = false)
    .AddTopic<InventoryLevelChanged>(
        "inventory.level.changed",
        "inventory",
        changed => changed.ItemId,
        authorizationPolicy: "inventory.read");
```

`inventory` is the internal HostLoom event source. `inventory.level.changed` is the public gateway
topic offered to clients. The key selector lets a client subscribe to one item identifier; omit it
to make every authorized subscriber receive every event on the public topic.

The default gateway subscription name, `hostloom-websocket`, is safe here because every process has
its own in-memory broker. The name does not connect or coordinate replicas.

## 2. Publish after the host has started

Map the gateway and publish through `IPublishEndpoint` from work handled by the running
application:

```csharp
var app = builder.Build();

app.UseAuthentication();
app.UseAuthorization();
app.UseHostLoomWebSockets();

app.MapHostLoomWebSocketHub("/realtime");

app.MapPost(
        "/inventory/{itemId}/availability/{available:int}",
        async (
            string itemId,
            int available,
            IPublishEndpoint publisher,
            CancellationToken cancellationToken) =>
        {
            await publisher.PublishAsync(
                "inventory",
                new InventoryLevelChanged(itemId, available),
                cancellationToken);

            return Results.Accepted();
        })
    .RequireAuthorization("inventory.write");

app.Run();

public sealed record InventoryLevelChanged(string ItemId, int Available) : IEvent;
```

Host startup attaches the gateway's event subscription. Do not publish startup events before the
host is running: an in-memory event with no attached subscription is dropped. Live gateway events
also have no replay, so provide an HTTP state query or an
`IWebSocketTopicSnapshotProvider<TEvent>` when a reconnecting client needs current state.

## 3. Verify from a browser

From an authenticated, same-origin page, open the browser console and connect with the JSON
subprotocol. Wait for `welcome` before subscribing:

```javascript
const scheme = location.protocol === "https:" ? "wss" : "ws";
const socket = new WebSocket(
  `${scheme}://${location.host}/realtime`,
  "hostloom.json.v1",
);

socket.addEventListener("message", ({ data }) => {
  const frame = JSON.parse(data);
  console.log(frame);

  if (frame.kind === "welcome") {
    socket.send(JSON.stringify({
      kind: "subscribe",
      streamId: crypto.randomUUID().replaceAll("-", ""),
      topic: "inventory.level.changed",
      key: "item-42",
      credit: 16,
    }));
  }
});
```

After the `subscribed` frame arrives, publish a matching change from the same page:

```javascript
await fetch("/inventory/item-42/availability/8", {
  method: "POST",
  credentials: "same-origin",
});
```

The socket receives an `event` frame whose `payload` is Base64-encoded application JSON. This
console client is only a connectivity check. A production client must replenish credit, reconnect
with backoff, resubscribe, and refresh current state after reconnecting.

## Understand the replica boundary

Each replica has an independent transport, gateway subscription, and set of sockets:

```text
replica A: local producer -> in-memory transport -> gateway -> sockets connected to A
replica B: local producer -> in-memory transport -> gateway -> sockets connected to B
```

There is no path from replica A's transport to replica B's gateway. This is the intended shape when
every replica independently produces equivalent state changes, or when a client needs information
only about the replica serving its current socket. A reconnect may land on another replica, so the
client must refresh state there.

Session affinity does not change event reachability. It can keep reconnects near one replica, but it
cannot make a locally published event appear in another process.

## Know when to move to a broker

Replace the in-memory transport with RabbitMQ, Kafka, or a dedicated backplane when any of these is
true:

- only one replica produces an event that clients on every replica must receive;
- the producer runs in another service or worker process;
- gateway replicas and producers must scale independently;
- delivery must survive a process restart or support a durable backlog;
- an aggregate view must combine state from several replicas.

With a broker-backed transport, give every gateway replica a distinct RabbitMQ queue or Kafka
consumer group when every replica must receive every event. Sharing one queue or group
load-balances events between replicas instead of fanning them out. HostLoom does not yet generate a
per-instance Kafka subscription automatically, so the application must supply a stable unique
subscription name during composition.

See [Run over RabbitMQ](use-rabbitmq.md), [Run over Kafka](use-kafka.md), and
[transport semantics](../explanation/transports.md) before changing the topology.

## Operational checks

- Keep the gateway's authentication and topic policies enabled in every environment.
- Confirm the event producer resolves `IPublishEndpoint` from the same application service provider
  as the gateway.
- Treat readiness as process-local: it proves the local subscription attached, not that another
  replica can receive the event.
- Monitor gateway delivery and dropped-event metrics; they describe this process only.
- Configure reverse-proxy upgrades and idle timeouts as described in the
  [WebSocket gateway reference](../reference/websocket-gateway.md#reverse-proxy-and-load-balancing).
