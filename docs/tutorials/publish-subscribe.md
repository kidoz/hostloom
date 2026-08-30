# Publish and subscribe

In this tutorial you will publish a typed event to a topic and watch two
independent subscriptions each receive it. You will also see what makes a
*subscription* different from a handler address — the distinction every
transport preserves.

This tutorial continues the `GreetingService` project from
[Your first request/response](getting-started.md); start there if you have
not set it up. You will add three files and rewrite `Program.cs`:

```text
GreetingService/
├── Program.cs            (rewritten below)
├── OrderPlaced.cs        (new)
├── AuditHandler.cs       (new)
└── ShippingHandler.cs    (new)
```

## Requests and events are different contracts

A request addresses **one** handler and expects **one** reply. An event is
published to a **topic** and delivered to **every subscription** on it,
and no reply exists. HostLoom keeps the two apart in the type system.

Create `OrderPlaced.cs`:

```csharp
using HostLoom;

public sealed record OrderPlaced(string Reference) : IEvent;
```

## 1. Write two subscribers

Create `AuditHandler.cs`:

```csharp
using HostLoom;

public sealed class AuditHandler : IEventHandler<OrderPlaced>
{
    public ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken)
    {
        Console.WriteLine($"audit: {@event.Reference}");
        return ValueTask.CompletedTask;
    }
}
```

Create `ShippingHandler.cs`:

```csharp
using HostLoom;

public sealed class ShippingHandler : IEventHandler<OrderPlaced>
{
    public ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken)
    {
        Console.WriteLine($"shipping: {@event.Reference}");
        return ValueTask.CompletedTask;
    }
}
```

## 2. Register and publish

Replace the entire contents of `Program.cs` with:

```csharp
using HostLoom;
using HostLoom.Transport.InMemory;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddHostLoom()
    .UseInMemory()
    .AddSubscriber<OrderPlaced, AuditHandler>("orders", subscription: "audit")
    .AddSubscriber<OrderPlaced, ShippingHandler>("orders", subscription: "shipping");

var host = builder.Build();
await host.StartAsync();

var publisher = host.Services.GetRequiredService<IPublishEndpoint>();
await publisher.PublishAsync("orders", new OrderPlaced("A-1"));

await host.StopAsync();
```

## 3. Verify

```text
dotnet run
```

Both lines print — each subscription received the event:

```text
audit: A-1
shipping: A-1
```

The two lines may appear in either order: delivery order *across*
subscriptions is unspecified, on every transport. Subscriptions are
independent consumers, and nothing orders one relative to another.

## The subscription rules

The behavior you just observed follows three rules that hold on every
transport:

- **Two subscription names on one topic each receive every event.** That
  is what a subscription is for: an independent consumer with its own
  backlog.
- **Two handlers under the *same* subscription name share one delivery**
  and one dependency-injection scope. Use this when several handlers form
  one logical consumer.
- **A subscription with no handler for a published contract ignores it**
  rather than failing. Topics can carry more than one event type.

Publish/subscribe is a separate transport capability, `IEventBroker`.
Publishing through a transport that lacks it throws, and registering a
subscription against one fails at startup — HostLoom refuses to start up
*looking* subscribed while nothing would be delivered.

## What a subscription becomes on a real broker

You wrote against logical names; each transport maps them onto its own
fan-out primitive:

| Transport | Topic | Subscription |
| --- | --- | --- |
| In-memory | named in-process channel | a named handler on the topic |
| RabbitMQ | fanout exchange | durable queue named `topic.subscription` bound to it |
| Kafka | Kafka topic | its own consumer group |

On RabbitMQ, subscriptions accumulate their own backlog rather than
competing for one queue. On Kafka, every group receives every record while
instances sharing a group divide the partitions. The
[transport semantics](../explanation/transports.md) page explains why
these mappings are deliberately not identical.

## Where next

- Wrap event delivery with retry and circuit breaking in
  [Harden the receive pipeline](../how-to/harden-receive-pipeline.md).
- Run this over a real broker: [RabbitMQ](../how-to/use-rabbitmq.md) or
  [Kafka](../how-to/use-kafka.md).
