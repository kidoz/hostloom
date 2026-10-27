# HostLoom.Examples.Messaging

A broker-free tour of HostLoom messaging. The program starts one in-memory host and checks three
communication styles:

1. **Request/response** — `GetGreeting` is handled at the `greetings` address and returns a typed
   `Greeting` through `IRequestClient<TRequest, TResponse>`.
2. **Event fan-out** — one `OrderPlaced` event reaches the independent `audit` and `shipping`
   subscriptions. Their output order is deliberately not assumed.
3. **In-process streaming** — `ListCatalog` yields catalog categories in order through
   `IStreamRequestClient<TRequest, TItem>`. The caller token flows into the handler's async
   iterator, and the handler scope ends when enumeration completes.

Run it from the repository root:

```text
dotnet run --project examples/HostLoom.Examples.Messaging
```

No RabbitMQ, Kafka, Redis, or Valkey service is required. The program prints each result and exits
non-zero if a reply, event delivery, or streamed category does not match the expected contract.
Streams are specific to the in-process transport; RabbitMQ and Kafka support requests and events,
not `IStreamRequest<T>`.
