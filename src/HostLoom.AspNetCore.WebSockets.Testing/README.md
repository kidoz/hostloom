# HostLoom.AspNetCore.WebSockets.Testing

In-process test client helpers for `HostLoom.AspNetCore.WebSockets` applications hosted by
ASP.NET Core `TestServer`.

```csharp
await using var client = new WebSocketTestClient(server);
client.ConfigureRequest = request => request.Headers.Origin = "https://app.example";

await client.ConnectAsync(new Uri("ws://localhost/hostloom"), cancellationToken);
var welcome = await client.AwaitWelcomeAsync(cancellationToken);

await client.SendAsync(
    new HubFrame
    {
        Kind = HubFrameKind.Subscribe,
        StreamId = 1,
        Topic = "orders.changed",
        Credit = 8,
    },
    cancellationToken);

await client.AwaitSubscribedAsync(1, cancellationToken);
```

`ConfigureRequest` can add an Origin, cookies, or test authentication headers to the upgrade.
The client negotiates `hostloom.json.v1` by default; pass another `IWebSocketHubProtocol` to its
constructor to exercise a different codec.
