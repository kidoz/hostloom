# HostLoom.AspNetCore.WebSockets.Testing

In-process test client helpers for `HostLoom.AspNetCore.WebSockets` applications hosted by
ASP.NET Core `TestServer`.

```csharp
await using var client = new WebSocketTestClient(server);
client.ConfigureRequest = request => request.Headers.Origin = "https://app.example";

await client.ConnectAsync(new Uri("ws://localhost/hostloom"), cancellationToken);
var welcome = await client.AwaitWelcomeAsync(cancellationToken);

// Stream identifiers are Guids; the client picks one per stream and never reuses it.
var streamId = Guid.NewGuid();
await client.SendAsync(
    new HubFrame
    {
        Kind = HubFrameKind.Subscribe,
        StreamId = streamId,
        Topic = "orders.changed",
        Key = "customer-1",
        Credit = 8,
    },
    cancellationToken);

await client.AwaitSubscribedAsync(streamId, cancellationToken);
```

`ConfigureRequest` can add an Origin, cookies, or test authentication headers to the upgrade.
The client negotiates `hostloom.json.v1` by default; pass another `IWebSocketHubProtocol` to its
constructor to exercise a different codec.

A close the server starts, such as session expiry or an administrative disconnect, ends the
session only after the client answers it or the gateway's `CloseTimeout` elapses. Answer it with
`client.Socket.CloseOutputAsync` when a test expects the session to finish.
