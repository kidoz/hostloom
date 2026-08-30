# Your first request/response

In this tutorial you will build a complete HostLoom application from an
empty project: define a typed request contract, handle it, and get a typed
reply — all in-process, with no broker to install. At the end you will
have a running host and understand the three registrations every HostLoom
application makes.

!!! note "What you need"
    The [.NET SDK 10.0.400](https://dotnet.microsoft.com/download/dotnet/10.0)
    or later. Nothing else — the in-memory transport has no external
    dependencies.

## 1. Create the project

```text
dotnet new worker -n GreetingService
cd GreetingService
dotnet add package HostLoom
dotnet add package HostLoom.Transport.InMemory
```

Delete the template's `Worker.cs` — this tutorial replaces it. You will
finish with three files:

```text
GreetingService/
├── Program.cs
├── GetGreeting.cs
└── GetGreetingHandler.cs
```

## 2. Define the contract

A request is a type implementing `IRequest<TResponse>`. The response is an
ordinary type. Records fit naturally.

Create `GetGreeting.cs`:

```csharp
using HostLoom;

public sealed record GetGreeting(string Name) : IRequest<Greeting>;

public sealed record Greeting(string Text);
```

The contract is the whole agreement between caller and handler — for
this in-process example, nothing more is needed. When caller and handler
are *separate services*, both sides must produce the same logical type
name (assembly name plus full type name), which normally means sharing a
contracts assembly; see the
[wire envelope reference](../reference/wire-envelope.md#logical-type-names).

## 3. Handle it

A handler implements `IRequestHandler<TRequest, TResponse>`.

Create `GetGreetingHandler.cs`:

```csharp
using HostLoom;

public sealed class GetGreetingHandler : IRequestHandler<GetGreeting, Greeting>
{
    public ValueTask<Greeting> HandleAsync(
        GetGreeting request,
        CancellationToken cancellationToken
    ) => ValueTask.FromResult(new Greeting($"Hello, {request.Name}!"));
}
```

Handlers are resolved from a fresh dependency-injection scope on every
delivery attempt, so take repositories and other scoped services through
the constructor as you would in an ASP.NET Core controller.

## 4. Compose the host

Replace the entire contents of `Program.cs` with:

```csharp
using HostLoom;
using HostLoom.Transport.InMemory;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddHostLoom(options => options.RequestTimeout = TimeSpan.FromSeconds(10))
    .UseInMemory()
    .AddHandler<GetGreeting, Greeting, GetGreetingHandler>("greetings");

var host = builder.Build();
await host.StartAsync();
```

Three registrations did all the work:

1. `AddHostLoom` adds the messaging runtime and its options
   (`RequestTimeout` defaults to 30 seconds).
2. `UseInMemory` selects the transport.
3. `AddHandler` binds the contract to your handler at the logical address
   `"greetings"` and makes the typed client resolvable for the same
   contract. A client-only application — one with no local handler —
   registers the contract with `AddRequestClient<GetGreeting, Greeting>()`
   instead.

## 5. Send a request

Add these lines at the **end of `Program.cs`**, after `await
host.StartAsync();`:

```csharp
var client = host.Services.GetRequiredService<IRequestClient<GetGreeting, Greeting>>();

var reply = await client.GetResponseAsync("greetings", new GetGreeting("Ada"));
Console.WriteLine(reply.Text);

await host.StopAsync();
```

## 6. Verify

```text
dotnet run
```

Among the host's startup log lines you should see:

```text
Hello, Ada!
```

If you see a `RequestTimeoutException` instead, check that the address in
`GetResponseAsync` matches the one in `AddHandler` — a request sent to an
address nobody listens on waits out the request timeout.

## What just happened

The client serialized `GetGreeting` into a wire envelope carrying a
message id, correlation id, and logical type name, the transport delivered
it to the handler registered at `"greetings"`, the handler ran inside its
own dependency-injection scope, and the reply travelled back correlated to
your request. Swapping `UseInMemory()` for `UseRabbitMq(...)` or
`UseKafka(...)` changes none of the code above — only the topology
underneath it.

Two failure shapes are worth knowing from day one: a handler exception
surfaces on the caller's side as a `RemoteRequestException` whose
`ErrorType` names the remote exception type (no stack trace crosses the
wire), and a request sent to an address nobody listens on fails with a
`RequestTimeoutException` when `RequestTimeout` elapses.

## Where next

- Fan events out to multiple subscribers in
  [Publish and subscribe](publish-subscribe.md).
- Move this exact application onto a broker with
  [Run over RabbitMQ](../how-to/use-rabbitmq.md).
- Understand what the envelope carries in the
  [wire envelope reference](../reference/wire-envelope.md).
