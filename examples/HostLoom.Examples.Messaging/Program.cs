using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using HostLoom;
using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<DeliveryLog>();
builder
    .Services.AddHostLoom(options => options.RequestTimeout = TimeSpan.FromSeconds(10))
    .UseInMemory()
    .AddHandler<GetGreeting, Greeting, GetGreetingHandler>("greetings")
    .AddSubscriber<OrderPlaced, AuditHandler>("orders", subscription: "audit")
    .AddSubscriber<OrderPlaced, ShippingHandler>("orders", subscription: "shipping")
    .AddStreamHandler<ListCatalog, string, ListCatalogHandler>("catalog");

using var host = builder.Build();
using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
var cancellationToken = timeout.Token;
await host.StartAsync(cancellationToken).ConfigureAwait(false);

var problems = new List<string>();
var greetings = host.Services.GetRequiredService<IRequestClient<GetGreeting, Greeting>>();
var reply = await greetings
    .GetResponseAsync("greetings", new GetGreeting("Ada"), cancellationToken: cancellationToken)
    .ConfigureAwait(false);
Expect(reply.Text == "Hello, Ada!", $"unexpected greeting: {reply.Text}");
Console.WriteLine(reply.Text);

var publisher = host.Services.GetRequiredService<IPublishEndpoint>();
await publisher
    .PublishAsync("orders", new OrderPlaced("A-1"), cancellationToken)
    .ConfigureAwait(false);
var deliveries = host.Services.GetRequiredService<DeliveryLog>().Snapshot();
Expect(
    deliveries.SequenceEqual(["audit:A-1", "shipping:A-1"]),
    $"unexpected event deliveries: {string.Join(", ", deliveries)}"
);
foreach (var delivery in deliveries)
{
    Console.WriteLine(delivery);
}

var catalog = host.Services.GetRequiredService<IStreamRequestClient<ListCatalog, string>>();
var categories = new List<string>();
await foreach (
    var category in catalog
        .StreamAsync("catalog", new ListCatalog("eu"), cancellationToken: cancellationToken)
        .WithCancellation(cancellationToken)
        .ConfigureAwait(false)
)
{
    categories.Add(category);
}
Expect(
    categories.SequenceEqual(["books", "music"]),
    $"unexpected catalog categories: {string.Join(", ", categories)}"
);
Console.WriteLine($"catalog: {string.Join(", ", categories)}");

await host.StopAsync(cancellationToken).ConfigureAwait(false);
Console.WriteLine(
    problems.Count == 0
        ? "Messaging verified: request/response, event fan-out, and streaming all match."
        : "Messaging mismatches: " + string.Join("; ", problems)
);
return problems.Count == 0 ? 0 : 1;

void Expect(bool condition, string problem)
{
    if (!condition)
    {
        problems.Add(problem);
    }
}

internal sealed record GetGreeting(string Name) : IRequest<Greeting>;

internal sealed record Greeting(string Text);

internal sealed class GetGreetingHandler : IRequestHandler<GetGreeting, Greeting>
{
    public ValueTask<Greeting> HandleAsync(
        GetGreeting request,
        CancellationToken cancellationToken
    ) => ValueTask.FromResult(new Greeting($"Hello, {request.Name}!"));
}

internal sealed record OrderPlaced(string Reference) : IEvent;

internal sealed class AuditHandler(DeliveryLog deliveries) : IEventHandler<OrderPlaced>
{
    public ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken)
    {
        deliveries.Record($"audit:{@event.Reference}");
        return ValueTask.CompletedTask;
    }
}

internal sealed class ShippingHandler(DeliveryLog deliveries) : IEventHandler<OrderPlaced>
{
    public ValueTask HandleAsync(OrderPlaced @event, CancellationToken cancellationToken)
    {
        deliveries.Record($"shipping:{@event.Reference}");
        return ValueTask.CompletedTask;
    }
}

internal sealed record ListCatalog(string Region) : IStreamRequest<string>;

internal sealed class ListCatalogHandler : IStreamRequestHandler<ListCatalog, string>
{
    public async IAsyncEnumerable<string> HandleAsync(
        ListCatalog request,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        string[] categories = request.Region == "eu" ? ["books", "music"] : [];
        foreach (var category in categories)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return category;
            await Task.Yield();
        }
    }
}

internal sealed class DeliveryLog
{
    private readonly ConcurrentQueue<string> _entries = new();

    public void Record(string entry) => _entries.Enqueue(entry);

    public string[] Snapshot() => [.. _entries.Order(StringComparer.Ordinal)];
}
