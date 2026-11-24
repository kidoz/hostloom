using HostLoom;
using HostLoom.Actors;
using HostLoom.Actors.DependencyInjection;
using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

if (args is ["--refresh"])
    return await HostLoom.Examples.Actors.CatalogRefreshExample.RunAsync().ConfigureAwait(false);

if (args is ["--recover", var directory])
    return await HostLoom
        .Examples.Actors.CatalogRecoveryExample.RunAsync(directory)
        .ConfigureAwait(false);

var builder = Host.CreateApplicationBuilder(args);
builder
    .Services.AddLocalActorSystem(1, 16, TimeSpan.FromSeconds(2))
    .AddLocalActor<CatalogCommand, CatalogSummary, CatalogActor>();
builder
    .Services.AddHostLoom()
    .UseInMemory()
    .AddHandler<ReadCatalog, CatalogSummary, ReadCatalogHandler>("catalog");

using var host = builder.Build();
using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
await host.StartAsync(deadline.Token).ConfigureAwait(false);
var catalog = host.Services.GetRequiredService<IActorRef<CatalogCommand, CatalogSummary>>();
var admitted = catalog.TryTell(new ObserveItems(3));
var client = host.Services.GetRequiredService<IRequestClient<ReadCatalog, CatalogSummary>>();
var summary = await client
    .GetResponseAsync("catalog", new ReadCatalog(), cancellationToken: deadline.Token)
    .ConfigureAwait(false);
await host.StopAsync(deadline.Token).ConfigureAwait(false);
Console.WriteLine($"Catalog items observed: {summary.Items}");
return admitted == ActorAdmission.Accepted && summary.Items == 3 ? 0 : 1;

internal abstract record CatalogCommand;

internal sealed record ObserveItems(int Count) : CatalogCommand;

internal sealed record ReadCatalog() : CatalogCommand, IRequest<CatalogSummary>;

internal sealed record CatalogSummary(int Items);

internal sealed class CatalogActor : IActor<CatalogCommand, CatalogSummary>
{
    private int _items;

    public ValueTask<CatalogSummary> ReceiveAsync(
        CatalogCommand message,
        CancellationToken stopping
    )
    {
        if (message is ObserveItems observation)
            _items += observation.Count;
        return ValueTask.FromResult(new CatalogSummary(_items));
    }
}

internal sealed class ReadCatalogHandler(IActorRef<CatalogCommand, CatalogSummary> catalog)
    : IRequestHandler<ReadCatalog, CatalogSummary>
{
    public ValueTask<CatalogSummary> HandleAsync(
        ReadCatalog request,
        CancellationToken cancellationToken
    ) => catalog.AskAsync(request, TimeSpan.FromSeconds(2), cancellationToken);
}
