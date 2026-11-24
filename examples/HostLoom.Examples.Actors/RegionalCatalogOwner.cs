using HostLoom.Actors;

namespace HostLoom.Examples.Actors;

internal sealed class RegionalCatalogOwner(
    ActorSystem system,
    string region,
    Func<string, CancellationToken, ValueTask<int>> currentVersion,
    LoadCatalog load,
    TimeSpan operationTimeout,
    TimeProvider clock
)
{
    private RegionalCatalogClient? _client;
    private int _running;

    internal ValueTask<CatalogReply> SendAsync(CatalogRequest request, TimeSpan timeout) =>
        Volatile.Read(ref _client)?.SendAsync(request, timeout)
        ?? ValueTask.FromResult(new CatalogReply(CatalogStatus.Unavailable));

    // A failed rebuild ends this run. The host can explicitly retry once the source is healthy.
    internal async Task RunAsync(
        Action<ActorHandle<CatalogRequest, CatalogReply>> ready,
        CancellationToken stopping
    )
    {
        if (Interlocked.CompareExchange(ref _running, 1, 0) != 0)
            throw new InvalidOperationException("This region already has an owner running.");
        try
        {
            while (!stopping.IsCancellationRequested)
            {
                var handle = await system
                    .SpawnAsync<CatalogRequest, CatalogReply>(() => RestoreAsync(stopping))
                    .ConfigureAwait(false);

                using var registration = stopping.Register(() =>
                    _ = handle.StopAsync(CancellationToken.None)
                );
                try
                {
                    if (!stopping.IsCancellationRequested)
                    {
                        Volatile.Write(
                            ref _client,
                            RegionalCatalogClient.FromReference(handle.Ref, operationTimeout, clock)
                        );
                        ready(handle);
                    }
                    try
                    {
                        await handle.Completion.ConfigureAwait(false);
                        return;
                    }
                    catch (ActorFaultException)
                    {
                        // Completion releases the old population slot before the next rebuild.
                    }
                }
                finally
                {
                    Volatile.Write(ref _client, null);
                    if (!handle.Completion.IsCompleted)
                        await handle.StopAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            Volatile.Write(ref _running, 0);
        }
    }

    private async ValueTask<IActor<CatalogRequest, CatalogReply>> RestoreAsync(
        CancellationToken stopping
    )
    {
        var version = await currentVersion(region, stopping).ConfigureAwait(false);
        if (version < 1)
            throw new InvalidDataException("The catalog version must be positive.");
        var items = await load(region, version, stopping).ConfigureAwait(false);
        stopping.ThrowIfCancellationRequested();
        return new RegionalCatalog(new CatalogSnapshot(region, version, items), load);
    }
}
