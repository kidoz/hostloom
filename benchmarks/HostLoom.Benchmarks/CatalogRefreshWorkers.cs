using HostLoom.Actors;
using HostLoom.Examples.Actors;

namespace HostLoom.Benchmarks;

// The experiment owns a fixed set of regions. It joins outstanding work before disposing the gates.
internal sealed class CatalogRefreshWorkers : IAsyncDisposable
{
    private readonly ActorSystem? _system;
    private readonly List<SemaphoreSlim> _gates = [];
    private readonly Dictionary<string, RegionalCatalogClient> _clients = new(
        StringComparer.Ordinal
    );

    internal CatalogRefreshWorkers(
        string[] regions,
        bool actors,
        LoadCatalog source,
        TimeSpan timeout,
        TimeProvider clock,
        int mailboxCapacity = 32,
        Action<Task>? observeSemaphoreTurn = null
    )
    {
        if (actors)
            _system = new ActorSystem(regions.Length, mailboxCapacity, timeout, clock);
        foreach (var region in regions)
        {
            if (_system is not null)
            {
                _clients.Add(
                    region,
                    RegionalCatalogClient.Spawn(_system, region, source, timeout, clock)
                );
            }
            else
            {
                var worker = new RegionalCatalog(region, source);
                var gate = new SemaphoreSlim(1, 1);
                _gates.Add(gate);
                var client = new RegionalCatalogClient(
                    (request, budget) =>
                    {
                        var turn = UnderGateAsync(gate, worker, request);
                        observeSemaphoreTurn?.Invoke(turn);
                        return new(turn.WaitAsync(budget, clock));
                    },
                    timeout,
                    clock
                );
                _clients.Add(region, client);
            }
        }
    }

    internal RegionalCatalogClient this[string region] => _clients[region];

    private static async Task<CatalogReply> UnderGateAsync(
        SemaphoreSlim gate,
        RegionalCatalog worker,
        CatalogRequest request
    )
    {
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            return await worker.ReceiveAsync(request, CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_system is not null)
            await _system.DisposeAsync().ConfigureAwait(false);
        foreach (var gate in _gates)
            gate.Dispose();
    }
}
