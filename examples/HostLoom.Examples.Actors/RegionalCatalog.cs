using System.Collections.Immutable;
using HostLoom.Actors;

namespace HostLoom.Examples.Actors;

internal sealed record CatalogSnapshot(string Region, int Version, ImmutableArray<string> Items);

internal readonly record struct CatalogReply(
    CatalogSnapshot Snapshot,
    bool SourceUnavailable = false
);

// A null minimum requests a read. A positive minimum requests that version or a newer snapshot.
internal readonly record struct CatalogRequest
{
    private CatalogRequest(int version) => MinimumVersion = version;

    public int? MinimumVersion { get; }
    public static CatalogRequest Read => default;

    public static CatalogRequest Refresh(int minimumVersion)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(minimumVersion, 1);
        return new(minimumVersion);
    }
}

internal delegate ValueTask<ImmutableArray<string>> LoadCatalog(
    string region,
    int version,
    CancellationToken stopping
);

/// <summary>One instance per region, with all calls serialized by its owner.</summary>
internal sealed class RegionalCatalog(string region, LoadCatalog load)
    : IActor<CatalogRequest, CatalogReply>
{
    private CatalogSnapshot _published = new(region, 0, []);

    public async ValueTask<CatalogReply> ReceiveAsync(
        CatalogRequest request,
        CancellationToken stopping
    )
    {
        if (request.MinimumVersion is int version && version > _published.Version)
        {
            try
            {
                var items = await load(region, version, stopping).ConfigureAwait(false);
                _published = new(region, version, items);
            }
            catch (IOException)
            {
                return new(_published, SourceUnavailable: true);
            }
        }
        return new(_published);
    }
}

/// <summary>One client per region. Overlapping requests for one version share even a failed source attempt.</summary>
internal sealed class RegionalCatalogClient(
    Func<CatalogRequest, TimeSpan, ValueTask<CatalogReply>> send,
    TimeSpan operationTimeout,
    TimeProvider clock
)
{
    private readonly Lock _gate = new();
    private readonly Dictionary<int, TaskCompletionSource<CatalogReply>> _pending = [];

    internal static RegionalCatalogClient Spawn(
        ActorSystem system,
        string region,
        LoadCatalog load,
        TimeSpan operationTimeout,
        TimeProvider clock
    )
    {
        var reference = system
            .Spawn<CatalogRequest, CatalogReply>(() => new RegionalCatalog(region, load))
            .Ref;
        return new(
            (request, timeout) => reference.AskAsync(request, timeout),
            operationTimeout,
            clock
        );
    }

    internal ValueTask<CatalogReply> SendAsync(CatalogRequest request, TimeSpan callerTimeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(callerTimeout, TimeSpan.Zero);
        if (request.MinimumVersion is not int version)
            return send(request, callerTimeout);

        TaskCompletionSource<CatalogReply> attempt;
        Task<CatalogReply> wait;
        lock (_gate)
        {
            if (_pending.TryGetValue(version, out var active))
                return new(active.Task.WaitAsync(callerTimeout, clock));
            attempt = new(TaskCreationOptions.RunContinuationsAsynchronously);
            wait = attempt.Task.WaitAsync(callerTimeout, clock);
            _pending.Add(version, attempt);
        }
        _ = RunAsync(request, version, attempt);
        return new(wait);
    }

    private async Task RunAsync(
        CatalogRequest request,
        int version,
        TaskCompletionSource<CatalogReply> attempt
    )
    {
        try
        {
            var reply = await send(request, operationTimeout).ConfigureAwait(false);
            lock (_gate)
            {
                _pending.Remove(version);
                attempt.SetResult(reply);
            }
        }
        catch (Exception error)
        {
            lock (_gate)
            {
                _pending.Remove(version);
                attempt.SetException(error);
                // Every caller may have already timed out.
                _ = attempt.Task.Exception;
            }
        }
    }
}
