using System.Collections.Immutable;
using HostLoom.Actors;

namespace HostLoom.Examples.Actors;

internal static class CatalogRefreshExample
{
    private static readonly string[] SourceItems = ["books", "music"];

    public static async Task<int> RunAsync()
    {
        var directory = Directory.CreateTempSubdirectory("hostloom-catalog-example-");
        try
        {
            await File.WriteAllLinesAsync(Path.Combine(directory.FullName, "eu-1.txt"), SourceItems)
                .ConfigureAwait(false);
            var reads = 0;
            async ValueTask<ImmutableArray<string>> LoadAsync(
                string region,
                int version,
                CancellationToken stopping
            )
            {
                Interlocked.Increment(ref reads);
                var file = FormattableString.Invariant($"{region}-{version}.txt");
                return
                [
                    .. await File.ReadAllLinesAsync(
                            Path.Combine(directory.FullName, file),
                            stopping
                        )
                        .ConfigureAwait(false),
                ];
            }
            var budget = TimeSpan.FromSeconds(5);
            var system = new ActorSystem(1, 16, budget);
            await using var owned = system.ConfigureAwait(false);
            var catalog = RegionalCatalogClient.Spawn(
                system,
                "eu",
                LoadAsync,
                budget,
                TimeProvider.System
            );
            var refresh = CatalogRequest.Refresh(1);
            var replies = await Task.WhenAll(
                    catalog.SendAsync(refresh, budget).AsTask(),
                    catalog.SendAsync(refresh, budget).AsTask()
                )
                .ConfigureAwait(false);
            var versionOneReads = reads;
            var missing = await catalog
                .SendAsync(CatalogRequest.Refresh(2), budget)
                .ConfigureAwait(false);
            var current = await catalog
                .SendAsync(CatalogRequest.Read, budget)
                .ConfigureAwait(false);
            if (current.Status != CatalogStatus.Published || current.Snapshot is null)
                return 1;
            await system.StopAsync().ConfigureAwait(false);
            var stopped = await catalog
                .SendAsync(CatalogRequest.Read, budget)
                .ConfigureAwait(false);
            Console.WriteLine(
                $"Region {current.Snapshot.Region}, published version {current.Snapshot.Version}, items {string.Join(", ", current.Snapshot.Items)}. Version 1 source reads: {versionOneReads}."
            );
            return
                replies.All(r =>
                    r.Status == CatalogStatus.Published
                    && ReferenceEquals(r.Snapshot, current.Snapshot)
                )
                && missing.Status == CatalogStatus.SourceUnavailable
                && stopped.Status == CatalogStatus.Unavailable
                && current.Snapshot.Version == 1
                && current.Snapshot.Items.SequenceEqual(SourceItems)
                && versionOneReads == 1
                && reads == 2
                ? 0
                : 1;
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }
}
