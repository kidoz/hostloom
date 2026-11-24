using System.Collections.Immutable;
using System.Globalization;
using HostLoom.Actors;

namespace HostLoom.Examples.Actors;

internal static class CatalogRecoveryExample
{
    internal static async Task<int> RunAsync(string directory)
    {
        async ValueTask<int> CurrentAsync(string region, CancellationToken token)
        {
            var text = await File.ReadAllTextAsync(
                    Path.Combine(directory, $"{region}.current"),
                    token
                )
                .ConfigureAwait(false);
            return int.Parse(text.Trim(), CultureInfo.InvariantCulture);
        }

        async ValueTask<ImmutableArray<string>> LoadAsync(
            string region,
            int version,
            CancellationToken token
        )
        {
            var name = FormattableString.Invariant($"{region}-{version}.txt");
            return
            [
                .. await File.ReadAllLinesAsync(Path.Combine(directory, name), token)
                    .ConfigureAwait(false),
            ];
        }

        var budget = TimeSpan.FromSeconds(5);
        var system = new ActorSystem(1, 16, budget);
        await using var owned = system.ConfigureAwait(false);
        using var stopping = new CancellationTokenSource();
        var owner = new RegionalCatalogOwner(
            system,
            "eu",
            CurrentAsync,
            LoadAsync,
            budget,
            TimeProvider.System
        );
        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var lifetime = owner.RunAsync(_ => ready.TrySetResult(), stopping.Token);
        try
        {
            if (await Task.WhenAny(ready.Task, lifetime).ConfigureAwait(false) == lifetime)
            {
                await lifetime.ConfigureAwait(false);
                return 1;
            }
            var request = CatalogRequest.Read;
            while (true)
            {
                try
                {
                    var reply = await owner.SendAsync(request, budget).ConfigureAwait(false);
                    if (reply is { Status: CatalogStatus.Published, Snapshot: { } snapshot })
                        Console.WriteLine(
                            FormattableString.Invariant(
                                $"{snapshot.Region} {snapshot.Version} {string.Join(", ", snapshot.Items)}"
                            )
                        );
                    else
                        Console.WriteLine(reply.Status);
                }
                catch (ActorFaultException error)
                {
                    Console.WriteLine(error.Message);
                }
                var command = await Console.In.ReadLineAsync().ConfigureAwait(false);
                if (command is null)
                    return 0;
                if (command == "refresh")
                    request = CatalogRequest.Refresh(
                        await CurrentAsync("eu", stopping.Token).ConfigureAwait(false)
                    );
                else if (command == "read")
                    request = CatalogRequest.Read;
                else
                    return 1;
            }
        }
        finally
        {
            await stopping.CancelAsync().ConfigureAwait(false);
            await lifetime.ConfigureAwait(false);
        }
    }
}
