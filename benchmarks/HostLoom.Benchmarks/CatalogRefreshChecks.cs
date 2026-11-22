using System.Collections.Concurrent;
using System.Collections.Immutable;
using HostLoom.Actors;
using HostLoom.Conformance;
using HostLoom.Examples.Actors;
using static HostLoom.Benchmarks.CatalogRefreshPilot;

namespace HostLoom.Benchmarks;

internal static class CatalogRefreshChecks
{
    // Each scripted read exposes an entry signal and a completion owned by the test.
    private sealed class ReadGate
    {
        internal TaskCompletionSource Entered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource<bool> Succeeded { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class Source
    {
        private readonly ConcurrentDictionary<int, ReadGate> _gates = new();
        private readonly ConcurrentDictionary<(string, int), int> _loads = new();

        internal ReadGate PauseNext(int version)
        {
            var gate = new ReadGate();
            if (!_gates.TryAdd(version, gate))
                throw new InvalidOperationException("Read already paused.");
            return gate;
        }

        internal int Reads(string region, int version) =>
            _loads.GetValueOrDefault((region, version));

        internal async ValueTask<ImmutableArray<string>> LoadAsync(
            string region,
            int version,
            CancellationToken stopping
        )
        {
            _loads.AddOrUpdate((region, version), 1, static (_, count) => count + 1);
            if (region == "eu" && _gates.TryRemove(version, out var gate))
            {
                gate.Entered.SetResult();
                if (!await gate.Succeeded.Task.WaitAsync(Guard, stopping).ConfigureAwait(false))
                    throw new IOException("Catalog source unavailable.");
            }
            return ["notebook", FormattableString.Invariant($"{region}-{version}")];
        }
    }

    internal static async Task RunAsync()
    {
        await RunCaseAsync(false).ConfigureAwait(false);
        await RunCaseAsync(true).ConfigureAwait(false);
        await CheckActorDrainAsync().ConfigureAwait(false);
    }

    private static async Task RunCaseAsync(bool actors)
    {
        var clock = new ManualTimeProvider();
        var source = new Source();
        var workers = new Workers(actors, source.LoadAsync, clock);
        await using var owned = workers.ConfigureAwait(false);
        await CheckAsync(workers, source, clock)
            .WaitAsync(TimeSpan.FromSeconds(15))
            .ConfigureAwait(false);
        Console.WriteLine(
            $"# {(actors ? "actor" : "semaphore")} checks passed; eu v1 loads={source.Reads("eu", 1)}, v2 loads including retry={source.Reads("eu", 2)}, reconciled v3 loads={source.Reads("eu", 3)}, failed v4 loads={source.Reads("eu", 4)}"
        );
    }

    private static async Task CheckAsync(Workers workers, Source source, ManualTimeProvider clock)
    {
        var eu = workers["eu"];
        var first = source.PauseNext(1);
        var a = eu(CatalogRequest.Refresh(1), Guard).AsTask();
        await first.Entered.Task.WaitAsync(Guard).ConfigureAwait(false);
        var b = eu(CatalogRequest.Refresh(1), Guard).AsTask();
        var other = await workers["us"](CatalogRequest.Refresh(1), Guard).ConfigureAwait(false);
        Check(
            other.Snapshot.Region == "us"
                && other.Snapshot.Version == 1
                && !a.IsCompleted
                && !b.IsCompleted
        );
        first.Succeeded.SetResult(true);
        var replies = await Task.WhenAll(a, b).ConfigureAwait(false);
        var original = replies[0].Snapshot;
        Check(
            !replies[0].SourceUnavailable
                && !replies[1].SourceUnavailable
                && ReferenceEquals(original, replies[1].Snapshot)
        );
        Check(
            original.Version == 1
                && original.Items.SequenceEqual(["notebook", "eu-1"])
                && source.Reads("eu", 1) == 1
        );

        var failing = source.PauseNext(2);
        var attempt = eu(CatalogRequest.Refresh(2), Guard).AsTask();
        await failing.Entered.Task.WaitAsync(Guard).ConfigureAwait(false);
        var duplicate = eu(CatalogRequest.Refresh(2), Guard).AsTask();
        failing.Succeeded.SetResult(false);
        var failure = await attempt.ConfigureAwait(false);
        var alsoFailed = await duplicate.ConfigureAwait(false);
        Check(failure.SourceUnavailable && alsoFailed.SourceUnavailable);
        Check(
            ReferenceEquals(failure.Snapshot, original)
                && ReferenceEquals(alsoFailed.Snapshot, original)
        );
        Check(source.Reads("eu", 2) == 1);
        var current = await eu(CatalogRequest.Read, Guard).ConfigureAwait(false);
        Check(!current.SourceUnavailable && ReferenceEquals(current.Snapshot, original));
        var retried = await eu(CatalogRequest.Refresh(2), Guard).ConfigureAwait(false);
        Check(
            !retried.SourceUnavailable
                && retried.Snapshot.Version == 2
                && source.Reads("eu", 2) == 2
        );
        var stale = await eu(CatalogRequest.Refresh(1), Guard).ConfigureAwait(false);
        Check(ReferenceEquals(stale.Snapshot, retried.Snapshot) && source.Reads("eu", 1) == 1);

        await ReconcileAsync(eu, source, clock, 3, succeeds: true).ConfigureAwait(false);
        await ReconcileAsync(eu, source, clock, 4, succeeds: false).ConfigureAwait(false);
    }

    private static async Task ReconcileAsync(
        Send send,
        Source source,
        ManualTimeProvider clock,
        int version,
        bool succeeds
    )
    {
        var held = source.PauseNext(version);
        var timeout = TimeSpan.FromMilliseconds(25);
        var request = send(CatalogRequest.Refresh(version), timeout).AsTask();
        await held.Entered.Task.WaitAsync(Guard).ConfigureAwait(false);
        clock.Advance(timeout);
        try
        {
            await request.WaitAsync(Guard).ConfigureAwait(false);
            throw new InvalidOperationException("Refresh caller missed its deadline.");
        }
        catch (TimeoutException) when (request.IsFaulted) { }
        var read = send(CatalogRequest.Read, Guard).AsTask();
        Check(!read.IsCompleted);
        held.Succeeded.SetResult(succeeds);
        var current = await read.WaitAsync(Guard).ConfigureAwait(false);
        var expected = succeeds ? version : version - 1;
        Check(
            !current.SourceUnavailable
                && current.Snapshot.Version == expected
                && current.Snapshot.Items.SequenceEqual([
                    "notebook",
                    FormattableString.Invariant($"eu-{expected}"),
                ])
        );
        Check(source.Reads("eu", version) == 1);
    }

    private static async Task CheckActorDrainAsync()
    {
        var clock = new ManualTimeProvider();
        var source = new Source();
        var system = new ActorSystem(1, 4, Guard, clock);
        await using var owned = system.ConfigureAwait(false);
        var client = RegionalCatalogClient.Spawn(system, "eu", source.LoadAsync, Guard, clock);
        var held = source.PauseNext(1);
        try
        {
            var callerBudget = TimeSpan.FromMilliseconds(25);
            var caller = client.SendAsync(CatalogRequest.Refresh(1), callerBudget).AsTask();
            await held.Entered.Task.WaitAsync(Guard).ConfigureAwait(false);
            clock.Advance(callerBudget);
            try
            {
                await caller.WaitAsync(Guard).ConfigureAwait(false);
                throw new InvalidOperationException("Expected caller timeout.");
            }
            catch (TimeoutException) when (caller.IsFaulted) { }
            var reconciliation = client.SendAsync(CatalogRequest.Read, Guard).AsTask();
            var stop = system.StopAsync();
            Check(!stop.IsCompleted && !reconciliation.IsCompleted);
            try
            {
                await client.SendAsync(CatalogRequest.Read, Guard).ConfigureAwait(false);
                throw new InvalidOperationException("Expected closed mailbox.");
            }
            catch (ActorAdmissionException e) when (e.Reason == ActorAdmission.Stopped) { }
            held.Succeeded.SetResult(true);
            var last = await reconciliation.WaitAsync(Guard).ConfigureAwait(false);
            await stop.WaitAsync(Guard).ConfigureAwait(false);
            Check(
                last.Snapshot.Version == 1 && !last.SourceUnavailable && source.Reads("eu", 1) == 1
            );
            Console.WriteLine(
                $"# actor shutdown drained abandoned refresh and admitted read; published version={last.Snapshot.Version}"
            );
        }
        finally
        {
            held.Succeeded.TrySetResult(false);
        }
    }

    private static void Check(
        bool success,
        [System.Runtime.CompilerServices.CallerArgumentExpression(nameof(success))]
            string? check = null
    )
    {
        if (!success)
            throw new InvalidOperationException($"Catalog refresh invariant failed: {check}");
    }
}
