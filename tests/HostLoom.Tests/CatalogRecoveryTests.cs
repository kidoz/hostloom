using System.Collections.Immutable;
using System.Threading.Channels;
using HostLoom.Actors;
using HostLoom.Examples.Actors;
using Xunit;

namespace HostLoom.Tests;

public sealed class CatalogRecoveryTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task FaultClosesOldReferenceAndRebuildsOnlyTheAffectedRegion(bool failRebuild)
    {
        var active = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fault = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var rebuilding = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var resume = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        ValueTask<int> CurrentAsync(string region, CancellationToken token) =>
            ValueTask.FromResult(Interlocked.Increment(ref attempts) == 1 ? 1 : 3);

        async ValueTask<ImmutableArray<string>> LoadAsync(
            string region,
            int version,
            CancellationToken token
        )
        {
            if (version == 2)
            {
                active.SetResult();
                await fault.Task.WaitAsync(Bound, token);
                throw new FormatException("Invalid catalog row.");
            }
            if (version == 3 && attempts == 2)
            {
                rebuilding.SetResult();
                await resume.Task.WaitAsync(Bound, token);
                if (failRebuild)
                    throw new IOException("Catalog source unavailable.");
            }
            return [FormattableString.Invariant($"{region}-{version}")];
        }

        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(Token);
        await using var system = new ActorSystem(2, 2, Bound);
        var peer = RegionalCatalogClient.Spawn(system, "us", LoadAsync, Bound, TimeProvider.System);
        var owner = new RegionalCatalogOwner(
            system,
            "eu",
            CurrentAsync,
            LoadAsync,
            Bound,
            TimeProvider.System
        );
        var ready = Channel.CreateUnbounded<ActorHandle<CatalogRequest, CatalogReply>>();
        void Publish(ActorHandle<CatalogRequest, CatalogReply> handle) =>
            Assert.True(ready.Writer.TryWrite(handle));
        Task<ActorHandle<CatalogRequest, CatalogReply>> NextAsync() =>
            ready.Reader.ReadAsync(Token).AsTask().WaitAsync(Bound, Token);

        var lifetime = owner.RunAsync(Publish, stopping.Token);
        try
        {
            var original = await NextAsync();
            Assert.Equal(1, (await owner.SendAsync(CatalogRequest.Read, Bound)).Snapshot?.Version);
            var started = owner.SendAsync(CatalogRequest.Refresh(2), Bound).AsTask();
            await active.Task.WaitAsync(Bound, Token);
            var queued = owner.SendAsync(CatalogRequest.Read, Bound).AsTask();
            Assert.Equal(ActorAdmission.Accepted, original.Ref.TryTell(CatalogRequest.Read));
            fault.SetResult();

            await Assert.ThrowsAsync<ActorFaultException>(() => started).WaitAsync(Bound, Token);
            await Assert
                .ThrowsAsync<ActorTerminatedException>(() => queued)
                .WaitAsync(Bound, Token);
            await rebuilding.Task.WaitAsync(Bound, Token);
            Assert.Equal(2, system.Snapshot.Discarded);
            Assert.Equal(
                new CatalogReply(CatalogStatus.Unavailable),
                await owner.SendAsync(CatalogRequest.Read, Bound)
            );
            Assert.Equal(
                "us",
                (await peer.SendAsync(CatalogRequest.Refresh(1), Bound)).Snapshot?.Region
            );
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                owner.RunAsync(Publish, stopping.Token)
            );
            resume.SetResult();
            if (failRebuild)
            {
                await Assert.ThrowsAsync<IOException>(() => lifetime).WaitAsync(Bound, Token);
                Assert.Equal(1, system.Snapshot.Actors);
                Assert.Equal(
                    new CatalogReply(CatalogStatus.Unavailable),
                    await owner.SendAsync(CatalogRequest.Read, Bound)
                );
                Assert.Equal(
                    1,
                    (await peer.SendAsync(CatalogRequest.Read, Bound)).Snapshot?.Version
                );
                lifetime = owner.RunAsync(Publish, stopping.Token);
            }
            var replacement = await NextAsync();
            Assert.NotSame(original.Ref, replacement.Ref);
            Assert.Equal(ActorAdmission.Stopped, original.Ref.TryTell(CatalogRequest.Read));
            var result = await owner.SendAsync(CatalogRequest.Read, Bound);
            Assert.Equal(CatalogStatus.Published, result.Status);
            Assert.Equal(3, result.Snapshot?.Version);
            Assert.Equal(["eu-3"], result.Snapshot!.Items.ToArray());
            await replacement.StopAsync(Token).WaitAsync(Bound, Token);
            await lifetime.WaitAsync(Bound, Token);
            Assert.Equal(failRebuild ? 3 : 2, attempts);
        }
        finally
        {
            fault.TrySetResult();
            resume.TrySetResult();
            await stopping.CancelAsync();
        }
    }

    [Fact]
    public async Task CancellationDuringConstructionDoesNotPublishTheRegion()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async ValueTask<ImmutableArray<string>> LoadAsync(
            string region,
            int version,
            CancellationToken token
        )
        {
            entered.SetResult();
            await release.Task.WaitAsync(Bound, Token);
            return ["books"];
        }
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(Token);
        await using var system = new ActorSystem(1, 1, Bound);
        var owner = new RegionalCatalogOwner(
            system,
            "eu",
            (_, _) => ValueTask.FromResult(1),
            LoadAsync,
            Bound,
            TimeProvider.System
        );
        var lifetime = owner.RunAsync(
            _ => Assert.Fail("Cancelled reconstruction published a reference."),
            stopping.Token
        );
        await entered.Task.WaitAsync(Bound, Token);
        await stopping.CancelAsync();
        release.SetResult();
        await Assert
            .ThrowsAnyAsync<OperationCanceledException>(() => lifetime)
            .WaitAsync(Bound, Token);
        Assert.Equal(0, system.Snapshot.Actors);
        Assert.Equal(
            new CatalogReply(CatalogStatus.Unavailable),
            await owner.SendAsync(CatalogRequest.Read, Bound)
        );
    }
}
