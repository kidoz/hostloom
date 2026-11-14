using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using HostLoom.DataGrid.Storage;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiskFullTests))]
public sealed class DataGridDiskFullTlsTests(ITestOutputHelper output)
{
    [Fact(Timeout = 120000)]
    public async Task LeaderEnospcNeverAcknowledgesUnreplicatedWriteAndHealthyMajorityReplaysIdentity()
    {
        using var a = new OwnedDiskFullVolume(0);
        using var b = new OwnedDiskFullVolume(1);
        using var c = new OwnedDiskFullVolume(2);
        var volumes = new[] { a, b, c };
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        deadline.CancelAfter(TimeSpan.FromSeconds(100));
        var token = deadline.Token;
        var armed = -1;
        var fills = 0;
        var filled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var lab = new DataGridSessionPressureTlsTests.Lab(
            1,
            observe: (member, step) =>
            {
                if (member != Volatile.Read(ref armed))
                    return;
                if (step == DataGridStorageStep.WalWriteStarting)
                {
                    Assert.InRange(Interlocked.Increment(ref fills), 1, 16);
                    volumes[member].Fill();
                    filled.TrySetResult();
                }
                else if (step == DataGridStorageStep.WalFlushed)
                {
                    // A small record can use the selected WAL's already allocated final page.
                    // Release before the next reserve check, then fill again before its I/O.
                    volumes[member].Release();
                }
            },
            paths: volumes.Select(volume => volume.StorePath).ToArray(),
            storageLimits: DataGridDiskFullTests.Limits
        );
        var leader = await lab.Cluster.Leader(token);
        var client = lab.Client(0, leader);
        var session = await lab.Open(client, token);
        var originalRequest = DataGridClientRequest.Mutate(
            DataGridClientCodecTests.Mutation(session, value: 1)
        );
        var original = Item(await lab.Send(client, originalRequest, token));
        await lab.AppliedEverywhere(original.Version.LogIndex, token);
        var healthy = Enumerable.Range(0, 3).Where(i => i != leader).ToArray();
        foreach (var member in healthy)
            await lab.Cluster.Stop(member);
        var walPath = DataGridDiskFullTests.Selected(volumes[leader].StorePath);
        var prefix = await File.ReadAllBytesAsync(walPath, token);
        var uncertainRequest = DataGridClientRequest.Mutate(
            new(
                DataGridMutationKind.Put,
                DataGridPeerFixture.Map,
                session,
                2,
                DataGridExpiryKind.Never,
                0,
                [new(DataGridPeerFixture.Key, new byte[65536])]
            )
        );
        Volatile.Write(ref armed, leader);
        var unknown = await lab.Send(client, uncertainRequest, token);
        await filled.Task.WaitAsync(token);
        Assert.Equal(DataGridClientFailureCode.OutcomeUnknown, unknown.Reply.Failure!.Code);
        Assert.True(unknown.MayHaveBeenAdmitted);
        Assert.Null(unknown.Reply.Result);
        var runtime = lab.Cluster.Nodes[leader]!;
        await runtime.Completion.WaitAsync(token);
        Assert.True(runtime.Diagnostics.Faulted);
        Assert.False(runtime.Diagnostics.LocalReadyHint);
        Assert.True(runtime.Diagnostics.Storage.Closed);
        output.WriteLine($"Storage failure after {fills} fill attempts: {runtime.Failure}");
        DataGridDiskFullTests.Enospc(Assert.IsAssignableFrom<IOException>(runtime.Failure));
        Assert.Equal(
            prefix,
            (await File.ReadAllBytesAsync(walPath, token)).AsSpan(0, prefix.Length).ToArray()
        );
        output.WriteLine(
            $"Leader {leader}: original write acknowledged at {original.Version.LogIndex}; next identity returned OutcomeUnknown/admitted, {unknown.Attempts.Count} SDK attempts; actual ENOSPC; owner closed; selected WAL {prefix.Length}->{new FileInfo(walPath).Length} bytes."
        );
        await lab.Cluster.Stop(leader);
        volumes[leader].Release();
        var files = DataGridDiskFullTests.Files(volumes[leader].StorePath);
        var recoveryError = Record.Exception(() =>
        {
            using var recovered = DataGridFileStore.Open(
                volumes[leader].StorePath,
                lab.Peer.Configuration,
                leader,
                DataGridDiskFullTests.Limits
            );
            Assert.True(recovered.Image.CommitIndex >= original.Version.LogIndex);
        });
        if (recoveryError is not null)
        {
            Assert.IsType<InvalidDataException>(recoveryError);
            Assert.Equal(files, DataGridDiskFullTests.Files(volumes[leader].StorePath));
            output.WriteLine(
                "Faulted member has a torn tail and refuses restart without modifying any file. Its rejoin requires a separately designed recovery procedure."
            );
        }
        else
            output.WriteLine("Faulted member's complete prefix reopens after space release.");
        foreach (var member in healthy)
            lab.Cluster.Start(member);
        client = lab.Client(0, await lab.Cluster.Leader(token));
        Assert.Equal(original, Item(await RecoverSend(originalRequest)));
        await lab.AssertRead(client, original.Version, 1, token);
        var resolved = Item(await RecoverSend(uncertainRequest));
        Assert.Equal(DataGridMutationStatus.Stored, resolved.Status);
        Assert.True(resolved.Version.LogIndex > original.Version.LogIndex);
        Assert.Equal(resolved, Item(await RecoverSend(uncertainRequest)));
        foreach (var member in healthy)
            await lab.Cluster.Nodes[member]!.WaitForAppliedAsync(resolved.Version.LogIndex, token);
        await lab.StopAll();
        foreach (var member in healthy)
            lab.Cluster.Start(member);
        client = lab.Client(0, await lab.Cluster.Leader(token));
        Assert.Equal(resolved, Item(await RecoverSend(uncertainRequest)));
        var read = await lab.Send(
            client,
            DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]),
            token
        );
        Assert.Null(read.Reply.Failure);
        Assert.Equal(resolved.Version, read.Reply.Read!.GetItem(0)!.Version);
        Assert.Equal(new byte[65536], read.Reply.Read.GetItem(0)!.GetValue());
        output.WriteLine(
            $"Healthy majority recovered the acknowledged identity unchanged; original uncertain identity resolved once at {resolved.Version.LogIndex} and retained that version through another restart."
        );

        async Task<DataGridClientOutcome> RecoverSend(DataGridClientRequest request)
        {
            for (var retry = 0; retry < 6; retry++)
            {
                var outcome = await lab.Send(client, request, token);
                output.WriteLine(
                    $"Recovery call {retry + 1}: {outcome.Reply.Failure?.Code.ToString() ?? "Completed"}, admitted={outcome.MayHaveBeenAdmitted}; "
                        + string.Join(
                            "; ",
                            healthy.Select(member =>
                                $"member {member}: {lab.Cluster.Nodes[member]!.Status}, failure={lab.Cluster.Nodes[member]!.Failure}"
                            )
                        )
                );
                if (outcome.Reply.Failure is null)
                    return outcome;
                Assert.Contains(
                    outcome.Reply.Failure.Code,
                    new[]
                    {
                        DataGridClientFailureCode.QuorumUnavailable,
                        DataGridClientFailureCode.OutcomeUnknown,
                    }
                );
                Assert.All(healthy, member => Assert.Null(lab.Cluster.Nodes[member]!.Failure));
                client = lab.Client(0, await lab.Cluster.Leader(token));
            }
            throw new InvalidOperationException(
                "Healthy majority did not recover within six bounded SDK calls."
            );
        }
    }

    private static DataGridMutationResult Item(DataGridClientOutcome outcome)
    {
        Assert.Null(outcome.Reply.Failure);
        Assert.True(outcome.MayHaveBeenAdmitted);
        return outcome.Reply.Result!.GetItem(0);
    }
}
