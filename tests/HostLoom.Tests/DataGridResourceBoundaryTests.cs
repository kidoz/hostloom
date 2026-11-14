using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using HostLoom.DataGrid.Transport;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridResourceBoundaryTests
{
    [Fact]
    public void MaximumRetainedVectorFitsItsChargeAndRoundTripsThroughWireAndSnapshot()
    {
        using var fixture = new DataGridPeerFixture();
        var machine = new DataGridLocalStateMachine(fixture.Configuration);
        var opened = machine.Apply(
            1,
            DataGridCommittedCommand.OpenSession(
                DataGridPeerFixture.Principal,
                Guid.NewGuid(),
                new(TimeSpan.TicksPerSecond * 4),
                default
            )
        );
        var command = new DataGridMutationCommand(
            DataGridMutationKind.Put,
            DataGridPeerFixture.Map,
            opened.SessionNumber,
            1,
            DataGridExpiryKind.Never,
            0,
            Enumerable
                .Range(0, DataGridMutationCommand.MaxItemCount)
                .Select(i => new DataGridMutationItem(
                    DataGridKey.FromUtf8(FormattableString.Invariant($"catalog:vector:{i}")),
                    [(byte)i]
                ))
                .ToArray()
        );
        var result = machine.Apply(
            2,
            DataGridCommittedCommand.Mutate(DataGridPeerFixture.Principal, command, default)
        );
        var charge = 2 + 31 * DataGridMutationCommand.MaxItemCount;
        Assert.Equal(3970, charge);
        Assert.Equal(charge, machine.RetainedResultByteCount);
        Assert.Equal(16261120, (long)charge * DataGridLocalStateMachine.MaxSessionCount);
        Assert.True(
            (long)charge * DataGridLocalStateMachine.MaxSessionCount
                < DataGridLocalStateMachine.MaxResultBytes
        );
        var request = DataGridClientRequest.Mutate(command);
        var frame = DataGridClientCodec.EncodeReply(
            request,
            DataGridClientReply.Completed(DataGridMessageKind.MutationResult, result),
            fixture.Configuration.IncarnationId,
            fixture.Configuration.GetMember(0).MemberId,
            Guid.NewGuid()
        );
        Assert.Equal(charge, frame.GetPayload().Length);
        var decoded = DataGridClientCodec
            .DecodeReply(frame, request, frame.AttemptId, fixture.Configuration)
            .Result!;
        AssertVector(decoded);
        frame.GetPayload()[^1] ^= 1;
        AssertVector(result);
        var snapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);
        var restored = DataGridLocalStateMachine.Restore(
            fixture.Configuration,
            snapshot.Descriptor,
            snapshot.GetContent()
        );
        Assert.Equal(charge, restored.RetainedResultByteCount);
        AssertVector(
            restored.Apply(
                3,
                DataGridCommittedCommand.Mutate(DataGridPeerFixture.Principal, command, default)
            )
        );
        Assert.Equal(128, restored.StoredKeyCount);
        Assert.Equal(charge, restored.RetainedResultByteCount);
        var changed = new DataGridMutationCommand(
            command.Kind,
            command.MapId,
            command.SessionNumber,
            command.RequestNumber,
            command.ExpiryKind,
            command.ExpiryTicks,
            Enumerable
                .Range(0, command.ItemCount)
                .Select(i => new DataGridMutationItem(command.GetItem(i).Key, [255]))
                .ToArray()
        );
        Assert.Equal(
            DataGridApplyStatus.RequestIdentityMismatch,
            restored
                .Apply(
                    4,
                    DataGridCommittedCommand.Mutate(DataGridPeerFixture.Principal, changed, default)
                )
                .Status
        );
        Assert.Equal(charge, restored.RetainedResultByteCount);
        for (var i = 0; i < command.ItemCount; i++)
        {
            var entry = restored.Read(command.MapId, command.GetItem(i).Key)!;
            Assert.Equal(new byte[] { (byte)i }, entry.GetValue());
            Assert.Equal(result.GetItem(i).Version, entry.Version);
        }

        void AssertVector(DataGridApplyResult actual)
        {
            Assert.Equal(DataGridApplyStatus.Completed, actual.Status);
            Assert.Equal(128, actual.ItemCount);
            for (var i = 0; i < 128; i++)
            {
                Assert.Equal(DataGridMutationStatus.Stored, actual.GetItem(i).Status);
                Assert.Equal(
                    new DataGridEntryVersion(fixture.Configuration.IncarnationId, 2, i),
                    actual.GetItem(i).Version
                );
            }
        }
    }

    [Fact]
    public void RecorderOperationLimitRefusesWithoutDroppingEarlierObservations()
    {
        var history = new DataGridClientHistory();
        var request = DataGridClientRequest.Read(
            DataGridPeerFixture.Map,
            [DataGridPeerFixture.Key]
        );
        for (var i = 0; i < 8192; i++)
            history.Begin(DataGridPeerFixture.Cluster, DataGridPeerFixture.Principal, request);
        var before = history.GetOperations();
        Assert.Throws<InvalidOperationException>(() =>
            history.Begin(DataGridPeerFixture.Cluster, DataGridPeerFixture.Principal, request)
        );
        Assert.Equal(before, history.GetOperations());
        Assert.Equal(8192, before.Count);
        Assert.Equal(
            DataGridHistoryStatus.Inconclusive,
            DataGridClientHistoryChecker.Check(before).Status
        );
    }

    [Fact]
    public void RecorderResponseChargeLimitLeavesTheUnrecordedCompletionPending()
    {
        using var fixture = new DataGridPeerFixture();
        var history = new DataGridClientHistory();
        var keys = Enumerable
            .Range(0, 15)
            .Select(i => DataGridKey.FromUtf8(FormattableString.Invariant($"catalog:history:{i}")))
            .ToArray();
        var request = DataGridClientRequest.Read(DataGridPeerFixture.Map, keys);
        var reply = DataGridClientReply.Completed(
            DataGridMessageKind.ReadResult,
            read: new(
                new(42),
                keys.Select(
                        (_, i) =>
                            new DataGridLocalEntry(
                                new byte[64 * 1024],
                                new(DataGridPeerFixture.Cluster, 2, i),
                                null
                            )
                    )
                    .ToArray()
            )
        );
        var frame = DataGridClientCodec.EncodeReply(
            request,
            reply,
            DataGridPeerFixture.Cluster,
            fixture.Configuration.GetMember(0).MemberId,
            Guid.NewGuid()
        );
        var outcome = new DataGridClientOutcome(
            DataGridClientCodec.DecodeReply(frame, request, frame.AttemptId, fixture.Configuration),
            true,
            []
        );
        for (var completed = 0; completed < 128; completed++)
        {
            var id = history.Begin(
                DataGridPeerFixture.Cluster,
                DataGridPeerFixture.Principal,
                request
            );
            if (completed < 68)
            {
                history.Complete(id, outcome);
                continue;
            }
            var before = history.GetOperations();
            Assert.Throws<InvalidOperationException>(() => history.Complete(id, outcome));
            Assert.Equal(69, before.Count);
            Assert.Equal(68, before.Count(operation => operation.Outcome is not null));
            Assert.Null(before[^1].CompletionOrder);
            Assert.Equal(before, history.GetOperations());
            Assert.Throws<InvalidOperationException>(() => history.Complete(id, outcome));
            Assert.Null(history.GetOperations()[^1].Outcome);
            return;
        }
        Assert.Fail("The charged response budget did not refuse the next completion.");
    }

    [Fact(Timeout = 20000)]
    public async Task SharedCredentialCaptureLimitSpansCurrentAndRetiringGenerations()
    {
        using var fixture = new DataGridPeerFixture();
        await using var security = new DataGridSecurityContext(fixture.Credentials(0));
        var leases = Enumerable.Range(0, 128).Select(_ => security.Acquire()).ToArray();
        DataGridCredentialReload? reload = null;
        try
        {
            Assert.Throws<IOException>(() => security.Acquire());
            reload = security.Reload(fixture.Credentials(0));
            Assert.False(reload.Retired.IsCompleted);
            Assert.Throws<IOException>(() => security.Acquire());
            leases[0].Dispose();
            leases[0].Dispose();
            using var current = security.Acquire();
            Assert.Equal(reload.Generation, security.Generation);
            Assert.Throws<IOException>(() => security.Acquire());
            Assert.Throws<IOException>(() => security.Reload(fixture.Credentials(0)));
            Assert.False(reload.Retired.IsCompleted);
        }
        finally
        {
            foreach (var lease in leases)
                lease.Dispose();
        }
        Assert.NotNull(reload);
        await reload.Retired.WaitAsync(TestContext.Current.CancellationToken);
        using var reused = security.Acquire();
        Assert.Equal(reload.Generation, security.Generation);
    }
}
