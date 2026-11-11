using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Runtime;
using HostLoom.DataGrid.Storage;
using HostLoom.DataGrid.Transport;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridControlLifetimeTlsTests
{
    [Theory(Timeout = 35000)]
    [InlineData("recover")]
    [InlineData("stop")]
    [InlineData("fault")]
    public async Task PeerDeadlineCannotRefundExecutingControlAndCombinedQueueDrainsAfterOwnerReturns(
        string exit
    )
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var path = Path.Combine(
            Path.GetTempPath(),
            "hostloom-control-" + Guid.NewGuid().ToString("N")
        );
        Directory.CreateDirectory(path);
        try
        {
            using (DataGridFileStore.Bootstrap(path, fixture.Configuration, 1)) { }
            using var store = DataGridFileStore.Open(
                path,
                fixture.Configuration,
                1,
                observe: step =>
                {
                    if (step != DataGridStorageStep.WalWritten)
                        return;
                    entered.TrySetResult();
                    release.Wait(timeout.Token);
                    if (exit == "fault")
                        throw new IOException("Controlled control-work persistence failure.");
                }
            );
            await using (var owner = new RuntimeOwner(store, fixture))
            await using (
                var sender = new DataGridPeerTransport(
                    fixture.Credentials(0),
                    fixture.Listeners[0],
                    DataGridPeerFixture.Routes,
                    new Receiver(fixture.Configuration)
                )
            )
            {
                var runtime = owner.Runtime;
                try
                {
                    using var caller = CancellationTokenSource.CreateLinkedTokenSource(
                        timeout.Token
                    );
                    var message = fixture.Message(RaftMessageKind.VoteRequest, term: 10);
                    var call = sender.SendAsync(message, caller.Token);
                    await entered.Task.WaitAsync(timeout.Token);
                    await caller.CancelAsync();
                    Assert.NotNull(await Record.ExceptionAsync(async () => await call));
                    // Peer physical deadlines remain real; only the actor's background timers are suspended.
                    await capture.WaitFor(
                        _ => capture.Reserved("connection", "peer") == 0,
                        timeout.Token
                    );
                    Assert.Equal(0, capture.Sum("runtime.queue.items", "control"));
                    Assert.Equal(1, capture.Sum("runtime.control.active"));
                    Assert.Equal(message.ByteCount, capture.Sum("runtime.control.bytes"));
                    var queued = Enumerable
                        .Range(0, 255)
                        .Select(_ => runtime.ObserveLeaderAsync(0, timeout.Token))
                        .ToArray();
                    Assert.Equal(255, capture.Sum("runtime.queue.items", "control"));
                    Assert.Equal(256, capture.Sum("runtime.control.active"));
                    Assert.Equal(
                        255 * 128 + message.ByteCount,
                        capture.Sum("runtime.control.bytes")
                    );
                    Assert.NotNull(
                        await Record.ExceptionAsync(() =>
                            runtime.ObserveLeaderAsync(0, timeout.Token)
                        )
                    );
                    var client = runtime.ReadAsync(
                        DataGridPeerFixture.Map,
                        DataGridPeerFixture.Key,
                        timeout.Token
                    );
                    Assert.Equal(1, capture.Sum("runtime.admission.active"));
                    Task? stopped = null;
                    if (exit == "stop")
                    {
                        stopped = runtime.DisposeAsync().AsTask();
                        Assert.Same(stopped, runtime.DisposeAsync().AsTask());
                        await Assert.ThrowsAsync<TimeoutException>(() => stopped);
                        Assert.False(runtime.Completion.IsCompleted);
                        Assert.Equal(256, capture.Sum("runtime.control.active"));
                        Assert.Equal(1, capture.Sum("runtime.admission.active"));
                        Assert.Throws<IOException>(() =>
                            DataGridFileStore.Open(path, fixture.Configuration, 1)
                        );
                    }
                    release.Set();
                    if (exit == "recover")
                    {
                        Assert.All(await Task.WhenAll(queued), term => Assert.Equal(10, term));
                        Assert.Equal(RaftCompletionKind.NotLeader, (await client).Kind);
                        await runtime.DisposeAsync();
                        Assert.Null(runtime.Failure);
                    }
                    else
                    {
                        await runtime.Completion.WaitAsync(timeout.Token);
                        foreach (var task in queued)
                            Assert.NotNull(await Record.ExceptionAsync(() => task));
                        Assert.NotNull(await Record.ExceptionAsync(async () => await client));
                        if (exit == "fault")
                            Assert.NotNull(runtime.Failure);
                    }
                    Assert.Equal(0, capture.Sum("runtime.control.active"));
                    Assert.Equal(0, capture.Sum("runtime.control.bytes"));
                    Assert.Equal(0, capture.Sum("runtime.queue.items", "control"));
                    Assert.Equal(0, capture.Sum("runtime.admission.active"));
                    Assert.True(runtime.Diagnostics.Storage.Closed);
                    using var recovered = DataGridFileStore.Open(path, fixture.Configuration, 1);
                    Assert.Equal(0, recovered.Image.CommitIndex);
                }
                finally
                {
                    release.Set();
                }
            }
        }
        finally
        {
            Directory.Delete(path, true);
        }
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Theory(Timeout = 20000)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task TransportDisposalJoinsOutgoingTlsAndReleasesGatesBeforeReturning(
        bool shared,
        bool callerCancellation
    )
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        await using var security = new DataGridSecurityContext(fixture.Credentials(0));
        await using var sender = shared
            ? new DataGridPeerTransport(
                security,
                fixture.Listeners[0],
                DataGridPeerFixture.Routes,
                new Receiver(fixture.Configuration)
            )
            : new DataGridPeerTransport(
                fixture.Credentials(0),
                fixture.Listeners[0],
                DataGridPeerFixture.Routes,
                new Receiver(fixture.Configuration)
            );
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
        var calls = Enumerable
            .Range(1, 2)
            .Select(peer =>
                sender.SendAsync(
                    new(
                        RaftMessageKind.VoteRequest,
                        0,
                        peer,
                        1,
                        new(1, peer),
                        fixture.Configuration.IncarnationId,
                        fixture.Configuration.ComputeDigest()
                    ),
                    caller.Token
                )
            )
            .ToArray();
        // Owned raw listeners accept TCP but never speak TLS.
        await capture.WaitFor(
            _ => capture.Reserved("handshake", "peer", "outbound") == 2,
            timeout.Token
        );
        if (callerCancellation)
            await caller.CancelAsync();
        var stopped = sender.DisposeAsync().AsTask();
        Assert.Same(stopped, sender.DisposeAsync().AsTask());
        await stopped;
        Assert.All(calls, call => Assert.True(call.IsCompleted));
        foreach (var call in calls)
        {
            var failure = await Record.ExceptionAsync(async () => await call);
            Assert.NotNull(failure);
            Assert.True(
                failure is IOException or OperationCanceledException,
                failure.GetType().Name
            );
        }
        var late = await Record.ExceptionAsync(async () =>
            await sender.SendAsync(fixture.Message(RaftMessageKind.VoteRequest), timeout.Token)
        );
        Assert.IsType<IOException>(late);
        using var material = security.Acquire();
        Assert.Equal(1, security.Generation);
        Assert.Null(sender.FatalError);
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Fact(Timeout = 20000)]
    public async Task SharedTransportStopJoinsSnapshotSenderWithoutClaimingRemoteRollback()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        var receiver = new PendingSnapshotReceiver();
        await using var server = new DataGridPeerTransport(
            fixture.Credentials(1),
            fixture.Listeners[1],
            DataGridPeerFixture.Routes,
            receiver
        );
        server.Start();
        await using var security = new DataGridSecurityContext(fixture.Credentials(0));
        await using var sender = new DataGridPeerTransport(
            security,
            fixture.Listeners[0],
            DataGridPeerFixture.Routes,
            new Receiver(fixture.Configuration)
        );
        var machine = new DataGridLocalStateMachine(fixture.Configuration);
        machine.Apply(1, DataGridCommittedCommand.AdvanceTime(new(1)));
        var snapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);
        var call = sender.SendAsync(
            new(
                RaftMessageKind.SnapshotRequest,
                0,
                1,
                3,
                new(1, 1),
                fixture.Configuration.IncarnationId,
                fixture.Configuration.ComputeDigest(),
                snapshot: snapshot
            ),
            timeout.Token
        );
        try
        {
            await receiver.Entered.Task.WaitAsync(timeout.Token);
            Assert.True(server.TransferActive);
            Assert.False(call.IsCompleted);
            await sender.DisposeAsync();
            Assert.True(call.IsCompleted);
            Assert.NotNull(await Record.ExceptionAsync(async () => await call));
            Assert.False(receiver.Stopped.Task.IsCompleted);
            Assert.True(server.TransferActive); // Local cancellation is not remote rollback.
        }
        finally
        {
            await server.DisposeAsync();
            await Record.ExceptionAsync(async () => await call);
        }
        await receiver.Stopped.Task.WaitAsync(timeout.Token);
        Assert.False(server.TransferActive);
        Assert.Null(sender.FatalError);
        Assert.Null(server.FatalError);
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    private static void RequireLinux() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "Live TLS 1.3 control ownership evidence requires Linux."
        );

    private static CancellationTokenSource Deadline(CancellationToken token)
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(token);
        source.CancelAfter(TimeSpan.FromSeconds(30));
        return source;
    }

    private sealed class RuntimeOwner(DataGridFileStore store, DataGridPeerFixture fixture)
        : IAsyncDisposable
    {
        internal DataGridPeerRuntime Runtime { get; } =
            new(
                store,
                fixture.Credentials(1),
                fixture.Listeners[1],
                DataGridPeerFixture.Routes,
                1,
                new SuspendedClock()
            );

        public async ValueTask DisposeAsync()
        {
            try
            {
                await Runtime.DisposeAsync();
            }
            catch (TimeoutException)
            {
                await Runtime.Completion;
            } // Join cleanup after the observed cached wait timeout.
        }
    }

    // Isolate the retained control boundary from election/heartbeat producers during the blocked WAL.
    private sealed class SuspendedClock : TimeProvider
    {
        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period
        ) => new SuspendedTimer();

        private sealed class SuspendedTimer : ITimer
        {
            private int _disposed;

            public bool Change(TimeSpan dueTime, TimeSpan period) =>
                Volatile.Read(ref _disposed) == 0;

            public void Dispose() => Interlocked.Exchange(ref _disposed, 1);

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }
        }
    }

    private sealed class Receiver(DataGridBootstrapConfiguration configuration)
        : IDataGridPeerReceiver
    {
        public Task<long> ObserveLeaderAsync(long term, CancellationToken cancellationToken) =>
            Task.FromResult(term);

        public Task<RaftMessage?> ReceiveAsync(
            RaftMessage message,
            CancellationToken cancellationToken
        ) =>
            Task.FromResult<RaftMessage?>(
                new(
                    RaftMessageKind.VoteResponse,
                    message.To,
                    message.From,
                    message.Term,
                    message.Token,
                    configuration.IncarnationId,
                    configuration.ComputeDigest(),
                    accepted: true
                )
            );
    }

    private sealed class PendingSnapshotReceiver : IDataGridPeerReceiver
    {
        internal TaskCompletionSource Entered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Stopped { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<long> ObserveLeaderAsync(long term, CancellationToken cancellationToken) =>
            Task.FromResult(term);

        public async Task<RaftMessage?> ReceiveAsync(
            RaftMessage message,
            CancellationToken cancellationToken
        )
        {
            Entered.TrySetResult();
            try
            {
                await new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously
                ).Task.WaitAsync(cancellationToken);
            }
            finally
            {
                Stopped.TrySetResult();
            }
            return null;
        }
    }
}
