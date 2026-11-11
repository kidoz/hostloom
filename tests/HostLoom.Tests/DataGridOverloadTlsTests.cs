using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Runtime;
using HostLoom.DataGrid.Storage;
using HostLoom.DataGrid.Transport;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridOverloadTlsTests
{
    [Fact(Timeout = 30000)]
    public async Task TimedOutDisposeRetainsOwnershipUntilActualCleanupThenReleasesItExactlyOnce()
    {
        RequireLinux();
        using var fixture = new DataGridPeerFixture();
        using var capture = new DataGridMeterCapture();
        await using var blocked = new BlockedRuntime(
            fixture,
            TestContext.Current.CancellationToken
        );
        await blocked.Entered.Task.WaitAsync(TestContext.Current.CancellationToken);
        var shutdown = blocked.Runtime.DisposeAsync().AsTask();
        Assert.Same(shutdown, blocked.Runtime.DisposeAsync().AsTask());
        await Assert.ThrowsAsync<TimeoutException>(() => shutdown);
        Assert.False(blocked.Runtime.Completion.IsCompleted);
        Assert.False(blocked.Runtime.Diagnostics.Storage.Closed);
        Assert.Throws<IOException>(() =>
            DataGridFileStore.Open(blocked.Path, fixture.Configuration, 0)
        );
        blocked.Release();
        await blocked.Runtime.Completion.WaitAsync(TestContext.Current.CancellationToken);
        Assert.True(blocked.Runtime.Diagnostics.Local.Stopped);
        Assert.True(blocked.Runtime.Diagnostics.Storage.Closed);
        using var recovered = DataGridFileStore.Open(blocked.Path, fixture.Configuration, 0);
        Assert.Equal(0, recovered.Image.CommitIndex);
        Assert.False(
            (
                await blocked.Runtime.ExecuteAsync(
                    DataGridPeerFixture.Principal,
                    Read(),
                    TestContext.Current.CancellationToken
                )
            )
                .Failure!
                .MayHaveBeenAdmitted
        );
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Theory(Timeout = 20000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FullActorQueueReturnsWireCapacityWithoutClaimingAdmission(bool mutation)
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var fixture = new DataGridPeerFixture();
        using var capture = new DataGridMeterCapture();
        await using var blocked = new BlockedRuntime(fixture, timeout.Token);
        await blocked.Entered.Task.WaitAsync(timeout.Token);
        var waiting = Enumerable
            .Range(0, 128)
            .Select(_ =>
                blocked.Runtime.ReadAsync(
                    DataGridPeerFixture.Map,
                    DataGridPeerFixture.Key,
                    timeout.Token
                )
            )
            .ToArray();
        var policy = Policy(fixture);
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        await using var server = new DataGridClientServer(
            fixture.Credentials(0),
            policy,
            listener,
            blocked.Runtime
        );
        server.Start();
        using var tcp = new TcpClient();
        using var tls = await DataGridTransportAdmissionTlsTests.ClientHello(
            fixture,
            tcp,
            ((IPEndPoint)listener.LocalEndpoint).Port,
            fixture.Certificates[0],
            DataGridPeerFixture.Principal,
            timeout.Token
        );
        var request = mutation
            ? DataGridClientRequest.OpenSession(
                Guid.NewGuid(),
                DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow.AddSeconds(4))
            )
            : Read();
        var reply = await Exchange(
            fixture,
            tls,
            request,
            DataGridPeerFixture.Principal,
            timeout.Token
        );
        Assert.Equal(DataGridClientFailureCode.CapacityExceeded, reply.Failure!.Code);
        Assert.False(reply.Failure.MayHaveBeenAdmitted);
        Assert.Equal(128, capture.Sum("runtime.queue.items", "client"));
        var shutdown = blocked.Runtime.DisposeAsync().AsTask();
        blocked.Release();
        await shutdown;
        foreach (var call in waiting)
            await Record.ExceptionAsync(() => call);
        Assert.Equal(0, capture.Sum("runtime.queue.items", "client"));
        var stopped = await blocked.Runtime.ExecuteAsync(
            DataGridPeerFixture.Principal,
            request,
            timeout.Token
        );
        Assert.Equal(DataGridClientFailureCode.QuorumUnavailable, stopped.Failure!.Code);
        Assert.False(stopped.Failure.MayHaveBeenAdmitted);
        using var recovered = DataGridFileStore.Open(blocked.Path, fixture.Configuration, 0);
        Assert.Equal(0, recovered.RestoreApplication().SessionCount);
        await server.DisposeAsync();
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Theory(Timeout = 20000)]
    [InlineData("wal")]
    [InlineData("directory")]
    [InlineData("reserve")]
    public async Task StorageBudgetRefusalFencesActorWithoutChangingPublishedStateAndReleasesOwner(
        string kind
    )
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var fixture = new DataGridPeerFixture();
        var path = NewDirectory();
        try
        {
            using (DataGridFileStore.Bootstrap(path, fixture.Configuration, 0)) { }
            var wal = Directory.GetFiles(path, "*.wal").Single();
            var original = await File.ReadAllBytesAsync(wal, timeout.Token);
            var used = Directory.GetFiles(path).Sum(file => new FileInfo(file).Length);
            var limits = kind switch
            {
                "wal" => new DataGridStorageLimits(WalBytes: original.Length + 1),
                "directory" => new DataGridStorageLimits(DirectoryBytes: used + 1),
                _ => new DataGridStorageLimits(FreeReserveBytes: long.MaxValue),
            };
            using var store = DataGridFileStore.Open(path, fixture.Configuration, 0, limits);
            await using var runtime = new DataGridPeerRuntime(
                store,
                fixture.Credentials(0),
                fixture.Listeners[0],
                DataGridPeerFixture.Routes,
                1
            );
            await Record.ExceptionAsync(() => runtime.ObserveLeaderAsync(10, timeout.Token));
            await runtime.Completion.WaitAsync(timeout.Token);
            Assert.True(runtime.Diagnostics.Faulted);
            Assert.False(runtime.Diagnostics.LocalReadyHint);
            Assert.True(runtime.Diagnostics.Storage.Closed);
            Assert.Equal(original, await File.ReadAllBytesAsync(wal, timeout.Token));
            using var recovered = DataGridFileStore.Open(path, fixture.Configuration, 0);
            Assert.Equal(0, recovered.Image.CommitIndex);
            Assert.Equal(0, recovered.Image.Term);
        }
        finally
        {
            Directory.Delete(path, recursive: true);
        }
    }

    [Theory(Timeout = 20000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SlowRequestBodiesFillLiveBudgetWithoutStarvingPeerControlAndDrainOnDeadlineOrStop(
        bool shutdown
    )
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        var second = Guid.NewGuid();
        await using var security = new DataGridSecurityContext(
            fixture.Credentials(0),
            Policy(fixture, second)
        );
        var control = new PeerReceiver(fixture.Configuration);
        await using var peer = new DataGridPeerTransport(
            security,
            fixture.Listeners[0],
            DataGridPeerFixture.Routes,
            control
        );
        peer.Start();
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        var application = new ClientReceiver(fixture.Configuration);
        await using var server = new DataGridClientServer(security, listener, application);
        server.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var sockets = new List<TcpClient>();
        var streams = new List<SslStream>();
        using var pacing = new PeriodicTimer(TimeSpan.FromMilliseconds(125)); // configured eight starts/second
        try
        {
            for (var i = 0; i < 12; i++)
            {
                await pacing.WaitForNextTickAsync(timeout.Token);
                var tcp = new TcpClient();
                sockets.Add(tcp);
                streams.Add(
                    await DataGridTransportAdmissionTlsTests.ClientHello(
                        fixture,
                        tcp,
                        port,
                        fixture.Certificates[i % 2 == 0 ? 0 : 2],
                        i % 2 == 0 ? DataGridPeerFixture.Principal : second,
                        timeout.Token
                    )
                );
            }
            await capture.WaitFor(_ => capture.Reserved("request", "client") == 12, timeout.Token);
            Assert.Equal(24L * 1024 * 1024, capture.Sum("transport.reservations.bytes"));
            await pacing.WaitForNextTickAsync(timeout.Token);
            using (var tcp = new TcpClient())
            using (
                var excess = await DataGridTransportAdmissionTlsTests.ClientHello(
                    fixture,
                    tcp,
                    port,
                    fixture.Certificates[0],
                    DataGridPeerFixture.Principal,
                    timeout.Token
                )
            )
                await DataGridTransportAdmissionTlsTests.Closed(excess, timeout.Token);
            Assert.Contains(
                capture.Measurements,
                m =>
                    m.Name == "hostloom.datagrid.transport.admission.rejected"
                    && m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.ResourceTag)
                        is "request"
                    && m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.ReasonTag)
                        is "capacity"
            );
            await using (
                var sender = new DataGridPeerTransport(
                    fixture.Credentials(1),
                    fixture.Listeners[1],
                    DataGridPeerFixture.Routes,
                    new PeerReceiver(fixture.Configuration)
                )
            )
                Assert.True(
                    (
                        await sender.SendAsync(
                            new(
                                RaftMessageKind.VoteRequest,
                                1,
                                0,
                                3,
                                new(1, 1),
                                fixture.Configuration.IncarnationId,
                                fixture.Configuration.ComputeDigest()
                            ),
                            timeout.Token
                        )
                    ).Accepted
                );
            Assert.Equal(1, control.Calls);
            Assert.Equal(0, application.Calls);
            if (shutdown)
                await server.DisposeAsync();
            foreach (var stream in streams)
                await DataGridTransportAdmissionTlsTests.Closed(stream, timeout.Token);
            await capture.WaitFor(_ => capture.Reserved("request", "client") == 0, timeout.Token);
            if (!shutdown)
            {
                using var tcp = new TcpClient();
                using var healthy = await DataGridTransportAdmissionTlsTests.ClientHello(
                    fixture,
                    tcp,
                    port,
                    fixture.Certificates[0],
                    DataGridPeerFixture.Principal,
                    timeout.Token
                );
                Assert.Null(
                    (
                        await Exchange(
                            fixture,
                            healthy,
                            Read(),
                            DataGridPeerFixture.Principal,
                            timeout.Token
                        )
                    ).Failure
                );
            }
        }
        finally
        {
            foreach (var stream in streams)
                await stream.DisposeAsync();
            foreach (var tcp in sockets)
                tcp.Dispose();
        }
        await server.DisposeAsync();
        await peer.DisposeAsync();
        Assert.Null(server.FatalError);
        Assert.Null(peer.FatalError);
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Fact(Timeout = 20000)]
    public async Task NonReadingClientCannotRetainManagedReservationsOrPreventNextLargeResponse()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        var receiver = new ClientReceiver(fixture.Configuration);
        await using var server = new DataGridClientServer(
            fixture.Credentials(0),
            Policy(fixture),
            listener,
            receiver
        );
        server.Start();
        var request = DataGridClientRequest.Read(
            DataGridPeerFixture.Map,
            Enumerable.Range(0, 15).Select(i => DataGridKey.FromUtf8("catalog:eu:" + i)).ToArray()
        );
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        using (var tcp = new TcpClient { ReceiveBufferSize = 1024 })
        using (
            var unread = await DataGridTransportAdmissionTlsTests.ClientHello(
                fixture,
                tcp,
                port,
                fixture.Certificates[0],
                DataGridPeerFixture.Principal,
                timeout.Token
            )
        )
        {
            await DataGridTlsSupport.WriteAsync(
                unread,
                DataGridClientCodec.EncodeRequest(
                    request,
                    fixture.Configuration.IncarnationId,
                    DataGridPeerFixture.Principal,
                    Guid.NewGuid()
                ),
                timeout.Token
            );
            await capture.WaitFor(
                _ => receiver.Calls == 1 && capture.Reserved("connection", "client") == 0,
                timeout.Token
            );
            using var next = new TcpClient();
            using var healthy = await DataGridTransportAdmissionTlsTests.ClientHello(
                fixture,
                next,
                port,
                fixture.Certificates[0],
                DataGridPeerFixture.Principal,
                timeout.Token
            );
            var result = await Exchange(
                fixture,
                healthy,
                request,
                DataGridPeerFixture.Principal,
                timeout.Token
            );
            Assert.Null(result.Failure);
            Assert.Equal(15, result.Read!.Count);
            Assert.Equal(65536, result.Read.GetItem(0)!.GetValue().Length);
        }
        await server.DisposeAsync();
        Assert.Equal(2, receiver.Calls);
        Assert.Null(server.FatalError);
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    private static DataGridClientRequest Read() =>
        DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]);

    private static DataGridClientAccessPolicy Policy(
        DataGridPeerFixture fixture,
        Guid? second = null
    )
    {
        var principals = new List<DataGridClientPrincipal>
        {
            new(
                DataGridPeerFixture.Principal,
                fixture.Fingerprints[0],
                true,
                [new(DataGridPeerFixture.Map, DataGridMapActions.Read | DataGridMapActions.Mutate)]
            ),
        };
        if (second is { } id)
            principals.Add(
                new(
                    id,
                    fixture.Fingerprints[2],
                    true,
                    [new(DataGridPeerFixture.Map, DataGridMapActions.Read)]
                )
            );
        return new(fixture.Configuration, fixture.Root, principals);
    }

    private static async Task<DataGridClientReply> Exchange(
        DataGridPeerFixture fixture,
        SslStream stream,
        DataGridClientRequest request,
        Guid principal,
        CancellationToken token
    )
    {
        var frame = DataGridClientCodec.EncodeRequest(
            request,
            fixture.Configuration.IncarnationId,
            principal,
            Guid.NewGuid()
        );
        await DataGridTlsSupport.WriteAsync(stream, frame, token);
        return DataGridClientCodec.DecodeReply(
            await DataGridTlsSupport.ReadAsync(
                stream,
                fixture.Configuration.IncarnationId,
                fixture.Configuration.GetMember(0).MemberId,
                token
            ),
            request,
            frame.AttemptId,
            fixture.Configuration
        );
    }

    private static void RequireLinux() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "Live TLS 1.3 overload/cleanup evidence requires Linux."
        );

    private static CancellationTokenSource Deadline(CancellationToken token)
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(token);
        source.CancelAfter(TimeSpan.FromSeconds(15));
        return source;
    }

    private static string NewDirectory()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            "hostloom-overload-" + Guid.NewGuid().ToString("N")
        );
        Directory.CreateDirectory(path);
        return path;
    }

    private sealed class BlockedRuntime : IAsyncDisposable
    {
        private readonly ManualResetEventSlim _release = new();
        internal string Path { get; } = NewDirectory();
        internal TaskCompletionSource Entered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal DataGridPeerRuntime Runtime { get; }
        private readonly Task _blocked;

        [SuppressMessage(
            "Reliability",
            "CA2000:Dispose objects before losing scope",
            Justification = "Successful runtime construction owns the file store through Completion; the constructor catch disposes a failed transfer."
        )]
        internal BlockedRuntime(DataGridPeerFixture fixture, CancellationToken token)
        {
            using (DataGridFileStore.Bootstrap(Path, fixture.Configuration, 0)) { }
            var store = DataGridFileStore.Open(
                Path,
                fixture.Configuration,
                0,
                observe: step =>
                {
                    if (step != DataGridStorageStep.WalWritten)
                        return;
                    Entered.TrySetResult();
                    _release.Wait(token);
                }
            );
            try
            {
                Runtime = new(
                    store,
                    fixture.Credentials(0),
                    fixture.Listeners[0],
                    DataGridPeerFixture.Routes,
                    1
                );
            }
            catch
            {
                store.Dispose();
                _release.Dispose();
                Directory.Delete(Path, true);
                throw;
            }
            _blocked = Runtime.ObserveLeaderAsync(10, token);
        }

        internal void Release() => _release.Set();

        public async ValueTask DisposeAsync()
        {
            var stopped = Runtime.DisposeAsync().AsTask();
            Release();
            try
            {
                await stopped;
            }
            catch (TimeoutException) { }
            await Runtime.Completion;
            await Record.ExceptionAsync(() => _blocked);
            _release.Dispose();
            Directory.Delete(Path, true);
        }
    }

    private sealed class ClientReceiver(DataGridBootstrapConfiguration configuration)
        : IDataGridClientReceiver
    {
        private int _calls;
        internal int Calls => Volatile.Read(ref _calls);

        public Task<DataGridClientReply> ExecuteAsync(
            Guid principal,
            DataGridClientRequest request,
            CancellationToken cancellationToken
        )
        {
            Interlocked.Increment(ref _calls);
            return Task.FromResult(
                DataGridClientReply.Completed(
                    DataGridMessageKind.ReadResult,
                    read: new(
                        new(1),
                        request.Keys.Select(
                            (_, index) =>
                                new DataGridLocalEntry(
                                    new byte[65536],
                                    new(configuration.IncarnationId, 1, index),
                                    null
                                )
                        )
                    )
                )
            );
        }
    }

    private sealed class PeerReceiver(DataGridBootstrapConfiguration configuration)
        : IDataGridPeerReceiver
    {
        private int _calls;
        internal int Calls => Volatile.Read(ref _calls);

        public Task<long> ObserveLeaderAsync(long term, CancellationToken cancellationToken) =>
            Task.FromResult(term);

        public Task<RaftMessage?> ReceiveAsync(
            RaftMessage message,
            CancellationToken cancellationToken
        )
        {
            Interlocked.Increment(ref _calls);
            return Task.FromResult<RaftMessage?>(
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
    }
}
