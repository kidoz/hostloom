using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Runtime;
using HostLoom.DataGrid.Storage;
using HostLoom.DataGrid.Transport;
using Xunit;

namespace HostLoom.Tests;

[SuppressMessage(
    "Security",
    "CA5398:Avoid hardcoded SslProtocols values",
    Justification = "Tests assert the exact TLS 1.3 contract and rejection of older protocols."
)]
public sealed class DataGridPeerTlsTests
{
    [Fact(Timeout = 20000)]
    public async Task MutualTlsAndMemberHelloPrecedeVoteDispatch()
    {
        RequireLinux();
        using var fixture = new DataGridPeerFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        var receiver = new EchoReceiver(fixture.Configuration);
        await using var server = new DataGridPeerTransport(
            fixture.Credentials(1),
            fixture.Listeners[1],
            DataGridPeerFixture.Routes,
            receiver
        );
        server.Start();
        await using var client = new DataGridPeerTransport(
            fixture.Credentials(0),
            fixture.Listeners[0],
            DataGridPeerFixture.Routes,
            new EchoReceiver(fixture.Configuration)
        );
        var response = await client.SendAsync(
            fixture.Message(RaftMessageKind.VoteRequest, term: 3),
            timeout.Token
        );
        Assert.True(response.Accepted);
        Assert.Equal(3, response.Term);
        Assert.Equal(1, response.From);
        Assert.Equal(1, receiver.Calls);
    }

    [Fact]
    public void ListenerStartupFailureReleasesTransferredStorageOwnership()
    {
        RequireLinux();
        using var fixture = new DataGridPeerFixture();
        var path = Path.Combine(
            Path.GetTempPath(),
            "hostloom-peer-start-" + Guid.NewGuid().ToString("N")
        );
        Directory.CreateDirectory(path);
        using var listener = new TcpListener(
            DataGridPeerFixture.Routes[0],
            fixture.Configuration.GetMember(0).Port
        );
        try
        {
            using var store = DataGridFileStore.Bootstrap(path, fixture.Configuration, 0);
            Assert.Throws<SocketException>(() =>
                new DataGridPeerRuntime(
                    store,
                    fixture.Credentials(0),
                    listener,
                    DataGridPeerFixture.Routes,
                    1
                )
            );
            using var reopened = DataGridFileStore.Open(path, fixture.Configuration, 0);
            Assert.Equal(0, reopened.Image.CommitIndex);
        }
        finally
        {
            listener.Stop();
            Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void ActorCannotUseAnotherMembersStoreAndFailureReleasesOwnership()
    {
        RequireLinux();
        using var fixture = new DataGridPeerFixture();
        var path = Path.Combine(
            Path.GetTempPath(),
            "hostloom-peer-binding-" + Guid.NewGuid().ToString("N")
        );
        Directory.CreateDirectory(path);
        try
        {
            using var store = DataGridFileStore.Bootstrap(path, fixture.Configuration, 1);
            Assert.Throws<InvalidDataException>(() =>
                new DataGridPeerRuntime(
                    store,
                    fixture.Credentials(0),
                    fixture.Listeners[0],
                    DataGridPeerFixture.Routes,
                    1
                )
            );
            using var reopened = DataGridFileStore.Open(path, fixture.Configuration, 1);
            Assert.Equal(0, reopened.Image.CommitIndex);
        }
        finally
        {
            Directory.Delete(path, recursive: true);
        }
    }

    [Theory(Timeout = 20000)]
    [InlineData("build")]
    [InlineData("unbound")]
    [InlineData("denied")]
    [InlineData("root")]
    public async Task IncompatibleOrUntrustedMemberCannotDispatch(string reason)
    {
        RequireLinux();
        using var fixture = new DataGridPeerFixture();
        using var foreign = new DataGridPeerFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var rogue = fixture.Issue("node-a.example");
        var receiver = new EchoReceiver(fixture.Configuration);
        var serverPolicy = fixture.Credentials(
            1,
            denied: reason == "denied" ? [Convert.ToHexString(fixture.Fingerprints[0])] : null
        );
        var fingerprints = fixture.Fingerprints.Select(bytes => (byte[])bytes.Clone()).ToArray();
        if (reason == "unbound")
            fingerprints[0] = rogue.GetCertHash(HashAlgorithmName.SHA256);
        var clientPolicy = fixture.Credentials(
            0,
            build: reason == "build" ? new byte[32] : null,
            certificate: reason == "unbound" ? rogue : null,
            root: reason == "root" ? foreign.Root : null,
            fingerprints: fingerprints
        );
        await using var server = new DataGridPeerTransport(
            serverPolicy,
            fixture.Listeners[1],
            DataGridPeerFixture.Routes,
            receiver
        );
        server.Start();
        if (reason == "root")
        {
            Assert.Throws<AuthenticationException>(() =>
                new DataGridPeerTransport(
                    clientPolicy,
                    fixture.Listeners[0],
                    DataGridPeerFixture.Routes,
                    new EchoReceiver(fixture.Configuration)
                )
            );
            Assert.Equal(0, receiver.Calls);
            return;
        }
        await using var client = new DataGridPeerTransport(
            clientPolicy,
            fixture.Listeners[0],
            DataGridPeerFixture.Routes,
            new EchoReceiver(fixture.Configuration)
        );
        var exception = await Record.ExceptionAsync(() =>
            client.SendAsync(fixture.Message(RaftMessageKind.VoteRequest), timeout.Token)
        );
        Assert.True(
            exception is IOException or AuthenticationException,
            $"Unexpected outcome: {exception}"
        );
        Assert.Equal(0, receiver.Calls);
    }

    [Theory(Timeout = 20000)]
    [InlineData("expired")]
    [InlineData("dns")]
    [InlineData("usage")]
    [InlineData("pin")]
    public async Task InvalidServerCertificateIsRejectedBeforeHello(string reason)
    {
        RequireLinux();
        using var fixture = new DataGridPeerFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var invalid = fixture.Issue(
            reason == "dns" ? "foreign.example" : "node-b.example",
            expired: reason == "expired",
            clientOnly: reason == "usage"
        );
        var fingerprints = fixture.Fingerprints.Select(bytes => (byte[])bytes.Clone()).ToArray();
        if (reason != "pin")
            fingerprints[1] = invalid.GetCertHash(HashAlgorithmName.SHA256);
        var raw = Task.Run(
            async () =>
            {
                using var peer = await fixture.Listeners[1].AcceptTcpClientAsync(timeout.Token);
                using var tls = new SslStream(peer.GetStream());
                var options = fixture.Credentials(1).ServerOptions();
                options.ServerCertificate = invalid;
                try
                {
                    await tls.AuthenticateAsServerAsync(options, timeout.Token);
                    var buffer = new byte[1];
                    if (await tls.ReadAsync(buffer, timeout.Token) != 0)
                        throw new InvalidDataException(
                            "Rejected peer unexpectedly sent application bytes."
                        );
                }
                catch (Exception exception)
                    when (exception is IOException or AuthenticationException) { }
            },
            timeout.Token
        );
        await using var client = new DataGridPeerTransport(
            fixture.Credentials(0, fingerprints: fingerprints),
            fixture.Listeners[0],
            DataGridPeerFixture.Routes,
            new EchoReceiver(fixture.Configuration)
        );
        var exception = await Record.ExceptionAsync(() =>
            client.SendAsync(fixture.Message(RaftMessageKind.VoteRequest), timeout.Token)
        );
        Assert.True(
            exception is IOException or AuthenticationException,
            $"Unexpected outcome: {exception}"
        );
        await raw;
    }

    [Theory(Timeout = 20000)]
    [InlineData("length")]
    [InlineData("sender")]
    [InlineData("cluster")]
    [InlineData("opcode")]
    [InlineData("before-hello")]
    public async Task AuthenticatedMalformedFramesAreClosedBeforeRaftDispatch(string reason)
    {
        RequireLinux();
        using var fixture = new DataGridPeerFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        var receiver = new EchoReceiver(fixture.Configuration);
        await using var server = new DataGridPeerTransport(
            fixture.Credentials(1),
            fixture.Listeners[1],
            DataGridPeerFixture.Routes,
            receiver
        );
        server.Start();
        using var tcp = new TcpClient();
        await tcp.ConnectAsync(
            DataGridPeerFixture.Routes[1],
            fixture.Configuration.GetMember(1).Port,
            timeout.Token
        );
        using var tls = new SslStream(tcp.GetStream());
        await tls.AuthenticateAsClientAsync(fixture.Credentials(0).ClientOptions(1), timeout.Token);
        if (reason != "before-hello")
            await Hello(tls, fixture, timeout.Token);
        var bytes = DataGridPeerCodec
            .Encode(fixture.Message(RaftMessageKind.VoteRequest), fixture.Configuration)
            .Encode();
        if (reason == "length")
        {
            bytes = new byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(bytes, uint.MaxValue);
        }
        if (reason == "sender")
            bytes[24] ^= 1;
        if (reason == "cluster")
            bytes[8] ^= 1;
        if (reason == "opcode")
            BinaryPrimitives.WriteUInt16BigEndian(
                bytes.AsSpan(6),
                (ushort)DataGridMessageKind.Mutation
            );
        await tls.WriteAsync(bytes, timeout.Token);
        var closed = false;
        try
        {
            closed = await tls.ReadAsync(new byte[1], timeout.Token) == 0;
        }
        catch (IOException)
        {
            closed = true;
        }
        Assert.True(closed);
        Assert.Equal(0, receiver.Calls);
    }

    [Theory(Timeout = 20000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MissingClientCertificateOrTls12CannotJoin(bool tls12)
    {
        RequireLinux();
        using var fixture = new DataGridPeerFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        var receiver = new EchoReceiver(fixture.Configuration);
        await using var server = new DataGridPeerTransport(
            fixture.Credentials(1),
            fixture.Listeners[1],
            DataGridPeerFixture.Routes,
            receiver
        );
        server.Start();
        using var tcp = new TcpClient();
        await tcp.ConnectAsync(
            DataGridPeerFixture.Routes[1],
            fixture.Configuration.GetMember(1).Port,
            timeout.Token
        );
        using var tls = new SslStream(tcp.GetStream());
        var options = fixture.Credentials(0).ClientOptions(1);
        if (tls12)
            options.EnabledSslProtocols = SslProtocols.Tls12;
        else
            options.ClientCertificates = null;
        var exception = await Record.ExceptionAsync(async () =>
        {
            await tls.AuthenticateAsClientAsync(options, timeout.Token);
            await Hello(tls, fixture, timeout.Token);
        });
        Assert.True(
            exception is IOException or AuthenticationException,
            $"Unexpected outcome: {exception}"
        );
        Assert.Equal(0, receiver.Calls);
    }

    [Fact(Timeout = 20000)]
    public async Task SnapshotInstalledReplyWaitsForReceiverCompletion()
    {
        RequireLinux();
        using var fixture = new DataGridPeerFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var receiver = new EchoReceiver(
            fixture.Configuration,
            async () =>
            {
                entered.TrySetResult();
                await release.Task.WaitAsync(timeout.Token);
            }
        );
        await using var server = new DataGridPeerTransport(
            fixture.Credentials(1),
            fixture.Listeners[1],
            DataGridPeerFixture.Routes,
            receiver
        );
        server.Start();
        await using var client = new DataGridPeerTransport(
            fixture.Credentials(0),
            fixture.Listeners[0],
            DataGridPeerFixture.Routes,
            new EchoReceiver(fixture.Configuration)
        );
        var application = new DataGridLocalStateMachine(fixture.Configuration);
        application.Apply(1, DataGridCommittedCommand.AdvanceTime(default));
        var snapshot = application.SaveSnapshot(Guid.NewGuid(), 1);
        var request = new RaftMessage(
            RaftMessageKind.SnapshotRequest,
            0,
            1,
            3,
            new(1, 1),
            DataGridPeerFixture.Cluster,
            fixture.Configuration.ComputeDigest(),
            snapshot: snapshot
        );
        var call = client.SendAsync(request, timeout.Token);
        await entered.Task.WaitAsync(timeout.Token);
        Assert.False(call.IsCompleted);
        release.TrySetResult();
        var response = await call;
        Assert.True(response.Accepted);
        Assert.Equal(1, response.Index);
    }

    [Fact(Timeout = 20000)]
    public async Task AlreadyCommittedSnapshotConfirmsCoverageWithoutReplacingNewerState()
    {
        RequireLinux();
        using var fixture = new DataGridPeerFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        var path = Path.Combine(
            Path.GetTempPath(),
            "hostloom-peer-covered-" + Guid.NewGuid().ToString("N")
        );
        Directory.CreateDirectory(path);
        try
        {
            using var store = DataGridFileStore.Bootstrap(path, fixture.Configuration, 1);
            var command = DataGridCommittedCommand.AdvanceTime(default);
            store.Persist(
                new(
                    fixture.Configuration,
                    2,
                    0,
                    [
                        new(1, 1, DataGridCommittedCommandCodec.Encode(command)),
                        new(2, 2, DataGridCommittedCommandCodec.Encode(command)),
                    ],
                    2
                ),
                timeout.Token
            );
            var generation = store.GenerationId;
            await using var runtime = new DataGridPeerRuntime(
                store,
                fixture.Credentials(1),
                fixture.Listeners[1],
                DataGridPeerFixture.Routes,
                1
            );
            await using var client = new DataGridPeerTransport(
                fixture.Credentials(0),
                fixture.Listeners[0],
                DataGridPeerFixture.Routes,
                new EchoReceiver(fixture.Configuration)
            );
            var state = new DataGridLocalStateMachine(fixture.Configuration);
            state.Apply(1, command);
            var snapshot = state.SaveSnapshot(Guid.NewGuid(), 1);
            var response = await client.SendAsync(
                new(
                    RaftMessageKind.SnapshotRequest,
                    0,
                    1,
                    1000,
                    new(1, 1),
                    DataGridPeerFixture.Cluster,
                    fixture.Configuration.ComputeDigest(),
                    snapshot: snapshot
                ),
                timeout.Token
            );
            Assert.True(response.Accepted);
            Assert.Equal(1, response.Index);
            Assert.Equal(2, runtime.Status.CommitIndex);
            Assert.Equal(0, runtime.Status.SnapshotIndex);
            Assert.Empty(Directory.GetFiles(path, "snapshot-*.bin"));
            await runtime.DisposeAsync();
            using var recovered = DataGridFileStore.Open(path, fixture.Configuration, 1);
            Assert.Equal(generation, recovered.GenerationId);
            Assert.Equal(2, recovered.Image.CommitIndex);
        }
        finally
        {
            Directory.Delete(path, recursive: true);
        }
    }

    [Fact(Timeout = 20000)]
    public async Task ExcessUnauthenticatedConnectionsStayBoundedAndShutdownDrainsThem()
    {
        RequireLinux();
        using var fixture = new DataGridPeerFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        var receiver = new EchoReceiver(fixture.Configuration);
        await using var server = new DataGridPeerTransport(
            fixture.Credentials(1),
            fixture.Listeners[1],
            DataGridPeerFixture.Routes,
            receiver
        );
        server.Start();
        var sockets = new List<TcpClient>();
        try
        {
            for (var i = 0; i < 17; i++)
            {
                var client = new TcpClient();
                sockets.Add(client);
                await client.ConnectAsync(
                    DataGridPeerFixture.Routes[1],
                    fixture.Configuration.GetMember(1).Port,
                    timeout.Token
                );
            }
            Assert.Equal(0, await sockets[^1].GetStream().ReadAsync(new byte[1], timeout.Token));
            Assert.InRange(server.ActiveConnections, 0, 16);
            Assert.True(server.RejectedConnections >= 1);
            Assert.Equal(0, receiver.Calls);
        }
        finally
        {
            foreach (var socket in sockets)
                socket.Dispose();
        }
        await server.DisposeAsync();
    }

    [Fact(Timeout = 20000)]
    public async Task ModifiedEncryptedRecordIsRejectedBeforeApplicationDispatch()
    {
        RequireLinux();
        using var fixture = new DataGridPeerFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        var receiver = new EchoReceiver(fixture.Configuration);
        await using var server = new DataGridPeerTransport(
            fixture.Credentials(1),
            fixture.Listeners[1],
            DataGridPeerFixture.Routes,
            receiver
        );
        server.Start();
        using var tcp = new TcpClient();
        await tcp.ConnectAsync(
            DataGridPeerFixture.Routes[1],
            fixture.Configuration.GetMember(1).Port,
            timeout.Token
        );
        using var altered = new AlteredRecordStream(tcp.GetStream());
        using var tls = new SslStream(altered);
        await tls.AuthenticateAsClientAsync(fixture.Credentials(0).ClientOptions(1), timeout.Token);
        altered.Armed = true;
        var failure = await Record.ExceptionAsync(() => Hello(tls, fixture, timeout.Token));
        Assert.True(
            failure is IOException or AuthenticationException,
            $"Unexpected outcome: {failure}"
        );
        Assert.Equal(0, receiver.Calls);
    }

    [Fact(Timeout = 35000)]
    public async Task MinorityCannotAcknowledgeAndUnknownWriteCanCommitAfterQuorumReturns()
    {
        RequireLinux();
        using var fixture = new DataGridPeerFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken, 30);
        await using var cluster = new LiveCluster(fixture);
        cluster.StartAll();
        var leader = await cluster.Leader(timeout.Token);
        var node = cluster.Nodes[leader]!;
        var open = await node.ProposeAsync(
            DataGridCommittedCommand.OpenSession(
                DataGridPeerFixture.Principal,
                DataGridPeerFixture.Nonce,
                DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow.AddSeconds(5)),
                DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow)
            ),
            timeout.Token
        );
        Assert.Equal(RaftCompletionKind.Applied, open.Kind);
        var peers = Enumerable.Range(0, 3).Where(i => i != leader).ToArray();
        foreach (var peer in peers)
            await cluster.Nodes[peer]!.WaitForAppliedAsync(open.Index, timeout.Token);
        foreach (var peer in peers)
            await cluster.Stop(peer);
        var command = Mutation(open.Result!.SessionNumber, 1, [8]);
        var mutation = node.ProposeAsync(command, timeout.Token);
        var read = node.ReadAsync(DataGridPeerFixture.Map, DataGridPeerFixture.Key, timeout.Token);
        Assert.Equal(RaftCompletionKind.OutcomeUnknown, (await mutation).Kind);
        Assert.Equal(RaftCompletionKind.Unavailable, (await read).Kind);
        cluster.Start(peers[0]);
        var committedRead = await node.ReadAsync(
            DataGridPeerFixture.Map,
            DataGridPeerFixture.Key,
            timeout.Token
        );
        Assert.Equal(RaftCompletionKind.Read, committedRead.Kind);
        Assert.Equal(new byte[] { 8 }, committedRead.Entry!.GetValue());
        var retry = await node.ProposeAsync(command, timeout.Token);
        Assert.Equal(committedRead.Entry.Version, retry.Result!.GetItem(0).Version);
        Assert.Null(node.Failure);
    }

    [Fact(Timeout = 45000)]
    public async Task LiveFileBackedPeersElectReplicateReadFailOverAndRestart()
    {
        RequireLinux();
        using var fixture = new DataGridPeerFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken, 40);
        await using var cluster = new LiveCluster(fixture);
        cluster.StartAll();
        var leader = await cluster.Leader(timeout.Token);
        var opened = await cluster.Nodes[leader]!.ProposeAsync(
            DataGridCommittedCommand.OpenSession(
                DataGridPeerFixture.Principal,
                DataGridPeerFixture.Nonce,
                DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow.AddSeconds(5)),
                DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow)
            ),
            timeout.Token
        );
        Assert.Equal(RaftCompletionKind.Applied, opened.Kind);
        var session = opened.Result!.SessionNumber;
        var command = Mutation(session, 1, [7]);
        var write = await cluster.Nodes[leader]!.ProposeAsync(command, timeout.Token);
        Assert.Equal(RaftCompletionKind.Applied, write.Kind);
        var read = await cluster.Nodes[leader]!.ReadAsync(
            DataGridPeerFixture.Map,
            DataGridPeerFixture.Key,
            timeout.Token
        );
        Assert.Equal(RaftCompletionKind.Read, read.Kind);
        Assert.Equal(new byte[] { 7 }, read.Entry!.GetValue());
        Assert.True(read.Index > write.Index);
        await Task.WhenAll(
            cluster.Nodes.Select(node => node!.WaitForAppliedAsync(read.Index, timeout.Token))
        );
        var old = leader;
        await cluster.Stop(old);
        leader = await cluster.Leader(timeout.Token);
        var retry = await cluster.Nodes[leader]!.ProposeAsync(command, timeout.Token);
        Assert.Equal(RaftCompletionKind.Applied, retry.Kind);
        Assert.Equal(write.Result!.GetItem(0).Version, retry.Result!.GetItem(0).Version);
        cluster.Start(old);
        await cluster.Nodes[old]!.WaitForAppliedAsync(retry.Index, timeout.Token);
        Assert.All(cluster.Nodes, node => Assert.Null(node!.Failure));
        for (var i = 0; i < 3; i++)
            await cluster.Stop(i);
        cluster.StartAll();
        leader = await cluster.Leader(timeout.Token);
        var after = await cluster.Nodes[leader]!.ReadAsync(
            DataGridPeerFixture.Map,
            DataGridPeerFixture.Key,
            timeout.Token
        );
        Assert.Equal(new byte[] { 7 }, after.Entry!.GetValue());
        Assert.Equal(write.Result.GetItem(0).Version, after.Entry.Version);
    }

    [Fact(Timeout = 45000)]
    public async Task StoppedFollowerCatchesUpThroughChunkedSnapshotAndCurrentPublication()
    {
        RequireLinux();
        using var fixture = new DataGridPeerFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken, 40);
        await using var cluster = new LiveCluster(fixture);
        cluster.StartAll();
        var leader = await cluster.Leader(timeout.Token);
        var follower = (leader + 1) % 3;
        await cluster.Stop(follower);
        var open = await cluster.Nodes[leader]!.ProposeAsync(
            DataGridCommittedCommand.OpenSession(
                DataGridPeerFixture.Principal,
                DataGridPeerFixture.Nonce,
                DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow.AddSeconds(5)),
                DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow)
            ),
            timeout.Token
        );
        var stored = await cluster.Nodes[leader]!.ProposeAsync(
            Mutation(open.Result!.SessionNumber, 1, new byte[65536]),
            timeout.Token
        );
        Assert.Equal(RaftCompletionKind.Applied, stored.Kind);
        await cluster.Nodes[leader]!.CompactAsync(timeout.Token);
        var snapshot = cluster.Nodes[leader]!.Status.SnapshotIndex;
        Assert.True(snapshot >= stored.Index);
        cluster.Start(follower);
        await cluster.Nodes[follower]!.WaitForAppliedAsync(snapshot, timeout.Token);
        Assert.Equal(snapshot, cluster.Nodes[follower]!.Status.SnapshotIndex);
        await cluster.Stop(follower);
        cluster.Start(follower);
        Assert.True(cluster.Nodes[follower]!.Status.AppliedIndex >= snapshot);
        Assert.Throws<IOException>(() =>
            DataGridFileStore.Open(cluster.Paths[follower], fixture.Configuration, follower)
        );
        await cluster.Stop(follower);
        using var recovered = DataGridFileStore.Open(
            cluster.Paths[follower],
            fixture.Configuration,
            follower
        );
        Assert.Equal(snapshot, recovered.Image.SnapshotIndex);
        Assert.Equal(
            65536,
            recovered
                .RestoreApplication()
                .Read(DataGridPeerFixture.Map, DataGridPeerFixture.Key)!
                .GetValue()
                .Length
        );
    }

    private static DataGridCommittedCommand Mutation(long session, ulong sequence, byte[] bytes) =>
        DataGridCommittedCommand.Mutate(
            DataGridPeerFixture.Principal,
            new(
                DataGridMutationKind.Put,
                DataGridPeerFixture.Map,
                session,
                sequence,
                DataGridExpiryKind.Never,
                0,
                [new(DataGridPeerFixture.Key, bytes)]
            ),
            DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow)
        );

    private static async Task Hello(
        SslStream tls,
        DataGridPeerFixture fixture,
        CancellationToken token
    )
    {
        var hello = DataGridPeerCodec.Hello(
            fixture.Configuration,
            0,
            Guid.NewGuid(),
            fixture.Build
        );
        await DataGridPeerCodec.WriteAsync(tls, hello, token);
        var reply = await DataGridPeerCodec.ReadAsync(tls, fixture.Configuration, 1, token);
        DataGridPeerCodec.ValidateHello(reply, fixture.Configuration, fixture.Build);
        Assert.Equal(hello.AttemptId, reply.AttemptId);
    }

    private static void RequireLinux() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "TLS 1.3 peer server tests run on Linux; no TLS downgrade on macOS."
        );

    private static CancellationTokenSource Deadline(
        CancellationToken cancellationToken,
        int seconds = 15
    )
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        source.CancelAfter(TimeSpan.FromSeconds(seconds));
        return source;
    }

    private sealed class AlteredRecordStream(Stream inner) : Stream
    {
        internal bool Armed { get; set; }
        public override bool CanRead => true;
        public override bool CanWrite => true;
        public override bool CanSeek => false;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            inner.Read(buffer, offset, count);

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default
        ) => inner.ReadAsync(buffer, cancellationToken);

        public override void Write(byte[] buffer, int offset, int count)
        {
            var changed = Change(buffer.AsMemory(offset, count));
            inner.Write(changed.Span);
        }

        public override ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default
        ) => inner.WriteAsync(Change(buffer), cancellationToken);

        private ReadOnlyMemory<byte> Change(ReadOnlyMemory<byte> buffer)
        {
            if (!Armed || buffer.Length == 0)
                return buffer;
            Armed = false;
            var bytes = buffer.ToArray();
            bytes[^1] ^= 1;
            return bytes;
        }

        public override void Flush() => inner.Flush();

        public override Task FlushAsync(CancellationToken cancellationToken) =>
            inner.FlushAsync(cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();
    }

    private sealed class EchoReceiver(
        DataGridBootstrapConfiguration config,
        Func<Task>? beforeSnapshot = null
    ) : IDataGridPeerReceiver
    {
        private int _calls;
        internal int Calls => Volatile.Read(ref _calls);

        public Task<long> ObserveLeaderAsync(long term, CancellationToken cancellationToken) =>
            Task.FromResult(term);

        public async Task<RaftMessage?> ReceiveAsync(
            RaftMessage message,
            CancellationToken cancellationToken
        )
        {
            Interlocked.Increment(ref _calls);
            if (message.Kind == RaftMessageKind.SnapshotRequest && beforeSnapshot is not null)
                await beforeSnapshot();
            var kind = message.Kind switch
            {
                RaftMessageKind.VoteRequest => RaftMessageKind.VoteResponse,
                RaftMessageKind.AppendRequest => RaftMessageKind.AppendResponse,
                _ => RaftMessageKind.SnapshotResponse,
            };
            return new(
                kind,
                message.To,
                message.From,
                message.Term,
                message.Token,
                config.IncarnationId,
                config.ComputeDigest(),
                index: message.Snapshot?.Descriptor.LastIncludedIndex
                    ?? (message.Entries.Count == 0 ? message.Index : message.Entries[^1].Index),
                accepted: true
            );
        }
    }

    internal sealed class LiveCluster : IAsyncDisposable
    {
        private readonly DataGridPeerFixture _fixture;
        private readonly IReadOnlyList<DataGridSecurityContext>? _security;
        private readonly Action<int, DataGridStorageStep>? _observe;
        private readonly long[] _epochs = new long[3];

        internal LiveCluster(
            DataGridPeerFixture fixture,
            IReadOnlyList<DataGridSecurityContext>? security = null,
            Action<int, DataGridStorageStep>? observe = null
        )
        {
            _fixture = fixture;
            _security = security;
            _observe = observe;
            Paths = Enumerable
                .Range(0, 3)
                .Select(_ =>
                    Path.Combine(
                        Path.GetTempPath(),
                        "hostloom-peer-" + Guid.NewGuid().ToString("N")
                    )
                )
                .ToArray();
            for (var i = 0; i < 3; i++)
            {
                Directory.CreateDirectory(Paths[i]);
                using var store = DataGridFileStore.Bootstrap(Paths[i], fixture.Configuration, i);
            }
        }

        internal string[] Paths { get; }
        internal DataGridPeerRuntime?[] Nodes { get; } = new DataGridPeerRuntime?[3];

        internal void StartAll()
        {
            for (var i = 0; i < 3; i++)
                Start(i);
        }

        [SuppressMessage(
            "Reliability",
            "CA2000:Dispose objects before losing scope",
            Justification = "Successful construction transfers the store to the actor; the catch disposes failed construction."
        )]
        internal void Start(int node)
        {
            var port = _fixture.Configuration.GetMember(node).Port;
            if (((System.Net.IPEndPoint)_fixture.Listeners[node].LocalEndpoint).Port != port)
            {
                _fixture.Listeners[node].Stop();
                _fixture.Listeners[node] = new TcpListener(System.Net.IPAddress.Loopback, port);
                _fixture.Listeners[node].Start(16);
            }
            var store = DataGridFileStore.Open(
                Paths[node],
                _fixture.Configuration,
                node,
                observe: _observe is null ? null : step => _observe(node, step)
            );
            try
            {
                Nodes[node] = new(
                    store,
                    _fixture.Credentials(node),
                    _fixture.Listeners[node],
                    DataGridPeerFixture.Routes,
                    ++_epochs[node],
                    security: _security?[node]
                );
            }
            catch
            {
                store.Dispose();
                throw;
            }
        }

        internal async Task Stop(int node)
        {
            if (Nodes[node] is { } runtime)
            {
                await runtime.DisposeAsync();
                Nodes[node] = null;
            }
        }

        internal async Task<int> Leader(CancellationToken token)
        {
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
            var candidates = Enumerable
                .Range(0, 3)
                .Where(i => Nodes[i] is not null)
                .Select(i => (Node: i, Ready: Nodes[i]!.WaitForLeaderAsync(cancellation.Token)))
                .ToArray();
            var ready = await Task.WhenAny(candidates.Select(item => item.Ready));
            await ready;
            var leader = candidates.First(item => item.Ready == ready).Node;
            await cancellation.CancelAsync();
            foreach (var candidate in candidates)
            {
                try
                {
                    await candidate.Ready;
                }
                catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
            }
            return leader;
        }

        public async ValueTask DisposeAsync()
        {
            for (var i = 0; i < 3; i++)
                await Stop(i);
            foreach (var path in Paths)
                Directory.Delete(path, recursive: true);
        }
    }
}
