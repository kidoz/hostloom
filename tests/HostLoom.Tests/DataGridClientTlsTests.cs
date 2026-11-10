using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using HostLoom.DataGrid.Runtime;
using HostLoom.DataGrid.Transport;
using Xunit;
using GridClient = HostLoom.DataGrid.Client.DataGridClient;

namespace HostLoom.Tests;

public sealed class DataGridClientTlsTests
{
    [Fact(Timeout = 30000)]
    public async Task ClientRoundTripPreservesConditionalVersionsAndBatchCapture()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var fixture = new ClientCluster();
        var leader = await fixture.Cluster.Leader(timeout.Token);
        await using var client = fixture.Client(leader);
        var session = await Open(client, timeout.Token);
        var put = await client.ExecuteAsync(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session)),
            timeout.Token
        );
        var version = AssertStored(put);
        var read = await client.ExecuteAsync(
            DataGridClientRequest.Read(
                DataGridPeerFixture.Map,
                [DataGridPeerFixture.Key, DataGridKey.FromUtf8("catalog:us")]
            ),
            timeout.Token
        );
        Assert.Null(read.Reply.Failure);
        Assert.Equal(version, read.Reply.Read!.GetItem(0)!.Version);
        Assert.Null(read.Reply.Read.GetItem(1));
        var reject = await client.ExecuteAsync(
            DataGridClientRequest.Mutate(
                DataGridClientCodecTests.Mutation(
                    session,
                    2,
                    DataGridMutationKind.CompareRemove,
                    new(fixture.Peer.Configuration.IncarnationId, version.LogIndex + 1)
                )
            ),
            timeout.Token
        );
        Assert.Equal(
            DataGridMutationStatus.VersionMismatch,
            reject.Reply.Result!.GetItem(0).Status
        );
        var remove = await client.ExecuteAsync(
            DataGridClientRequest.Mutate(
                DataGridClientCodecTests.Mutation(
                    session,
                    3,
                    DataGridMutationKind.CompareRemove,
                    version
                )
            ),
            timeout.Token
        );
        Assert.Equal(DataGridMutationStatus.Removed, remove.Reply.Result!.GetItem(0).Status);
        var recreate = await client.ExecuteAsync(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session, 4)),
            timeout.Token
        );
        Assert.NotEqual(version, AssertStored(recreate));
        var close = await client.ExecuteAsync(
            DataGridClientRequest.CloseSession(session, 5),
            timeout.Token
        );
        Assert.Equal(DataGridMessageKind.CloseSessionResult, close.Reply.Kind);
        var expired = await client.ExecuteAsync(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session, 6)),
            timeout.Token
        );
        Assert.Equal(DataGridClientFailureCode.SessionExpired, expired.Reply.Failure!.Code);
        fixture.AssertHealthy();
    }

    [Fact(Timeout = 30000)]
    public async Task FollowerRoutingAndBoundaryHistoryRejectStaleReadNegativeControl()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var fixture = new ClientCluster();
        var leader = await fixture.Cluster.Leader(timeout.Token);
        await using var opener = fixture.Client(leader);
        var session = await Open(opener, timeout.Token);
        var history = new DataGridClientHistory();
        await using var client = fixture.Client((leader + 1) % 3, history);
        var stored = await client.ExecuteAsync(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session)),
            timeout.Token
        );
        var version = AssertStored(stored);
        Assert.InRange(stored.Attempts.Count, 2, 3);
        Assert.Equal(
            stored.Attempts.Count,
            stored.Attempts.Select(a => a.AttemptId).Distinct().Count()
        );
        var cas = await client.ExecuteAsync(
            DataGridClientRequest.Mutate(
                DataGridClientCodecTests.Mutation(
                    session,
                    2,
                    DataGridMutationKind.CompareExchange,
                    version,
                    9
                )
            ),
            timeout.Token
        );
        AssertStored(cas);
        await client.ExecuteAsync(
            DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]),
            timeout.Token
        );
        var operations = history.GetOperations();
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker.Check(operations).Status
        );
        var original = operations[^1];
        var badFrame = DataGridClientCodec.EncodeReply(
            original.Request,
            DataGridClientReply.Completed(
                DataGridMessageKind.ReadResult,
                read: new(new(1), [null])
            ),
            fixture.Peer.Configuration.IncarnationId,
            fixture.Peer.Configuration.GetMember(leader).MemberId,
            Guid.NewGuid()
        );
        var badReply = DataGridClientCodec.DecodeReply(
            badFrame,
            original.Request,
            badFrame.AttemptId,
            fixture.Peer.Configuration
        );
        // Replay a forged boundary response through a separate receiver to obtain a real outcome.
        fixture.Override = (_, request, _) =>
            Task.FromResult(
                request.Kind == DataGridMessageKind.Read
                    ? badReply
                    : throw new InvalidOperationException()
            );
        var forged = await client.ExecuteAsync(original.Request, timeout.Token);
        var negative = operations.ToArray();
        negative[^1] = original with { Outcome = forged };
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            DataGridClientHistoryChecker.Check(negative).Status
        );
        Assert.Equal(
            DataGridHistoryStatus.Inconclusive,
            DataGridClientHistoryChecker.Check(operations, 1).Status
        );
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker
                .Check([original with { Outcome = null, CompletionOrder = null }])
                .Status
        );
        fixture.AssertHealthy();
    }

    [Fact(Timeout = 30000)]
    public async Task LostCommittedReplyRetriesSameLogicalBytesAndReturnsOriginalVersion()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var fixture = new ClientCluster();
        var leader = await fixture.Cluster.Leader(timeout.Token);
        await using var client = fixture.Client(leader);
        var session = await Open(client, timeout.Token);
        DataGridEntryVersion original = default;
        var digests = new List<byte[]>();
        fixture.Override = async (node, request, token) =>
        {
            var reply = await node.ExecuteAsync(DataGridPeerFixture.Principal, request, token);
            if (request.Mutation is { } command)
            {
                lock (digests)
                    digests.Add(command.ComputeDigest());
                if (
                    reply.Result?.GetItem(0).Status == DataGridMutationStatus.Stored
                    && original.IsEmpty
                )
                {
                    original = reply.Result.GetItem(0).Version;
                    throw new IOException("Injected loss after durable application.");
                }
            }
            return reply;
        };
        var request = DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session));
        var outcome = await client.ExecuteAsync(request, timeout.Token);
        Assert.Equal(original, AssertStored(outcome));
        Assert.InRange(outcome.Attempts.Count, 2, 3);
        Assert.True(outcome.MayHaveBeenAdmitted);
        Assert.All(digests, digest => Assert.Equal(request.Mutation!.ComputeDigest(), digest));
        var retry = await client.ExecuteAsync(request, timeout.Token);
        Assert.Equal(original, AssertStored(retry));
        fixture.AssertHealthy();
    }

    [Fact(Timeout = 30000)]
    public async Task LaterPreAdmissionDenialRetainsEarlierUncertainAdmission()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var fixture = new ClientCluster();
        var leader = await fixture.Cluster.Leader(timeout.Token);
        await using var client = fixture.Client(leader);
        var session = await Open(client, timeout.Token);
        var lost = false;
        fixture.Override = async (node, request, token) =>
        {
            if (lost)
                return DataGridClientReply.Failed(
                    new(DataGridClientFailureCode.Unauthorized, false)
                );
            var reply = await node.ExecuteAsync(DataGridPeerFixture.Principal, request, token);
            if (reply.Result is not null)
            {
                lost = true;
                throw new IOException("Injected committed reply loss.");
            }
            return reply;
        };
        var outcome = await client.ExecuteAsync(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session)),
            timeout.Token
        );
        Assert.Equal(DataGridClientFailureCode.Unauthorized, outcome.Reply.Failure!.Code);
        Assert.False(outcome.Reply.Failure.MayHaveBeenAdmitted);
        Assert.True(outcome.MayHaveBeenAdmitted);
        Assert.Equal(2, outcome.Attempts.Count);
        Assert.All(outcome.Attempts, a => Assert.True(a.RequestSubmitted));
        fixture.AssertHealthy();
    }

    [Fact(Timeout = 30000)]
    public async Task DisableAfterCommitReturnsUnknownAndStopsFurtherAttempts()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var fixture = new ClientCluster();
        var leader = await fixture.Cluster.Leader(timeout.Token);
        await using var client = fixture.Client(leader);
        var session = await Open(client, timeout.Token);
        fixture.Override = async (node, request, token) =>
        {
            var reply = await node.ExecuteAsync(DataGridPeerFixture.Principal, request, token);
            Assert.NotNull(reply.Result);
            client.Disable();
            throw new IOException("Injected loss after disable.");
        };
        var outcome = await client.ExecuteAsync(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session)),
            timeout.Token
        );
        Assert.Equal(DataGridClientFailureCode.OutcomeUnknown, outcome.Reply.Failure!.Code);
        Assert.True(outcome.MayHaveBeenAdmitted);
        Assert.Single(outcome.Attempts);
        var disabled = await client.ExecuteAsync(
            DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]),
            timeout.Token
        );
        Assert.False(disabled.MayHaveBeenAdmitted);
        Assert.Empty(disabled.Attempts);
        await client.DisposeAsync();
        await client.DisposeAsync();
        var disposed = await client.ExecuteAsync(
            DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]),
            timeout.Token
        );
        Assert.Empty(disposed.Attempts);
    }

    [Fact(Timeout = 30000)]
    public async Task ReadOnlyGrantsRejectSessionsAndWritesBeforeActorDispatch()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var fixture = new ClientCluster();
        var leader = await fixture.Cluster.Leader(timeout.Token);
        await using var client = fixture.Client(leader, readOnly: true);
        fixture.Override = (_, _, _) =>
            throw new InvalidOperationException("Unauthorized operation reached actor.");
        var open = await client.ExecuteAsync(
            DataGridClientRequest.OpenSession(
                Guid.NewGuid(),
                new DataGridTimestamp(Now().UtcTicks + TimeSpan.FromSeconds(4).Ticks)
            ),
            timeout.Token
        );
        Assert.Equal(DataGridClientFailureCode.Unauthorized, open.Reply.Failure!.Code);
        var mutation = await client.ExecuteAsync(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation()),
            timeout.Token
        );
        Assert.Equal(DataGridClientFailureCode.Unauthorized, mutation.Reply.Failure!.Code);
        Assert.False(mutation.MayHaveBeenAdmitted);
        Assert.Null(fixture.Servers[leader].FatalError);
        fixture.Override = null;
        var read = await client.ExecuteAsync(
            DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]),
            timeout.Token
        );
        Assert.Null(read.Reply.Failure);
        fixture.AssertHealthy();
    }

    [Theory(Timeout = 30000)]
    [InlineData("server-pin")]
    [InlineData("client-pin")]
    [InlineData("principal")]
    [InlineData("expired")]
    public async Task UnboundOrExpiredCredentialsNeverReachActor(string reason)
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var fixture = new ClientCluster();
        var leader = await fixture.Cluster.Leader(timeout.Token);
        using var invalid = fixture.Peer.Issue(
            "catalog-unknown.example",
            expired: reason == "expired",
            clientOnly: true
        );
        if (reason == "expired")
        {
            Assert.Throws<System.Security.Authentication.AuthenticationException>(() =>
                fixture.Client(leader, certificate: invalid)
            );
            fixture.AssertHealthy();
            return;
        }
        await using var client = fixture.Client(
            leader,
            certificate: reason is "client-pin" or "expired" ? invalid : null,
            wrongServerPin: reason == "server-pin",
            principal: reason == "principal" ? Guid.NewGuid() : null
        );
        var calls = 0;
        fixture.Override = (_, _, _) =>
        {
            Interlocked.Increment(ref calls);
            throw new InvalidOperationException();
        };
        var result = await client.ExecuteAsync(
            DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]),
            timeout.Token
        );
        Assert.NotNull(result.Reply.Failure);
        Assert.False(result.MayHaveBeenAdmitted);
        Assert.Equal(0, calls);
        fixture.AssertHealthy();
    }

    [Fact(Timeout = 30000)]
    public async Task AppliedSessionCannotBeUsedByAnotherAuthenticatedPrincipal()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var fixture = new ClientCluster();
        var leader = await fixture.Cluster.Leader(timeout.Token);
        await using var owner = fixture.Client(leader);
        var session = await Open(owner, timeout.Token);
        await using var other = fixture.Client(leader, otherWriter: true);
        // Full map/session grants do not transfer ownership of another principal's session.
        var result = await other.ExecuteAsync(
            DataGridClientRequest.CloseSession(session, 1),
            timeout.Token
        );
        Assert.Equal(DataGridClientFailureCode.SessionExpired, result.Reply.Failure!.Code);
        Assert.True(result.MayHaveBeenAdmitted);
    }

    [Fact(Timeout = 30000)]
    public async Task BatchReadCapacityFailureDoesNotFaultActorAndTtlSharesCaptureTime()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var fixture = new ClientCluster();
        var leader = await fixture.Cluster.Leader(timeout.Token);
        await using var client = fixture.Client(leader);
        var session = await Open(client, timeout.Token);
        var keys = Enumerable
            .Range(0, 16)
            .Select(i => DataGridKey.FromUtf8($"catalog:{i}"))
            .ToArray();
        var value = new byte[65500];
        for (var part = 0; part < 2; part++)
        {
            var command = new DataGridMutationCommand(
                DataGridMutationKind.Put,
                DataGridPeerFixture.Map,
                session,
                (ulong)(part + 1),
                DataGridExpiryKind.Relative,
                TimeSpan.FromMinutes(1).Ticks,
                keys.Skip(part * 8)
                    .Take(8)
                    .Select(key => new DataGridMutationItem(key, value))
                    .ToArray()
            );
            var outcome = await client.ExecuteAsync(
                DataGridClientRequest.Mutate(command),
                timeout.Token
            );
            Assert.Null(outcome.Reply.Failure);
            Assert.All(
                Enumerable.Range(0, 8),
                i =>
                    Assert.Equal(
                        DataGridMutationStatus.Stored,
                        outcome.Reply.Result!.GetItem(i).Status
                    )
            );
        }
        var large = await client.ExecuteAsync(
            DataGridClientRequest.Read(DataGridPeerFixture.Map, keys),
            timeout.Token
        );
        Assert.Equal(DataGridClientFailureCode.CapacityExceeded, large.Reply.Failure!.Code);
        var read = await client.ExecuteAsync(
            DataGridClientRequest.Read(DataGridPeerFixture.Map, keys[..2]),
            timeout.Token
        );
        Assert.Null(read.Reply.Failure);
        Assert.Equal(
            read.Reply.Read!.GetItem(0)!.RemainingTimeToLive,
            read.Reply.Read.GetItem(1)!.RemainingTimeToLive
        );
        Assert.InRange(
            read.Reply.Read.GetItem(0)!.RemainingTimeToLive!.Value.Ticks,
            1,
            TimeSpan.FromMinutes(1).Ticks
        );
        fixture.AssertHealthy();
    }

    [Fact(Timeout = 30000)]
    public async Task LostConditionalDeleteReplyCannotDeleteRecreatedEntryOnRetry()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var fixture = new ClientCluster();
        var leader = await fixture.Cluster.Leader(timeout.Token);
        await using var client = fixture.Client(leader);
        var session = await Open(client, timeout.Token);
        var replacementSession = await Open(client, timeout.Token);
        var put = await client.ExecuteAsync(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session)),
            timeout.Token
        );
        var version = AssertStored(put);
        var injected = false;
        fixture.Override = async (node, request, token) =>
        {
            var result = await node.ExecuteAsync(DataGridPeerFixture.Principal, request, token);
            if (
                !injected
                && request.Mutation?.Kind == DataGridMutationKind.CompareRemove
                && result.Result?.GetItem(0).Status == DataGridMutationStatus.Removed
            )
            {
                injected = true;
                var recreate = await node.ExecuteAsync(
                    DataGridPeerFixture.Principal,
                    DataGridClientRequest.Mutate(
                        DataGridClientCodecTests.Mutation(replacementSession, value: 9)
                    ),
                    token
                );
                Assert.NotNull(recreate.Result);
                throw new IOException("Injected delete reply loss after recreation.");
            }
            return result;
        };
        var remove = await client.ExecuteAsync(
            DataGridClientRequest.Mutate(
                DataGridClientCodecTests.Mutation(
                    session,
                    2,
                    DataGridMutationKind.CompareRemove,
                    version
                )
            ),
            timeout.Token
        );
        Assert.True(injected);
        Assert.Null(remove.Reply.Failure);
        Assert.Equal(DataGridMutationStatus.Removed, remove.Reply.Result!.GetItem(0).Status);
        var read = await client.ExecuteAsync(
            DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]),
            timeout.Token
        );
        Assert.Equal(9, read.Reply.Read!.GetItem(0)!.GetValue()[0]);
        Assert.NotEqual(version, read.Reply.Read.GetItem(0)!.Version);
        fixture.AssertHealthy();
    }

    [Fact(Timeout = 30000)]
    public async Task MalformedServerResultIsTerminalAndPreservesSubmissionUncertainty()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var fixture = new ClientCluster();
        var leader = await fixture.Cluster.Leader(timeout.Token);
        await using var client = fixture.Client(leader);
        fixture.Override = (_, _, _) =>
            Task.FromResult(
                DataGridClientReply.Completed(
                    DataGridMessageKind.ReadResult,
                    read: new(new(42), [null, null])
                )
            );
        var outcome = await client.ExecuteAsync(
            DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]),
            timeout.Token
        );
        Assert.Equal(DataGridClientFailureCode.ProtocolMismatch, outcome.Reply.Failure!.Code);
        Assert.True(outcome.MayHaveBeenAdmitted);
        Assert.Single(outcome.Attempts);
        fixture.AssertHealthy();
    }

    private static DataGridTimestamp Now() => new(DateTimeOffset.UtcNow.Ticks);

    private static void RequireLinux() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "Live TLS 1.3 server evidence requires Linux."
        );

    private static CancellationTokenSource Deadline(CancellationToken token)
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(token);
        source.CancelAfter(TimeSpan.FromSeconds(25));
        return source;
    }

    private static async Task<long> Open(GridClient client, CancellationToken token)
    {
        var open = await client.ExecuteAsync(
            DataGridClientRequest.OpenSession(
                Guid.NewGuid(),
                new DataGridTimestamp(Now().UtcTicks + TimeSpan.FromSeconds(4).Ticks)
            ),
            token
        );
        Assert.Null(open.Reply.Failure);
        return open.Reply.Result!.SessionNumber;
    }

    private static DataGridEntryVersion AssertStored(DataGridClientOutcome outcome)
    {
        Assert.Null(outcome.Reply.Failure);
        Assert.Equal(DataGridMutationStatus.Stored, outcome.Reply.Result!.GetItem(0).Status);
        return outcome.Reply.Result.GetItem(0).Version;
    }

    internal sealed class ClientCluster : IAsyncDisposable
    {
        private readonly X509Certificate2 _writer,
            _reader,
            _otherWriter;
        private readonly Guid _otherId = Guid.NewGuid();
        private readonly Guid _readerId = Guid.NewGuid();
        private readonly TcpListener[] _listeners;
        internal DataGridPeerFixture Peer { get; } = new();
        internal DataGridPeerTlsTests.LiveCluster Cluster { get; }
        internal DataGridClientServer[] Servers { get; }
        internal Func<
            DataGridPeerRuntime,
            DataGridClientRequest,
            CancellationToken,
            Task<DataGridClientReply>
        >? Override { get; set; }

        internal ClientCluster()
        {
            _writer = Peer.Issue("catalog-writer.example", clientOnly: true);
            _reader = Peer.Issue("catalog-reader.example", clientOnly: true);
            _otherWriter = Peer.Issue("catalog-editor.example", clientOnly: true);
            Cluster = new(Peer);
            Cluster.StartAll();
            _listeners = Enumerable
                .Range(0, 3)
                .Select(_ => new TcpListener(IPAddress.Loopback, 0))
                .ToArray();
            var policy = new DataGridClientAccessPolicy(
                Peer.Configuration,
                Peer.Root,
                [
                    new(
                        _otherId,
                        _otherWriter.GetCertHash(HashAlgorithmName.SHA256),
                        true,
                        [
                            new(
                                DataGridPeerFixture.Map,
                                DataGridMapActions.Read | DataGridMapActions.Mutate
                            ),
                        ]
                    ),
                    new(
                        DataGridPeerFixture.Principal,
                        _writer.GetCertHash(HashAlgorithmName.SHA256),
                        true,
                        [
                            new(
                                DataGridPeerFixture.Map,
                                DataGridMapActions.Read | DataGridMapActions.Mutate
                            ),
                        ]
                    ),
                    new(
                        _readerId,
                        _reader.GetCertHash(HashAlgorithmName.SHA256),
                        false,
                        [new(DataGridPeerFixture.Map, DataGridMapActions.Read)]
                    ),
                ]
            );
            Servers = Enumerable
                .Range(0, 3)
                .Select(i => new DataGridClientServer(
                    Peer.Credentials(i),
                    policy,
                    _listeners[i],
                    new Receiver(this, Cluster.Nodes[i]!)
                ))
                .ToArray();
            foreach (var server in Servers)
                server.Start();
        }

        internal GridClient Client(
            int member,
            DataGridClientHistory? history = null,
            bool readOnly = false,
            X509Certificate2? certificate = null,
            bool wrongServerPin = false,
            Guid? principal = null,
            bool otherWriter = false
        ) =>
            new(
                new(
                    Peer.Configuration,
                    principal
                        ?? (
                            otherWriter ? _otherId
                            : readOnly ? _readerId
                            : DataGridPeerFixture.Principal
                        ),
                    certificate
                        ?? (
                            otherWriter ? _otherWriter
                            : readOnly ? _reader
                            : _writer
                        ),
                    Peer.Root,
                    Enumerable
                        .Range(0, 3)
                        .Select(i => new DataGridClientEndpoint(
                            Peer.Configuration.GetMember(i).MemberId,
                            Peer.Configuration.GetMember(i).DnsName,
                            (ushort)((IPEndPoint)_listeners[i].LocalEndpoint).Port,
                            wrongServerPin ? new byte[32] : Peer.Fingerprints[i],
                            IPAddress.Loopback
                        ))
                        .ToArray(),
                    Peer.Build,
                    member
                ),
                history
            );

        internal void AssertHealthy()
        {
            Assert.All(Servers, server => Assert.Null(server.FatalError));
            Assert.All(Cluster.Nodes, node => Assert.Null(node!.Failure));
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var server in Servers)
                await server.DisposeAsync();
            await Cluster.DisposeAsync();
            _writer.Dispose();
            _reader.Dispose();
            _otherWriter.Dispose();
            Peer.Dispose();
        }

        private sealed class Receiver(ClientCluster cluster, DataGridPeerRuntime node)
            : IDataGridClientReceiver
        {
            public Task<DataGridClientReply> ExecuteAsync(
                Guid principal,
                DataGridClientRequest request,
                CancellationToken cancellationToken
            ) =>
                cluster.Override is { } hook
                    ? hook(node, request, cancellationToken)
                    : node.ExecuteAsync(principal, request, cancellationToken);
        }
    }
}
