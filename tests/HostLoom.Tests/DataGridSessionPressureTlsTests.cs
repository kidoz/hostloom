using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using HostLoom.DataGrid.Storage;
using HostLoom.DataGrid.Transport;
using Xunit;
using GridClient = HostLoom.DataGrid.Client.DataGridClient;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridSessionPressureTlsTests
{
    [Fact(Timeout = 180000)]
    public async Task PrincipalSessionPressurePreservesIsolationRetainedResultsAndRecovery()
    {
        RequireLinux();
        using var timeout = Deadline(
            TimeSpan.FromSeconds(170),
            TestContext.Current.CancellationToken
        );
        using var capture = new DataGridMeterCapture();
        await using (var lab = new Lab(2))
        {
            var token = timeout.Token;
            var leader = await lab.Cluster.Leader(token);
            var writer = lab.Client(0, leader);
            var sessions = new long[64];
            for (var i = 0; i < sessions.Length; i++)
                sessions[i] = await lab.Open(writer, token);
            Assert.Equal(64, sessions.Distinct().Count());
            Failure(
                await lab.Send(writer, lab.OpenRequest(), token),
                DataGridClientFailureCode.CapacityExceeded
            );

            var storedRequest = Mutation(sessions[0]);
            var original = Item(await lab.Send(writer, storedRequest, token));
            Assert.Equal(DataGridMutationStatus.Stored, original.Status);
            var rejectedRequest = DataGridClientRequest.Mutate(
                DataGridClientCodecTests.Mutation(
                    sessions[1],
                    kind: DataGridMutationKind.CompareRemove,
                    version: new(
                        lab.Peer.Configuration.IncarnationId,
                        original.Version.LogIndex + 1
                    )
                )
            );
            var rejected = Item(await lab.Send(writer, rejectedRequest, token));
            Assert.Equal(DataGridMutationStatus.VersionMismatch, rejected.Status);

            // Dispose closes the SDK's transport work, not the replicated sessions or their replay state.
            await writer.DisposeAsync();
            await capture.WaitFor(_ => capture.Reserved("request", "client") == 0, token);
            var other = lab.Client(1, leader);
            var otherSession = await lab.Open(other, token);
            var current = Item(await lab.Send(other, Mutation(otherSession, value: 9), token));
            Assert.NotEqual(original.Version, current.Version);
            await lab.AppliedEverywhere(current.Version.LogIndex, token);

            writer = lab.Client(0, leader);
            await Retained(writer);
            var closedRequest = DataGridClientRequest.CloseSession(sessions[2], 1);
            Assert.Equal(
                DataGridMessageKind.CloseSessionResult,
                (await lab.Send(writer, closedRequest, token)).Reply.Kind
            );
            Failure(
                await lab.Send(writer, lab.OpenRequest(), token),
                DataGridClientFailureCode.CapacityExceeded
            );

            await lab.Cluster.Stop(leader);
            var successor = await lab.Cluster.Leader(token);
            Assert.NotEqual(leader, successor);
            writer = lab.Client(0, successor);
            await Retained(writer);
            await lab.AssertRead(writer, current.Version, 9, token);
            lab.Cluster.Start(leader);
            await lab.AppliedEverywhere(lab.Cluster.Nodes[successor]!.Status.AppliedIndex, token);
            await lab.Compact(token);
            await lab.StopAll();
            lab.AssertRecoveredSessions(65);
            lab.Cluster.StartAll();
            leader = await lab.Cluster.Leader(token);
            writer = lab.Client(0, leader);
            await Retained(writer);
            Failure(
                await lab.Send(writer, lab.OpenRequest(), token),
                DataGridClientFailureCode.CapacityExceeded
            );

            lab.Clock.Advance(TimeSpan.FromMinutes(10) + TimeSpan.FromTicks(1));
            // A strong read commits a time-advance/retirement step; opens alone do not reclaim sessions.
            await lab.AssertRead(writer, current.Version, 9, token);
            var fresh = await lab.Open(writer, token);
            Assert.True(fresh > otherSession);
            Failure(
                await lab.Send(writer, storedRequest, token),
                DataGridClientFailureCode.SessionExpired
            );
            Failure(
                await lab.Send(writer, rejectedRequest, token),
                DataGridClientFailureCode.SessionExpired
            );
            Failure(
                await lab.Send(writer, Mutation(sessions[2]), token),
                DataGridClientFailureCode.SessionExpired
            );
            await lab.AssertRead(writer, current.Version, 9, token);
            lab.Healthy();

            async Task Retained(GridClient client)
            {
                Assert.Equal(original, Item(await lab.Send(client, storedRequest, token)));
                Assert.Equal(rejected, Item(await lab.Send(client, rejectedRequest, token)));
            }
        }
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Fact(Timeout = 900000)]
    public async Task GlobalSessionPressureSurvivesCompactionRestartAndBoundedExpiry()
    {
        RequireLinux();
        Assert.SkipUnless(
            Environment.GetEnvironmentVariable("HOSTLOOM_DATA_GRID_SESSION_PRESSURE") == "1",
            "Full TLS session-capacity qualification takes about eleven minutes at the production handshake rate; opt in explicitly."
        );
        using var timeout = Deadline(
            TimeSpan.FromMinutes(14),
            TestContext.Current.CancellationToken
        );
        await using var lab = new Lab(65);
        var token = timeout.Token;
        var leader = await lab.Cluster.Leader(token);
        var clients = Enumerable.Range(0, 65).Select(i => lab.Client(i, leader)).ToArray();
        var sessions = new HashSet<long>();
        long closed = 0;
        DataGridClientRequest? request = null;
        DataGridMutationResult original = default;
        for (var round = 0; round < 64; round++)
        {
            for (var principal = 0; principal < 64; principal++)
            {
                var session = await lab.Open(clients[principal], token);
                Assert.True(sessions.Add(session));
                if (round == 0 && principal == 0)
                {
                    request = Mutation(session);
                    original = Item(await lab.Send(clients[principal], request, token));
                }
                if (round == 1 && principal == 0)
                    closed = session;
            }
            if ((round + 1) % 8 == 0)
            {
                TestContext.Current.TestOutputHelper!.WriteLine(
                    FormattableString.Invariant(
                        $"Opened {sessions.Count} sessions with automatic maintenance."
                    )
                );
            }
        }
        Assert.Equal(4096, sessions.Count);
        Assert.Equal(4096, sessions.Max());
        Assert.NotNull(request);
        await lab.WaitFor(
            () => lab.Cluster.Nodes.All(node => node!.Status.SnapshotIndex >= 3072),
            token
        );
        Assert.All(lab.Cluster.Nodes, node => Assert.True(node!.Status.AppliedIndex > 4096));
        // Principal 65 has no sessions: this distinguishes the group ceiling from a principal ceiling.
        Failure(
            await lab.Send(clients[64], lab.OpenRequest(), token),
            DataGridClientFailureCode.CapacityExceeded
        );
        Assert.Equal(original, Item(await lab.Send(clients[0], request, token)));
        Assert.Equal(DataGridMutationStatus.Stored, original.Status);
        await lab.AssertRead(clients[64], original.Version, 7, token);
        Assert.Equal(
            DataGridMessageKind.CloseSessionResult,
            (await lab.Send(clients[0], DataGridClientRequest.CloseSession(closed, 1), token))
                .Reply
                .Kind
        );
        Failure(
            await lab.Send(clients[64], lab.OpenRequest(), token),
            DataGridClientFailureCode.CapacityExceeded
        );
        await lab.AppliedEverywhere(lab.Cluster.Nodes[leader]!.Status.AppliedIndex, token);
        await lab.WaitFor(
            () => lab.Cluster.Paths.All(path => Directory.GetFiles(path).Length <= 4),
            token
        );
        await lab.StopAll();
        lab.AssertRecoveredSessions(4096);
        lab.Cluster.StartAll();
        leader = await lab.Cluster.Leader(token);
        var replay = lab.Client(0, leader);
        var outsider = lab.Client(64, leader);
        Assert.Equal(original, Item(await lab.Send(replay, request, token)));
        Failure(
            await lab.Send(outsider, lab.OpenRequest(), token),
            DataGridClientFailureCode.CapacityExceeded
        );
        await lab.AssertRead(outsider, original.Version, 7, token);
        lab.Clock.Advance(TimeSpan.FromMinutes(10) + TimeSpan.FromTicks(1));
        await lab.AssertRead(outsider, original.Version, 7, token);
        Assert.Equal(4097, await lab.Open(outsider, token));
        Failure(await lab.Send(replay, request, token), DataGridClientFailureCode.SessionExpired);
        await lab.AssertRead(outsider, original.Version, 7, token);
        lab.Healthy();
    }

    private static void RequireLinux() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "Live TLS 1.3 session-pressure evidence requires Linux."
        );

    private static CancellationTokenSource Deadline(TimeSpan duration, CancellationToken token)
    {
        var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(duration);
        return timeout;
    }

    private static DataGridClientRequest Mutation(long session, byte value = 7) =>
        DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session, value: value));

    private static DataGridMutationResult Item(DataGridClientOutcome outcome)
    {
        Assert.Null(outcome.Reply.Failure);
        Assert.Equal(DataGridMessageKind.MutationResult, outcome.Reply.Kind);
        return outcome.Reply.Result!.GetItem(0);
    }

    private static void Failure(DataGridClientOutcome outcome, DataGridClientFailureCode code)
    {
        Assert.Equal(code, outcome.Reply.Failure!.Code);
        Assert.True(outcome.Reply.Failure.MayHaveBeenAdmitted);
        Assert.True(outcome.MayHaveBeenAdmitted);
        Assert.Contains(outcome.Attempts, attempt => attempt.RequestSubmitted);
    }

    internal sealed class LogicalClock : TimeProvider
    {
        private long _ticks = DateTimeOffset.UtcNow.Ticks;

        public override DateTimeOffset GetUtcNow() =>
            new(Interlocked.Read(ref _ticks), TimeSpan.Zero);

        internal void Advance(TimeSpan delta) => Interlocked.Add(ref _ticks, delta.Ticks);
    }

    internal sealed class Lab : IAsyncDisposable
    {
        // Pace real connections below the listener's eight starts/s without changing any production limit.
        private readonly PeriodicTimer _pacing = new(TimeSpan.FromMilliseconds(150));
        private readonly List<GridClient> _clients = [];
        private readonly X509Certificate2[] _certificates;
        private readonly Guid[] _principals;
        private readonly DataGridSecurityContext[] _security;
        private readonly DataGridClientServer[] _servers;
        private readonly int[] _ports;

        internal DataGridPeerFixture Peer { get; }
        internal LogicalClock Clock { get; } = new();
        internal DataGridPeerTlsTests.LiveCluster Cluster { get; }

        internal Lab(int principalCount, int mapKeys = 1000, long mapBytes = 1048576)
        {
            Peer = new(mapKeys, mapBytes);
            _certificates = Enumerable
                .Range(0, principalCount)
                .Select(i =>
                    Peer.Issue(
                        FormattableString.Invariant($"catalog-client-{i}.example"),
                        clientOnly: true
                    )
                )
                .ToArray();
            _principals = Enumerable
                .Range(0, principalCount)
                .Select(i => new Guid(i + 100, 0, 0, new byte[8]))
                .ToArray();
            var policy = new DataGridClientAccessPolicy(
                Peer.Configuration,
                Peer.Root,
                Enumerable
                    .Range(0, principalCount)
                    .Select(i => new DataGridClientPrincipal(
                        _principals[i],
                        _certificates[i].GetCertHash(HashAlgorithmName.SHA256),
                        true,
                        [
                            new(
                                DataGridPeerFixture.Map,
                                DataGridMapActions.Read | DataGridMapActions.Mutate
                            ),
                        ]
                    ))
                    .ToArray()
            );
            _security = Enumerable
                .Range(0, 3)
                .Select(i => new DataGridSecurityContext(Peer.Credentials(i), policy))
                .ToArray();
            Cluster = new(Peer, _security, timeProvider: Clock);
            Cluster.StartAll();
            var listeners = Enumerable
                .Range(0, 3)
                .Select(_ => new TcpListener(IPAddress.Loopback, 0))
                .ToArray();
            _servers = Enumerable
                .Range(0, 3)
                .Select(i => new DataGridClientServer(
                    _security[i],
                    listeners[i],
                    new Receiver(this, i)
                ))
                .ToArray();
            foreach (var server in _servers)
                server.Start();
            _ports = listeners
                .Select(listener => ((IPEndPoint)listener.LocalEndpoint).Port)
                .ToArray();
        }

        internal GridClient Client(int principal, int member)
        {
            var client = new GridClient(
                new(
                    Peer.Configuration,
                    _principals[principal],
                    _certificates[principal],
                    Peer.Root,
                    Enumerable
                        .Range(0, 3)
                        .Select(i => new DataGridClientEndpoint(
                            Peer.Configuration.GetMember(i).MemberId,
                            Peer.Configuration.GetMember(i).DnsName,
                            (ushort)_ports[i],
                            Peer.Fingerprints[i],
                            IPAddress.Loopback
                        ))
                        .ToArray(),
                    Peer.Build,
                    member
                )
            );
            _clients.Add(client);
            return client;
        }

        internal DataGridClientRequest OpenRequest() =>
            DataGridClientRequest.OpenSession(
                Guid.NewGuid(),
                DataGridTimestamp.FromUtc(Clock.GetUtcNow().AddSeconds(4))
            );

        internal async Task<DataGridClientOutcome> Send(
            GridClient client,
            DataGridClientRequest request,
            CancellationToken token
        )
        {
            Assert.True(await _pacing.WaitForNextTickAsync(token));
            return await client.ExecuteAsync(request, token);
        }

        internal async Task<long> Open(GridClient client, CancellationToken token)
        {
            var result = await Send(client, OpenRequest(), token);
            Assert.Null(result.Reply.Failure);
            Assert.Equal(DataGridMessageKind.OpenSessionResult, result.Reply.Kind);
            return result.Reply.Result!.SessionNumber;
        }

        internal async Task AssertRead(
            GridClient client,
            DataGridEntryVersion version,
            byte value,
            CancellationToken token
        )
        {
            var result = await Send(
                client,
                DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]),
                token
            );
            Assert.Null(result.Reply.Failure);
            Assert.Equal(version, result.Reply.Read!.GetItem(0)!.Version);
            Assert.Equal(new[] { value }, result.Reply.Read.GetItem(0)!.GetValue());
        }

        internal Task AppliedEverywhere(long index, CancellationToken token) =>
            Task.WhenAll(Cluster.Nodes.Select(node => node!.WaitForAppliedAsync(index, token)));

        internal async Task Compact(CancellationToken token)
        {
            var leader = await Cluster.Leader(token);
            var index = Cluster.Nodes[leader]!.Status.AppliedIndex;
            await AppliedEverywhere(index, token);
            foreach (var node in Cluster.Nodes)
            {
                await node!.CompactAsync(token);
                // Join a later actor turn before reading the passive snapshot publication.
                await node.WaitForAppliedAsync(index, token);
            }
            Assert.All(Cluster.Nodes, node => Assert.True(node!.Status.SnapshotIndex >= index));
        }

        internal async Task StopAll()
        {
            for (var i = 0; i < 3; i++)
                await Cluster.Stop(i);
        }

        internal async Task WaitFor(Func<bool> condition, CancellationToken token)
        {
            using var poll = new PeriodicTimer(TimeSpan.FromMilliseconds(25));
            while (!condition())
            {
                Assert.All(_servers, server => Assert.Null(server.FatalError));
                Assert.All(
                    Cluster.Nodes.Where(node => node is not null),
                    node => Assert.Null(node!.Failure)
                );
                Assert.True(await poll.WaitForNextTickAsync(token));
            }
        }

        internal void AssertRecoveredSessions(int count)
        {
            for (var i = 0; i < 3; i++)
            {
                using var store = DataGridFileStore.Open(Cluster.Paths[i], Peer.Configuration, i);
                var state = store.RestoreApplication();
                Assert.Equal(count, state.SessionCount);
                Assert.Equal(count, state.SessionHighWater);
            }
        }

        internal void Healthy()
        {
            Assert.All(_servers, server => Assert.Null(server.FatalError));
            Assert.All(Cluster.Nodes, node => Assert.Null(node!.Failure));
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var client in _clients)
                await client.DisposeAsync();
            await Task.WhenAll(_servers.Select(server => server.DisposeAsync().AsTask()));
            await Cluster.DisposeAsync();
            await Task.WhenAll(_security.Select(security => security.DisposeAsync().AsTask()));
            foreach (var certificate in _certificates)
                certificate.Dispose();
            _pacing.Dispose();
            Peer.Dispose();
        }

        private sealed class Receiver(Lab lab, int member) : IDataGridClientReceiver
        {
            public Task<DataGridClientReply> ExecuteAsync(
                Guid principal,
                DataGridClientRequest request,
                CancellationToken cancellationToken
            ) => lab.Cluster.Nodes[member]!.ExecuteAsync(principal, request, cancellationToken);
        }
    }
}
