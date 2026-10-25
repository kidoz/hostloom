using System.Collections.Concurrent;
using HostLoom.Conformance;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Locking;
using HostLoom.NodeLeasing;
using HostLoom.Redis;
using HostLoom.Valkey;
using ValkeyDotNet;
using Xunit;

namespace HostLoom.IntegrationTests;

[Collection(nameof(NodeLeasingBackendTests))]
[CollectionDefinition(nameof(NodeLeasingBackendTests), DisableParallelization = true)]
public sealed class NodeLeasingBackendTests
{
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(2);
    private const string RedisChaosSkip =
        "Set HOSTLOOM_REDIS_CHAOS=1 for an isolated Redis node-leasing outage.";
    private const string ValkeyChaosSkip =
        "Set HOSTLOOM_VALKEY_CHAOS=1 for an isolated Valkey node-leasing outage.";
    public static bool Redis => RedisAvailability.Redis;
    public static bool Valkey => ValkeyAvailability.Available;
    public static bool RedisChaos =>
        Redis && Environment.GetEnvironmentVariable("HOSTLOOM_REDIS_CHAOS") == "1";
    public static bool ValkeyChaos =>
        Valkey && Environment.GetEnvironmentVariable("HOSTLOOM_VALKEY_CHAOS") == "1";

    [Fact(Timeout = 60_000, Skip = RedisAvailability.Skip, SkipUnless = nameof(Redis))]
    public async Task Redis_allocates_distinct_ids_renews_and_hands_an_id_to_the_waiter() =>
        await AllocationAsync(false, TestContext.Current.CancellationToken);

    [Fact(Timeout = 60_000, Skip = ValkeyAvailability.Skip, SkipUnless = nameof(Valkey))]
    public async Task Valkey_allocates_distinct_ids_renews_and_hands_an_id_to_the_waiter() =>
        await AllocationAsync(true, TestContext.Current.CancellationToken);

    [Fact(Timeout = 60_000, Skip = RedisChaosSkip, SkipUnless = nameof(RedisChaos))]
    public async Task Redis_outage_ends_assignment_within_the_lease_and_recovers_on_the_same_elector() =>
        await OutageAsync(false, TestContext.Current.CancellationToken);

    [Fact(Timeout = 60_000, Skip = ValkeyChaosSkip, SkipUnless = nameof(ValkeyChaos))]
    public async Task Valkey_outage_ends_assignment_within_the_lease_and_recovers_on_the_same_elector() =>
        await OutageAsync(true, TestContext.Current.CancellationToken);

    private static async Task AllocationAsync(bool valkey, CancellationToken token)
    {
        var ns = "node-" + Guid.NewGuid().ToString("N");
        await using var first = Instance.Create(valkey, ns, poolSize: 2);
        await using var second = Instance.Create(valkey, ns, poolSize: 2);
        await first.Elector.StartAsync(token);
        var firstId = await first
            .Elector.WaitForAssignmentAsync(token)
            .AsTask()
            .WaitAsync(TimeSpan.FromSeconds(15), token);
        await second.Elector.StartAsync(token);
        var secondId = await second
            .Elector.WaitForAssignmentAsync(token)
            .AsTask()
            .WaitAsync(TimeSpan.FromSeconds(15), token);
        Assert.NotEqual(firstId, secondId);
        Assert.InRange(firstId, 0, 1);
        Assert.InRange(secondId, 0, 1);
        var oldToken = first.Elector.AssignmentToken;
        await using var waiting = Instance.Create(valkey, ns, poolSize: 2);
        await waiting.Elector.StartAsync(token);
        var assignment = waiting.Elector.WaitForAssignmentAsync(token).AsTask();
        await UntilAsync(() =>
            waiting.Provider.Acquisitions >= 2
            && first.Provider.Renewals >= 5
            && second.Provider.Renewals >= 5
        );
        Assert.False(assignment.IsCompleted);
        Assert.Null(waiting.Elector.NodeId);
        Assert.Equal(1, first.Elector.Term);
        Assert.Equal(1, second.Elector.Term);
        Assert.False(oldToken.IsCancellationRequested);
        Assert.Equal(firstId, first.Elector.NodeId);
        Assert.Equal(secondId, second.Elector.NodeId);
        await first.Elector.StopAsync(token);
        Assert.True(oldToken.IsCancellationRequested);
        Assert.Null(first.Elector.NodeId);
        Assert.Equal(firstId, await assignment.WaitAsync(TimeSpan.FromSeconds(15), token));
        Assert.NotEqual(second.Elector.NodeId, waiting.Elector.NodeId);
    }

    private static async Task OutageAsync(bool valkey, CancellationToken token)
    {
        var ns = "node-outage-" + Guid.NewGuid().ToString("N");
        await using var proxy = new TcpFaultProxy(
            "localhost",
            valkey ? ValkeyAvailability.Port : RedisAvailability.Port
        );
        await using var first = Instance.Create(valkey, ns, poolSize: 1, proxy.Port);
        await first.Elector.StartAsync(token);
        Assert.Equal(
            0,
            await first
                .Elector.WaitForAssignmentAsync(token)
                .AsTask()
                .WaitAsync(TimeSpan.FromSeconds(15), token)
        );
        var originalToken = first.Elector.AssignmentToken;
        await UntilAsync(() => first.Provider.Renewals >= 1);
        await using var second = Instance.Create(valkey, ns, poolSize: 1);
        await second.Elector.StartAsync(token);
        await UntilAsync(() => second.Provider.Acquisitions >= 1);
        Assert.Null(second.Elector.NodeId);
        proxy.SetEnabled(false);
        try
        {
            await UntilAsync(() =>
                first.Elector.NodeId is null && originalToken.IsCancellationRequested
            );
            Assert.Equal(
                0,
                await second
                    .Elector.WaitForAssignmentAsync(token)
                    .AsTask()
                    .WaitAsync(TimeSpan.FromSeconds(15), token)
            );
            // State/token updates precede release and change notification. Await the journal
            // explicitly rather than assuming a waiter resumes after all observers ran.
            await UntilAsync(() =>
                first.Changes.Any(entry => entry.Change.Reason == NodeLeaseChangeReason.Lost)
                && second.Changes.Any(entry =>
                    entry.Change.Reason == NodeLeaseChangeReason.Acquired
                )
            );
            var lost = Assert.Single(
                first.Changes,
                entry => entry.Change.Reason == NodeLeaseChangeReason.Lost
            );
            var acquired = Assert.Single(
                second.Changes,
                entry => entry.Change.Reason == NodeLeaseChangeReason.Acquired
            );
            Assert.True(
                lost.At - acquired.At < Lease,
                $"Assignment belief overlap exceeded {Lease}: lost={lost.At:O}, acquired={acquired.At:O}."
            );
        }
        finally
        {
            proxy.SetEnabled(true);
            first.Dump();
            second.Dump();
        }
        var renewals = second.Provider.Renewals;
        await UntilAsync(() => second.Provider.Renewals >= renewals + 2 && first.Provider.Healthy);
        Assert.Null(first.Elector.NodeId);
        Assert.Equal(1, first.Elector.Term);
        Assert.Equal(0, second.Elector.NodeId);
        await second.Elector.StopAsync(token);
        Assert.Equal(
            0,
            await first
                .Elector.WaitForAssignmentAsync(token)
                .AsTask()
                .WaitAsync(TimeSpan.FromSeconds(15), token)
        );
        Assert.Equal(2, first.Elector.Term);
        Assert.True(originalToken.IsCancellationRequested);
        Assert.False(first.Elector.AssignmentToken.IsCancellationRequested);
    }

    private static Task UntilAsync(Func<bool> condition) =>
        CacheConformance.WaitUntilAsync(() => Task.FromResult(condition()), 20);

    private sealed class ObservedProvider(ILockProvider inner) : ILockProvider
    {
        private int _acquisitions;
        private int _renewals;
        private int _healthy;
        public int Acquisitions => Volatile.Read(ref _acquisitions);
        public int Renewals => Volatile.Read(ref _renewals);
        public bool Healthy => Volatile.Read(ref _healthy) == 1;

        public async ValueTask<bool> TryAcquireAsync(
            string key,
            string owner,
            TimeSpan lease,
            CancellationToken cancellationToken = default
        )
        {
            Interlocked.Increment(ref _acquisitions);
            try
            {
                var result = await inner.TryAcquireAsync(key, owner, lease, cancellationToken);
                Volatile.Write(ref _healthy, 1);
                return result;
            }
            catch
            {
                Volatile.Write(ref _healthy, 0);
                throw;
            }
        }

        public ValueTask<bool> ReleaseAsync(
            string key,
            string owner,
            CancellationToken cancellationToken = default
        ) => inner.ReleaseAsync(key, owner, cancellationToken);

        public async ValueTask<bool> ExtendAsync(
            string key,
            string owner,
            TimeSpan lease,
            CancellationToken cancellationToken = default
        )
        {
            var result = await inner.ExtendAsync(key, owner, lease, cancellationToken);
            if (result)
                Interlocked.Increment(ref _renewals);
            return result;
        }
    }

    private sealed class Instance : IAsyncDisposable
    {
        private readonly IAsyncDisposable _connection;
        private readonly ILockProvider _backend;
        private readonly DistributedLock _locks;
        private readonly IDisposable _subscription;
        private readonly ConcurrentQueue<(NodeLeaseChange Change, DateTimeOffset At)> _changes =
            new();
        public NodeIdElector Elector { get; }
        public ObservedProvider Provider { get; }
        public (NodeLeaseChange Change, DateTimeOffset At)[] Changes => _changes.ToArray();

        private Instance(
            IAsyncDisposable connection,
            ILockProvider backend,
            string ns,
            int poolSize
        )
        {
            _connection = connection;
            _backend = backend;
            Provider = new ObservedProvider(backend);
            _locks = new DistributedLock(
                new LockingOptions { Namespace = ns, DefaultLease = Lease },
                Provider
            );
            Elector = new NodeIdElector(
                "orders-workers",
                _locks,
                new NodeLeasingOptions
                {
                    PoolSize = poolSize,
                    Lease = Lease,
                    RenewInterval = TimeSpan.FromMilliseconds(500),
                    RetryInterval = TimeSpan.FromMilliseconds(200),
                    RetryJitter = TimeSpan.Zero,
                }
            );
            _subscription = Elector.OnChange(change =>
                _changes.Enqueue((change, DateTimeOffset.UtcNow))
            );
        }

        public static Instance Create(bool valkey, string ns, int poolSize, int? port = null)
        {
#pragma warning disable CA2000 // Connection, backend and lock are owned by Instance.
            if (valkey)
            {
                var connection = new ValkeyConnection(
                    new ValkeyOptions
                    {
                        Connection = new ValkeyClientOptions
                        {
                            Host = "localhost",
                            Port = port ?? ValkeyAvailability.Port,
                        },
                        CommandTimeout = TimeSpan.FromSeconds(1),
                        HealthTimeout = TimeSpan.FromMilliseconds(300),
                    }
                );
                return new Instance(connection, new ValkeyLockProvider(connection), ns, poolSize);
            }
            var redis = new RedisConnection(
                new RedisOptions
                {
                    Configuration = "localhost:" + (port ?? RedisAvailability.Port),
                    ConnectTimeout = TimeSpan.FromSeconds(1),
                    CommandTimeout = TimeSpan.FromSeconds(1),
                    HealthTimeout = TimeSpan.FromMilliseconds(300),
                }
            );
            return new Instance(redis, new RedisLockProvider(redis), ns, poolSize);
#pragma warning restore CA2000
        }

        public void Dump() =>
            TestContext.Current.TestOutputHelper!.WriteLine(
                string.Join(Environment.NewLine, Changes.Select(entry => entry.ToString()))
            );

        public async ValueTask DisposeAsync()
        {
            await Elector.DisposeAsync();
            _subscription.Dispose();
            await _locks.DisposeAsync();
            if (_backend is IAsyncDisposable disposable)
                await disposable.DisposeAsync();
            await _connection.DisposeAsync();
        }
    }
}
