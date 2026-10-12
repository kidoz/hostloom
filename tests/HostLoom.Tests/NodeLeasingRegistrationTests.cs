using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using HostLoom.NodeLeasing;
using HostLoom.NodeLeasing.DependencyInjection;
using HostLoom.NodeLeasing.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit;

namespace HostLoom.Tests;

public sealed class NodeLeasingRegistrationTests
{
    private static readonly TimeSpan Renew = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan Retry = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task AddPool_registers_a_keyed_elector_and_the_single_pool_resolves_unkeyed()
    {
        var clock = new TestClock();
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(clock);
        services.AddHostLoomLocking(locking => locking.Namespace = "orders").UseInMemory();
        services
            .AddHostLoomNodeLeasing()
            .AddPool("orders-workers", leasing => leasing.Lease = TimeSpan.FromSeconds(20));
        await using var provider = services.BuildServiceProvider();

        var keyed = provider.GetRequiredKeyedService<INodeLeasing>("orders-workers");
        var elector = provider.GetRequiredKeyedService<NodeIdElector>("orders-workers");

        Assert.Same(keyed, elector);
        Assert.Same(keyed, provider.GetRequiredService<INodeLeasing>());
        Assert.Equal(TimeSpan.FromSeconds(20), elector.Options.Lease);
        Assert.Equal(NodeLeasingStatus.Idle, elector.Status);
    }

    [Fact]
    public async Task Several_pools_need_a_key_and_a_repeated_pool_is_refused()
    {
        var services = new ServiceCollection();
        services.AddHostLoomLocking(locking => locking.Namespace = "orders").UseInMemory();
        var builder = services
            .AddHostLoomNodeLeasing()
            .AddPool("orders-workers")
            .AddPool("inventory-workers");
        await using var provider = services.BuildServiceProvider();

        var failure = Assert.Throws<InvalidOperationException>(() =>
            provider.GetRequiredService<INodeLeasing>()
        );
        Assert.Contains("orders-workers", failure.Message, StringComparison.Ordinal);
        Assert.Contains("inventory-workers", failure.Message, StringComparison.Ordinal);
        Assert.NotSame(
            provider.GetRequiredKeyedService<INodeLeasing>("orders-workers"),
            provider.GetRequiredKeyedService<INodeLeasing>("inventory-workers")
        );
        var repeated = Assert.Throws<InvalidOperationException>(() =>
            builder.AddPool("orders-workers")
        );
        Assert.Contains("'orders-workers'", repeated.Message, StringComparison.Ordinal);
        Assert.Throws<InvalidOperationException>(() =>
            services.AddHostLoomNodeLeasing().AddPool("inventory-workers")
        );
    }

    [Fact]
    public async Task Options_are_validated_per_pool_naming_the_pool_and_the_key()
    {
        var services = new ServiceCollection();
        services.AddHostLoomLocking(locking => locking.Namespace = "orders").UseInMemory();
        services
            .AddHostLoomNodeLeasing()
            .AddPool("orders-workers", leasing => leasing.RenewInterval = leasing.Lease);
        await using var provider = services.BuildServiceProvider();

        var failure = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<IOptionsMonitor<NodeLeasingOptions>>().Get("orders-workers")
        );

        Assert.Contains("Pool 'orders-workers'", failure.Message, StringComparison.Ordinal);
        Assert.Contains("NodeLeasing:RenewInterval", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_pool_lease_beyond_the_locks_MaxLease_fails_naming_the_pool_and_both_values()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoomLocking(locking =>
            {
                locking.Namespace = "orders";
                locking.MaxLease = TimeSpan.FromMinutes(1);
            })
            .UseInMemory();
        services
            .AddHostLoomNodeLeasing()
            .AddPool("orders-workers", leasing => leasing.Lease = TimeSpan.FromMinutes(2))
            .AddPool("inventory-workers", leasing => leasing.Lease = TimeSpan.FromMinutes(1));
        await using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<NodeLeasingOptions>>();

        var failure = Assert.Throws<OptionsValidationException>(() =>
            monitor.Get("orders-workers")
        );

        Assert.Contains("Pool 'orders-workers'", failure.Message, StringComparison.Ordinal);
        Assert.Contains("NodeLeasing:Lease (00:02:00)", failure.Message, StringComparison.Ordinal);
        Assert.Contains("Locking:MaxLease (00:01:00)", failure.Message, StringComparison.Ordinal);
        // A lease equal to the cap is what the lock grants, so it passes.
        Assert.Equal(TimeSpan.FromMinutes(1), monitor.Get("inventory-workers").Lease);
    }

    [Fact]
    public async Task The_lease_check_fails_the_host_at_startup_and_skips_a_disabled_lock()
    {
        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services
                    .AddHostLoomLocking(locking =>
                    {
                        locking.Namespace = "orders";
                        locking.MaxLease = TimeSpan.FromSeconds(30);
                    })
                    .UseInMemory();
                services
                    .AddHostLoomNodeLeasing()
                    .AddPool("orders-workers", leasing => leasing.Lease = TimeSpan.FromMinutes(1));
            })
            .Build();

        var failure = await Assert.ThrowsAsync<OptionsValidationException>(() =>
            host.StartAsync(TestContext.Current.CancellationToken)
        );
        Assert.Contains("Pool 'orders-workers'", failure.Message, StringComparison.Ordinal);
        Assert.Contains("Locking:MaxLease (00:00:30)", failure.Message, StringComparison.Ordinal);

        // Single-instance mode takes no lease, so there is nothing for the lease to exceed.
        var services = new ServiceCollection();
        services
            .AddHostLoomLocking(locking =>
            {
                locking.Namespace = "orders";
                locking.Enabled = false;
                locking.MaxLease = TimeSpan.FromSeconds(30);
            })
            .UseInMemory();
        services
            .AddHostLoomNodeLeasing()
            .AddPool("orders-workers", leasing => leasing.Lease = TimeSpan.FromMinutes(1));
        await using var disabled = services.BuildServiceProvider();
        Assert.Equal(
            TimeSpan.FromMinutes(1),
            disabled
                .GetRequiredService<IOptionsMonitor<NodeLeasingOptions>>()
                .Get("orders-workers")
                .Lease
        );
    }

    [Fact]
    public async Task The_hosted_service_starts_electors_with_the_host_and_releases_on_stop()
    {
        var clock = new TestClock();
        var backend = new InMemoryLockProvider(clock);
        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton<TimeProvider>(clock);
                services.AddSingleton(backend);
                services
                    .AddHostLoomLocking(locking =>
                    {
                        locking.Namespace = "orders";
                        locking.DefaultLease = TimeSpan.FromSeconds(15);
                    })
                    .UseProvider<InMemoryLockProvider>("Shared");
                services
                    .AddHostLoomNodeLeasing()
                    .AddPool(
                        "orders-workers",
                        leasing =>
                        {
                            leasing.PoolSize = 2;
                            leasing.Lease = TimeSpan.FromSeconds(15);
                            leasing.RenewInterval = TimeSpan.FromSeconds(5);
                            leasing.RetryInterval = TimeSpan.FromSeconds(2);
                            leasing.RetryJitter = TimeSpan.Zero;
                        }
                    );
            })
            .Build();
        var elector = host.Services.GetRequiredKeyedService<NodeIdElector>("orders-workers");

        await host.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => elector.NodeId is not null);
        Assert.Equal(1, backend.Count);

        await host.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(NodeLeasingStatus.Stopped, elector.Status);
        Assert.Equal(0, backend.Count);
    }

    [Fact]
    public void Foreign_registrations_are_refused_and_named()
    {
        // CA2000: ownership of the doubles transfers to the service collections.
#pragma warning disable CA2000
        var unkeyed = new ServiceCollection();
        unkeyed.AddSingleton<INodeLeasing>(new ManualNodeLeasing("orders-workers"));
        var failure = Assert.Throws<InvalidOperationException>(() =>
            unkeyed.AddHostLoomNodeLeasing()
        );
        Assert.Contains("ManualNodeLeasing", failure.Message, StringComparison.Ordinal);

        var keyed = new ServiceCollection();
        var builder = keyed.AddHostLoomNodeLeasing();
        keyed.AddKeyedSingleton<INodeLeasing>(
            "orders-workers",
            new ManualNodeLeasing("orders-workers")
        );
        var refused = Assert.Throws<InvalidOperationException>(() =>
            builder.AddPool("orders-workers")
        );
        Assert.Contains("'orders-workers'", refused.Message, StringComparison.Ordinal);
        Assert.Contains("ManualNodeLeasing", refused.Message, StringComparison.Ordinal);
#pragma warning restore CA2000
    }

    [Fact]
    public async Task A_manual_node_leasing_drives_a_node_id_consumer()
    {
        using var leasing = new ManualNodeLeasing("orders-workers");
        var changes = new List<NodeLeaseChangeReason>();
        using var _ = leasing.OnChange(change => changes.Add(change.Reason));

        Assert.Null(leasing.NodeId);
        Assert.True(leasing.AssignmentToken.IsCancellationRequested);
        var waiting = leasing.WaitForAssignmentAsync(TestContext.Current.CancellationToken);
        leasing.Assign(3);
        Assert.Equal(3, await waiting);
        var token = leasing.AssignmentToken;
        Assert.False(token.IsCancellationRequested);

        leasing.Lose();

        Assert.True(token.IsCancellationRequested);
        Assert.Null(leasing.NodeId);
        Assert.Equal(1, leasing.Term);
        Assert.Equal([NodeLeaseChangeReason.Acquired, NodeLeaseChangeReason.Lost], changes);

        leasing.Assign(4);
        leasing.Release();
        Assert.Equal(2, leasing.Term);
        Assert.Equal(4, leasing.Changes.Count);
        Assert.Equal(
            [
                NodeLeaseChangeReason.Acquired,
                NodeLeaseChangeReason.Lost,
                NodeLeaseChangeReason.Acquired,
                NodeLeaseChangeReason.Released,
            ],
            changes
        );

        Assert.True(leasing.IsCoordinated);
        leasing.IsCoordinated = false;
        Assert.False(leasing.IsCoordinated);
    }

    [Fact]
    public async Task By_default_no_id_is_assigned_over_a_disabled_lock_and_the_warning_and_probe_say_why()
    {
        var clock = new TestClock();
        await using var locks = Disabled();
        var logger = new RecordingLogger<NodeIdElector>();
        await using var first = Elector(locks, clock, logger);
        await using var second = Elector(locks, clock);
        var changes = new List<NodeLeaseChange>();
        using var _ = first.OnChange(changes.Add);
        Assert.Equal(UncoordinatedNodeAssignment.None, new NodeLeasingOptions().WhenUncoordinated);
        Assert.False(first.IsCoordinated);

        await first.StartAsync(TestContext.Current.CancellationToken);
        await second.StartAsync(TestContext.Current.CancellationToken);
        // Each candidate refuses its placeholder and arms one retry timer; three rounds prove
        // it keeps doing so.
        for (var round = 0; round < 3; round++)
        {
            await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers >= 2);
            clock.Advance(Retry);
        }

        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers >= 2);

        Assert.Null(first.NodeId);
        Assert.Null(second.NodeId);
        Assert.Equal(NodeLeasingStatus.Candidate, first.Status);
        Assert.Equal(0, first.Term);
        Assert.True(first.AssignmentToken.IsCancellationRequested);
        Assert.Empty(changes);
        var warning = Assert.Single(
            logger.Entries,
            entry => entry.Event.Id == NodeLeasingEvents.Uncoordinated.Id
        );
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Contains("'orders-workers'", warning.Message, StringComparison.Ordinal);
        Assert.Contains("None", warning.Message, StringComparison.Ordinal);

        var probe = NodeLeasingProbe.Describe(first);
        Assert.False(probe.Coordinated);
        Assert.Equal(NodeLeasingStatus.Candidate, probe.Status);
        Assert.Null(probe.NodeId);
        Assert.Contains(
            probe.Lines,
            line => line.Contains("NodeLeasing:WhenUncoordinated = None", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task AssignZero_gives_every_instance_node_zero_without_coordination_and_says_so()
    {
        var clock = new TestClock();
        await using var locks = Disabled();
        var logger = new RecordingLogger<NodeIdElector>();
        await using var first = Elector(
            locks,
            clock,
            logger,
            UncoordinatedNodeAssignment.AssignZero
        );
        await using var second = Elector(
            locks,
            clock,
            when: UncoordinatedNodeAssignment.AssignZero
        );

        await first.StartAsync(TestContext.Current.CancellationToken);
        await second.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() =>
            first.NodeId is not null && second.NodeId is not null
        );

        Assert.Equal(0, first.NodeId ?? -1);
        Assert.Equal(0, second.NodeId ?? -1);
        Assert.Equal(1, first.Term);
        Assert.Equal(1, second.Term);
        Assert.False(first.IsCoordinated);
        Assert.False(first.AssignmentToken.IsCancellationRequested);
        var warning = Assert.Single(
            logger.Entries,
            entry => entry.Event.Id == NodeLeasingEvents.Uncoordinated.Id
        );
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Contains("AssignZero", warning.Message, StringComparison.Ordinal);
        var probe = NodeLeasingProbe.Describe(first);
        Assert.False(probe.Coordinated);
        Assert.Equal(NodeLeasingStatus.Assigned, probe.Status);
        Assert.Contains(
            probe.Lines,
            line =>
                line.Contains(
                    "NodeLeasing:WhenUncoordinated = AssignZero",
                    StringComparison.Ordinal
                )
        );

        // A placeholder renews for ever: two renewal periods later both still hold in term one,
        // and the warning was not repeated.
        for (var round = 0; round < 2; round++)
        {
            await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers >= 2);
            clock.Advance(Renew);
        }

        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers >= 2);
        Assert.Equal(0, first.NodeId ?? -1);
        Assert.Equal(0, second.NodeId ?? -1);
        Assert.Equal(1, first.Term);
        Assert.Single(
            logger.Entries,
            entry => entry.Event.Id == NodeLeasingEvents.Uncoordinated.Id
        );

        var token = first.AssignmentToken;
        await first.StopAsync(TestContext.Current.CancellationToken);
        Assert.Equal(NodeLeasingStatus.Stopped, first.Status);
        Assert.True(token.IsCancellationRequested);
    }

    private static DistributedLock Disabled() =>
        new(new LockingOptions { Namespace = "orders", Enabled = false }, provider: null);

    private static NodeIdElector Elector(
        IDistributedLock locks,
        TestClock clock,
        ILogger<NodeIdElector>? logger = null,
        UncoordinatedNodeAssignment when = UncoordinatedNodeAssignment.None
    ) =>
        new(
            "orders-workers",
            locks,
            new NodeLeasingOptions
            {
                PoolSize = 4,
                Lease = TimeSpan.FromSeconds(15),
                RenewInterval = Renew,
                RetryInterval = Retry,
                RetryJitter = TimeSpan.Zero,
                WhenUncoordinated = when,
            },
            clock,
            logger
        );
}
