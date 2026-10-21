using System.Collections.Concurrent;
using HostLoom.Leadership;
using HostLoom.Leadership.DependencyInjection;
using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NSubstitute;
using Xunit;

namespace HostLoom.Tests;

public sealed class LeadershipKeyTests
{
    [Theory]
    [InlineData(null, "leader:{role}", "billing:lock:leader:scheduler")]
    [InlineData("leader", "leader:{role}", "billing:lock:leader")]
    [InlineData(null, "election:eu:{role}", "billing:lock:election:eu:scheduler")]
    public async Task The_elector_uses_the_selected_key_for_acquisition_renewal_and_release(
        string? lockKey,
        string lockKeyFormat,
        string expectedKey
    )
    {
        var clock = new TestClock();
        var backend = new LeadershipKeyTestProvider(new InMemoryLockProvider(clock));
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "billing" },
            backend,
            clock
        );
        var options = new LeadershipOptions { LockKey = lockKey, LockKeyFormat = lockKeyFormat };
        await using var elector = new LeaderElector("scheduler", locks, options, clock);

        await elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => elector.IsLeader);
        Assert.Equal(expectedKey, Assert.Single(backend.Acquired));
        // Live option mutation must not move an existing elector onto a different key.
        options.LockKey = "changed-after-construction";
        options.LockKeyFormat = "changed:{role}";
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers >= 3);
        clock.Advance(TimeSpan.FromSeconds(5));
        await SchedulingTests.WaitUntilAsync(() => !backend.Extended.IsEmpty);
        Assert.All(backend.Extended, key => Assert.Equal(expectedKey, key));

        await elector.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(expectedKey, Assert.Single(backend.Released));
        Assert.False(elector.IsLeader);
        Assert.Equal(expectedKey["billing:lock:".Length..], elector.LockKey);
        var probe = LeadershipProbe.Describe(elector);
        Assert.Equal(elector.LockKey, probe.LockKey);
        Assert.Contains(
            probe.Lines,
            line => line.Contains($"LockKey = {elector.LockKey}", StringComparison.Ordinal)
        );
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("has space")]
    [InlineData("has\tcontrol")]
    [InlineData("has\u0000control")]
    public async Task Invalid_custom_keys_fail_validation_before_election(string lockKey)
    {
        var options = new LeadershipOptions { LockKey = lockKey };
        Assert.Contains(
            options.Validate(),
            problem => problem.Contains("Leadership:LockKey", StringComparison.Ordinal)
        );
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "billing" },
            new InMemoryLockProvider()
        );

        var failure = Assert.Throws<ArgumentException>(() =>
            new LeaderElector("scheduler", locks, options)
        );

        Assert.Contains("Leadership:LockKey", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Custom_key_length_uses_the_default_lock_key_limit()
    {
        Assert.Empty(
            new LeadershipOptions { LockKey = new string('a', 512) }.Validate("scheduler")
        );
        Assert.Contains(
            new LeadershipOptions { LockKey = new string('a', 513) }.Validate("scheduler"),
            problem => problem.Contains("Leadership:LockKey", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task A_custom_key_does_not_bypass_role_validation()
    {
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "billing" },
            new InMemoryLockProvider()
        );

        Assert.Throws<ArgumentException>(() =>
            new LeaderElector("has space", locks, new LeadershipOptions { LockKey = "leader" })
        );
    }

    [Fact]
    public async Task Named_options_report_the_role_and_invalid_key()
    {
        var services = new ServiceCollection();
        services.AddHostLoomLocking(locking => locking.Namespace = "billing").UseInMemory();
        services
            .AddHostLoomLeadership()
            .AddRole("scheduler", options => options.LockKey = "has space");
        await using var provider = services.BuildServiceProvider();

        var failure = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<IOptionsMonitor<LeadershipOptions>>().Get("scheduler")
        );

        Assert.Contains("Role 'scheduler'", failure.Message, StringComparison.Ordinal);
        Assert.Contains("Leadership:LockKey", failure.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("election")]
    [InlineData("election:{Role}")]
    [InlineData("election: {role}")]
    [InlineData("election:\u0000{role}")]
    public void Invalid_patterns_report_the_option_before_election(string? format)
    {
        var options = new LeadershipOptions { LockKeyFormat = format! };

        Assert.Contains(
            options.Validate(),
            problem => problem.Contains("Leadership:LockKeyFormat", StringComparison.Ordinal)
        );
        Assert.Contains(
            options.Validate("scheduler"),
            problem => problem.Contains("Leadership:LockKeyFormat", StringComparison.Ordinal)
        );
        Assert.Throws<ArgumentException>(() => options.ResolveLockKey("scheduler"));
    }

    [Theory]
    [InlineData("election:eu:{role}", "election:eu:Scheduler")]
    [InlineData("{role}:active:{role}", "Scheduler:active:Scheduler")]
    [InlineData("{elections}:{role}", "{elections}:Scheduler")]
    [InlineData("{{role}}:active", "{Scheduler}:active")]
    public void Patterns_replace_only_the_role_placeholder_and_preserve_case_and_hash_tags(
        string format,
        string expected
    )
    {
        Assert.Equal(
            expected,
            new LeadershipOptions { LockKeyFormat = format }.ResolveLockKey("Scheduler")
        );
    }

    [Fact]
    public void An_exact_key_overrides_the_pattern_without_placeholder_expansion()
    {
        var options = new LeadershipOptions { LockKey = "exact:{role}", LockKeyFormat = null! };

        Assert.Empty(options.Validate("scheduler"));
        Assert.Equal("exact:{role}", options.ResolveLockKey("scheduler"));
        Assert.Throws<ArgumentException>(() => options.ResolveLockKey("has space"));
    }

    [Theory]
    [InlineData(null, "leader:{role}")]
    [InlineData(null, "{role}")]
    [InlineData("election:scheduler", "leader:{role}")]
    [InlineData(null, "election:eu:{role}")]
    public async Task Resolved_keys_exceeding_the_lock_limit_fail_at_construction_and_host_startup(
        string? lockKey,
        string format
    )
    {
        var backend = new LeadershipKeyTestProvider(new InMemoryLockProvider());
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "billing", MaxKeyLength = 8 },
            backend
        );
        var options = new LeadershipOptions { LockKey = lockKey, LockKeyFormat = format };

        var failure = Assert.Throws<ArgumentException>(() =>
            new LeaderElector("scheduler", locks, options)
        );
        Assert.Contains("Locking:MaxKeyLength", failure.Message, StringComparison.Ordinal);
        Assert.Empty(backend.Acquired);

        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services
                    .AddHostLoomLocking(o =>
                    {
                        o.Namespace = "billing";
                        o.MaxKeyLength = 8;
                    })
                    .UseInMemory();
                services
                    .AddHostLoomLeadership()
                    .AddRole(
                        "scheduler",
                        o =>
                        {
                            o.LockKey = lockKey;
                            o.LockKeyFormat = format;
                        }
                    );
            })
            .Build();

        var startupFailure = await Assert.ThrowsAsync<OptionsValidationException>(() =>
            host.StartAsync(TestContext.Current.CancellationToken)
        );
        Assert.Contains("Role 'scheduler'", startupFailure.Message, StringComparison.Ordinal);
        Assert.Contains("Locking:MaxKeyLength", startupFailure.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Larger_configured_limits_allow_long_exact_and_expanded_keys(bool useFormat)
    {
        var prefix = new string('a', 600);
        var options = new LeadershipOptions
        {
            LockKey = useFormat ? null : prefix,
            LockKeyFormat = prefix + ":{role}",
        };
        Assert.Empty(options.Validate());
        Assert.Empty(options.Validate("scheduler", 1024));
        var clock = new TestClock();
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "billing", MaxKeyLength = 1024 },
            new InMemoryLockProvider(clock),
            clock
        );
        await using var elector = new LeaderElector("scheduler", locks, options, clock);
        await elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => elector.IsLeader);
        Assert.Equal(useFormat ? prefix + ":scheduler" : prefix, elector.LockKey);

        var services = new ServiceCollection();
        services
            .AddHostLoomLocking(o =>
            {
                o.Namespace = "billing";
                o.MaxKeyLength = 1024;
            })
            .UseInMemory();
        services
            .AddHostLoomLeadership()
            .AddRole(
                "scheduler",
                o =>
                {
                    o.LockKey = options.LockKey;
                    o.LockKeyFormat = options.LockKeyFormat;
                }
            );
        await using var provider = services.BuildServiceProvider();
        Assert.Equal(
            elector.LockKey,
            provider.GetRequiredKeyedService<LeaderElector>("scheduler").LockKey
        );
    }

    [Fact]
    public async Task Shared_configuration_formats_all_roles_and_named_options_can_override_it()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(
                new Dictionary<string, string?>
                {
                    ["Leadership:LockKeyFormat"] = "election:eu:{role}",
                    ["Reconciler:LockKey"] = "legacy:reconciler",
                }
            )
            .Build();
        var services = new ServiceCollection();
        services.AddHostLoomLocking(o => o.Namespace = "billing").UseInMemory();
        services.ConfigureAll<LeadershipOptions>(o =>
            configuration.GetSection("Leadership").Bind(o)
        );
        services.Configure<LeadershipOptions>("reconciler", configuration.GetSection("Reconciler"));
        services
            .AddHostLoomLeadership()
            .AddRole("scheduler")
            .AddRole("compactor")
            .AddRole("reconciler");
        await using var provider = services.BuildServiceProvider();

        Assert.Equal(
            "election:eu:scheduler",
            provider.GetRequiredKeyedService<LeaderElector>("scheduler").LockKey
        );
        Assert.Equal(
            "election:eu:compactor",
            provider.GetRequiredKeyedService<LeaderElector>("compactor").LockKey
        );
        Assert.Equal(
            "legacy:reconciler",
            provider.GetRequiredKeyedService<LeaderElector>("reconciler").LockKey
        );
    }

    [Fact]
    public async Task A_custom_lock_retains_control_of_its_own_key_length_limit()
    {
        var locks = Substitute.For<IDistributedLock>();
        var key = new string('a', 600);
        await using var elector = new LeaderElector(
            "scheduler",
            locks,
            new LeadershipOptions { LockKey = key }
        );
        Assert.Equal(key, elector.LockKey);

        var services = new ServiceCollection();
        services.AddSingleton(locks);
        services.AddHostLoomLeadership().AddRole("scheduler", options => options.LockKey = key);
        await using var provider = services.BuildServiceProvider();

        Assert.Equal(key, provider.GetRequiredKeyedService<LeaderElector>("scheduler").LockKey);
    }

    [Theory]
    [InlineData("election:shared", false)]
    [InlineData("election:other", true)]
    public async Task Electors_contend_by_resolved_key_and_distinct_keys_lead_independently(
        string secondKey,
        bool independent
    )
    {
        var clock = new TestClock();
        var backend = new LeadershipKeyTestProvider(new InMemoryLockProvider(clock));
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "billing" },
            backend,
            clock
        );
        await using var first = new LeaderElector(
            "scheduler",
            locks,
            new LeadershipOptions { LockKey = "election:shared", RetryJitter = TimeSpan.Zero },
            clock
        );
        await using var second = new LeaderElector(
            "reconciler",
            locks,
            new LeadershipOptions { LockKey = secondKey, RetryJitter = TimeSpan.Zero },
            clock
        );
        await first.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => first.IsLeader && clock.PendingTimers >= 3);
        await second.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() =>
            backend.Acquired.Count == 2 && clock.PendingTimers >= (independent ? 6 : 4)
        );

        Assert.Equal(independent, second.IsLeader);
        await first.StopAsync(TestContext.Current.CancellationToken);
        if (!independent)
        {
            clock.Advance(TimeSpan.FromSeconds(2));
            await SchedulingTests.WaitUntilAsync(() => second.IsLeader);
        }
        Assert.True(second.IsLeader);
    }

    [Fact]
    public async Task Options_mutation_before_start_or_reacquisition_does_not_change_the_captured_key()
    {
        var clock = new TestClock();
        var backend = new LeadershipKeyTestProvider(new InMemoryLockProvider(clock));
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "billing" },
            backend,
            clock
        );
        var options = new LeadershipOptions
        {
            LockKeyFormat = "election:{role}",
            RetryJitter = TimeSpan.Zero,
        };
        await using var elector = new LeaderElector("scheduler", locks, options, clock);
        options.LockKey = "changed";
        options.LockKeyFormat = "changed:{role}";
        await elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => elector.IsLeader);
        await elector.ResignAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        clock.Advance(TimeSpan.FromSeconds(2));
        await SchedulingTests.WaitUntilAsync(() => elector.IsLeader);

        Assert.Equal(2, backend.Acquired.Count);
        Assert.All(backend.Acquired, key => Assert.Equal("billing:lock:election:scheduler", key));
        Assert.Equal("election:scheduler", LeadershipProbe.Describe(elector).LockKey);
    }
}

internal sealed class LeadershipKeyTestProvider(ILockProvider backend) : ILockProvider
{
    public ConcurrentQueue<string> Acquired { get; } = new();
    public ConcurrentQueue<string> Extended { get; } = new();
    public ConcurrentQueue<string> Released { get; } = new();

    public ValueTask<bool> TryAcquireAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken
    )
    {
        Acquired.Enqueue(key);
        return backend.TryAcquireAsync(key, owner, lease, cancellationToken);
    }

    public ValueTask<bool> ExtendAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken
    )
    {
        Extended.Enqueue(key);
        return backend.ExtendAsync(key, owner, lease, cancellationToken);
    }

    public ValueTask<bool> ReleaseAsync(
        string key,
        string owner,
        CancellationToken cancellationToken
    )
    {
        Released.Enqueue(key);
        return backend.ReleaseAsync(key, owner, cancellationToken);
    }
}
