using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace HostLoom.Tests;

public sealed class LockingRegistrationTests
{
    [Fact]
    public async Task AddHostLoomLocking_with_UseInMemory_resolves_a_working_lock()
    {
        var services = new ServiceCollection();
        services.AddHostLoomLocking(locking => locking.Namespace = "orders").UseInMemory();
        await using var provider = services.BuildServiceProvider();

        var locks = provider.GetRequiredService<IDistributedLock>();
        var result = await locks.ExecuteWithLockAsync(
            "k",
            _ => ValueTask.FromResult("ok"),
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Equal("ok", result);
        Assert.Same(locks, provider.GetRequiredService<IDistributedLock>());
        Assert.IsType<InMemoryLockProvider>(provider.GetRequiredService<ILockProvider>());
        var description = LockingProbe.Describe(locks);
        Assert.Equal("orders", description.Namespace);
        Assert.Equal(nameof(InMemoryLockProvider), description.Provider);
    }

    [Fact]
    public void A_second_provider_throws_naming_the_first()
    {
        var services = new ServiceCollection();
        var builder = services.AddHostLoomLocking(locking => locking.Namespace = "orders");
        builder.UseInMemory();

        var failure = Assert.Throws<InvalidOperationException>(() =>
            builder.UseProvider<ProbingLockProvider>("Redis")
        );

        Assert.Contains("InMemory", failure.Message, StringComparison.Ordinal);
        Assert.Contains("Redis", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Repeated_registration_shares_one_registration_state()
    {
        var services = new ServiceCollection();
        services.AddHostLoomLocking(locking => locking.Namespace = "orders").UseInMemory();

        var again = services.AddHostLoomLocking();
        var failure = Assert.Throws<InvalidOperationException>(() => again.UseInMemory());

        Assert.Contains("InMemory", failure.Message, StringComparison.Ordinal);
        Assert.Single(services, d => d.ServiceType == typeof(IDistributedLock));
        Assert.Single(services, d => d.ServiceType == typeof(ILockProvider));
    }

    [Fact]
    public async Task Options_are_validated_with_messages_naming_the_option_key()
    {
        var services = new ServiceCollection();
        services.AddHostLoomLocking(locking => locking.Namespace = "Not Valid").UseInMemory();
        await using var provider = services.BuildServiceProvider();

        var failure = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<IOptions<LockingOptions>>().Value
        );

        Assert.Contains("Locking:Namespace", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task An_enabled_lock_without_a_provider_fails_validation_naming_the_builder()
    {
        var services = new ServiceCollection();
        services.AddHostLoomLocking(locking => locking.Namespace = "orders");
        await using var provider = services.BuildServiceProvider();

        var failure = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<IDistributedLock>()
        );

        Assert.Contains("UseInMemory()", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_disabled_lock_needs_no_provider()
    {
        var services = new ServiceCollection();
        services.AddHostLoomLocking(locking =>
        {
            locking.Namespace = "orders";
            locking.Enabled = false;
        });
        await using var provider = services.BuildServiceProvider();

        var locks = provider.GetRequiredService<IDistributedLock>();
        var result = await locks.ExecuteWithLockAsync(
            "k",
            _ => ValueTask.FromResult(1),
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Equal(1, result);
        Assert.Equal("(disabled)", LockingProbe.Describe(locks).Provider);
    }

    [Fact]
    public async Task ValidateOnStart_fails_the_host_before_anything_runs()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddHostLoomLocking(locking => locking.Namespace = "BAD").UseInMemory();
        using var host = builder.Build();

        var failure = await Assert.ThrowsAsync<OptionsValidationException>(() =>
            host.StartAsync(TestContext.Current.CancellationToken)
        );

        Assert.Contains("Locking:Namespace", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Readiness_is_healthy_without_a_probe_and_follows_the_probe_when_present()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services
            .AddHostLoomLocking(locking => locking.Namespace = "orders")
            .UseInMemory()
            .AddHealthChecks();
        await using var provider = services.BuildServiceProvider();
        var health = provider.GetRequiredService<HealthCheckService>();

        var ready = await health.CheckHealthAsync(
            r => r.Tags.Contains("ready"),
            TestContext.Current.CancellationToken
        );

        Assert.Equal(HealthStatus.Healthy, ready.Status);
        var entry = Assert.Single(ready.Entries);
        Assert.Equal("hostloom-lock-ready", entry.Key);
        Assert.Contains(
            "does not report health",
            entry.Value.Description,
            StringComparison.Ordinal
        );

        var probing = new ServiceCollection();
        probing.AddLogging();
        probing.AddSingleton(TimeProvider.System);
        probing
            .AddHostLoomLocking(locking => locking.Namespace = "orders")
            .UseProvider<ProbingLockProvider>("Probing")
            .AddHealthChecks();
        await using var probingProvider = probing.BuildServiceProvider();
        probingProvider.GetRequiredService<ProbingLockProvider>().Health =
            LockProviderHealth.Unhealthy("backend down");

        var unhealthy = await probingProvider
            .GetRequiredService<HealthCheckService>()
            .CheckHealthAsync(r => r.Tags.Contains("ready"), TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Unhealthy, unhealthy.Status);
        Assert.Equal("backend down", Assert.Single(unhealthy.Entries).Value.Description);
        Assert.Same(
            probingProvider.GetRequiredService<ILockProvider>(),
            probingProvider.GetRequiredService<ILockProviderHealthProbe>()
        );
    }

    [Fact]
    public async Task A_registered_TimeProvider_is_used_because_every_registration_is_TryAdd()
    {
        var clock = new TestClock();
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(clock);
        services.AddHostLoomLocking(locking => locking.Namespace = "orders").UseInMemory();
        await using var provider = services.BuildServiceProvider();

        await using var handle = await provider
            .GetRequiredService<IDistributedLock>()
            .TryAcquireAsync("k", cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(handle);
        Assert.Equal(DateTimeOffset.UnixEpoch + TimeSpan.FromSeconds(30), handle.LeaseEnd);
    }

    [Fact]
    public async Task Container_free_and_container_built_compositions_behave_the_same()
    {
        var clock = new TestClock();
        var shared = new InMemoryLockProvider(clock);
        await using var direct = new DistributedLock(
            new LockingOptions { Namespace = "orders" },
            shared,
            clock
        );
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(clock);
        services.AddSingleton(shared);
        services.AddHostLoomLocking(locking => locking.Namespace = "orders").UseInMemory();
        await using var provider = services.BuildServiceProvider();
        var built = provider.GetRequiredService<IDistributedLock>();

        await using var held = await direct.TryAcquireAsync(
            "k",
            cancellationToken: TestContext.Current.CancellationToken
        );
        var contended = await built.TryAcquireAsync(
            "k",
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.NotNull(held);
        Assert.Null(contended);
        Assert.Equal(LockingProbe.Describe(direct).Lines, LockingProbe.Describe(built).Lines);
    }
}
