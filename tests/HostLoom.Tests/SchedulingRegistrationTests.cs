using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using HostLoom.Locking.Testing;
using HostLoom.Scheduling;
using HostLoom.Scheduling.DependencyInjection;
using HostLoom.Scheduling.Locking;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace HostLoom.Tests;

public sealed class SchedulingRegistrationTests
{
    private static readonly TimeSpan TenSeconds = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task AddSchedule_resolves_the_job_from_a_fresh_scope_for_every_run()
    {
        var clock = new TestClock();
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(clock);
        services.AddSingleton<RunLog>();
        services.AddScoped<ScopedMarker>();
        services
            .AddHostLoomScheduling()
            .AddSchedule<MarkerJob>("mark", ScheduleTrigger.FixedRate(TenSeconds));
        await using var provider = services.BuildServiceProvider();
        var scheduler = provider.GetRequiredService<Scheduler>();
        var log = provider.GetRequiredService<RunLog>();

        await scheduler.StartAsync(TestContext.Current.CancellationToken);
        await log.WaitForRunsAsync(1);
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        clock.Advance(TenSeconds);
        await log.WaitForRunsAsync(2);

        Assert.Equal(2, log.Markers.Count);
        Assert.NotSame(log.Markers[0], log.Markers[1]);
        Assert.All(log.Markers, marker => Assert.True(marker.Disposed));
        Assert.Same(scheduler, provider.GetRequiredService<Scheduler>());
        Assert.Single(provider.GetServices<IHostedService>());
    }

    [Fact]
    public async Task The_delegate_overload_receives_the_root_provider()
    {
        var clock = new TestClock();
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(clock);
        services.AddSingleton<RunLog>();
        services
            .AddHostLoomScheduling()
            .AddSchedule(
                "delegate",
                ScheduleTrigger.FixedRate(TenSeconds),
                static (provider, run, _) =>
                {
                    provider.GetRequiredService<RunLog>().Record(new ScopedMarker());
                    return ValueTask.CompletedTask;
                }
            );
        await using var provider = services.BuildServiceProvider();
        var scheduler = provider.GetRequiredService<Scheduler>();

        await scheduler.StartAsync(TestContext.Current.CancellationToken);
        await provider.GetRequiredService<RunLog>().WaitForRunsAsync(1);

        Assert.Equal(1, scheduler.GetState("delegate").Runs);
    }

    [Fact]
    public void A_repeated_schedule_name_is_refused_at_registration_even_across_builders()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoomScheduling()
            .AddSchedule<MarkerJob>("mark", ScheduleTrigger.FixedRate(TenSeconds));

        var again = services.AddHostLoomScheduling();
        var failure = Assert.Throws<InvalidOperationException>(() =>
            again.AddSchedule<MarkerJob>("mark", ScheduleTrigger.FixedRate(TenSeconds))
        );

        Assert.Contains("'mark'", failure.Message, StringComparison.Ordinal);
        Assert.Single(services, d => d.ServiceType == typeof(Scheduler));
        Assert.Single(services, d => d.ServiceType == typeof(ScheduleDefinition));
    }

    [Fact]
    public async Task An_exclusive_schedule_without_a_guard_fails_validation_naming_the_builder()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoomScheduling()
            .AddSchedule<MarkerJob>(
                "nightly",
                ScheduleTrigger.FixedRate(TenSeconds),
                options => options.Exclusive = true
            );
        await using var provider = services.BuildServiceProvider();

        var failure = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<IOptions<SchedulingOptions>>().Value
        );

        Assert.Contains("'nightly'", failure.Message, StringComparison.Ordinal);
        Assert.Contains("UseDistributedLock()", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Bad_options_and_a_second_guard_are_refused_naming_what_was_there()
    {
        var services = new ServiceCollection();
        var builder = services.AddHostLoomScheduling();

        var options = Assert.Throws<ArgumentException>(() =>
            builder.AddSchedule<MarkerJob>(
                "bad",
                ScheduleTrigger.FixedRate(TenSeconds),
                o => o.Timeout = TimeSpan.Zero
            )
        );
        Assert.Contains("Timeout must be positive", options.Message, StringComparison.Ordinal);

        builder.UseDistributedLock();
        var guard = Assert.Throws<InvalidOperationException>(() =>
            builder.UseGuard<DistributedLockScheduleGuard>("Other")
        );
        Assert.Contains("DistributedLock", guard.Message, StringComparison.Ordinal);
        Assert.Contains("Other", guard.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UseDistributedLock_claims_through_the_registered_lock_and_skips_while_held()
    {
        var clock = new TestClock();
        var lockProvider = new ManualLockProvider(clock);
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(clock);
        services.AddSingleton(lockProvider);
        services.AddSingleton<RunLog>();
        services.AddScoped<ScopedMarker>();
        services
            .AddHostLoomLocking(locking => locking.Namespace = "orders")
            .UseProvider<ManualLockProvider>("Manual");
        services
            .AddHostLoomScheduling(scheduling => scheduling.DefaultLease = TimeSpan.FromMinutes(1))
            .UseDistributedLock()
            .AddSchedule<MarkerJob>(
                "nightly",
                ScheduleTrigger.FixedRate(TenSeconds),
                options => options.Exclusive = true
            );
        await using var provider = services.BuildServiceProvider();
        var scheduler = provider.GetRequiredService<Scheduler>();
        var log = provider.GetRequiredService<RunLog>();
        Assert.IsType<DistributedLockScheduleGuard>(scheduler.Guard);
        Assert.True(
            await lockProvider.TryAcquireAsync(
                "orders:lock:schedule:nightly",
                ManualLockProvider.TestOwner,
                TimeSpan.FromHours(1),
                TestContext.Current.CancellationToken
            )
        );

        await scheduler.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => scheduler.GetState("nightly").Runs == 1);
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        Assert.Equal(ScheduleRunOutcome.Skipped, scheduler.GetState("nightly").LastOutcome);
        Assert.Empty(log.Markers);

        Assert.True(
            await lockProvider.ReleaseAsync(
                "orders:lock:schedule:nightly",
                ManualLockProvider.TestOwner,
                TestContext.Current.CancellationToken
            )
        );
        clock.Advance(TenSeconds);
        await log.WaitForRunsAsync(1);
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);

        Assert.Equal(ScheduleRunOutcome.Succeeded, scheduler.GetState("nightly").LastOutcome);
        Assert.Equal(0, lockProvider.Count);
        Assert.Equal("schedule:nightly", DistributedLockScheduleGuard.KeyFor("nightly"));
    }

    [Fact]
    public async Task The_hosted_service_starts_and_stops_the_scheduler_with_the_host()
    {
        var clock = new TestClock();
        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton<TimeProvider>(clock);
                services.AddSingleton<RunLog>();
                services.AddScoped<ScopedMarker>();
                services
                    .AddHostLoomScheduling()
                    .AddSchedule<MarkerJob>("mark", ScheduleTrigger.FixedRate(TenSeconds));
            })
            .Build();
        var log = host.Services.GetRequiredService<RunLog>();
        var scheduler = host.Services.GetRequiredService<Scheduler>();

        await host.StartAsync(TestContext.Current.CancellationToken);
        await log.WaitForRunsAsync(1);
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        Assert.NotNull(scheduler.GetState("mark").NextDue);

        await host.StopAsync(TestContext.Current.CancellationToken);

        Assert.Null(scheduler.GetState("mark").NextDue);
        Assert.Equal(0, clock.PendingTimers);
    }

    internal sealed class ScopedMarker : IDisposable
    {
        public bool Disposed { get; private set; }

        public void Dispose() => Disposed = true;
    }

    internal sealed class RunLog
    {
        private readonly SemaphoreSlim _signal = new(0);
        private readonly List<ScopedMarker> _markers = [];

        public IReadOnlyList<ScopedMarker> Markers
        {
            get
            {
                lock (_markers)
                {
                    return [.. _markers];
                }
            }
        }

        public void Record(ScopedMarker marker)
        {
            lock (_markers)
            {
                _markers.Add(marker);
            }

            _signal.Release();
        }

        public async Task WaitForRunsAsync(int count)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                timeout.Token,
                TestContext.Current.CancellationToken
            );
            while (Markers.Count < count)
            {
                await _signal.WaitAsync(linked.Token);
            }
        }
    }

    internal sealed class MarkerJob(ScopedMarker marker, RunLog log) : IScheduledJob
    {
        public ValueTask ExecuteAsync(ScheduledRun run, CancellationToken cancellationToken)
        {
            log.Record(marker);
            return ValueTask.CompletedTask;
        }
    }
}
