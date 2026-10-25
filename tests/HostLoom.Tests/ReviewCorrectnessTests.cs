using System.Runtime.CompilerServices;
using HostLoom.Generators;
using HostLoom.Leadership;
using HostLoom.Locking;
using HostLoom.NodeLeasing;
using HostLoom.Pipelines;
using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using Xunit;

namespace HostLoom.Tests;

public sealed class ReviewCorrectnessTests
{
    [Theory]
    [InlineData(false, "stop")]
    [InlineData(true, "stop")]
    [InlineData(false, "dispose")]
    [InlineData(true, "dispose")]
    [InlineData(false, "before-start")]
    [InlineData(true, "before-start")]
    public async Task Terminal_electors_end_existing_and_future_waiters(bool node, string operation)
    {
        var locks = Substitute.For<IDistributedLock>();
        locks.IsCoordinated.Returns(true);
        locks
            .TryAcquireAsync(
                Arg.Any<string>(),
                Arg.Any<LockOptions>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(ValueTask.FromResult<ILockHandle?>(null));
        var clock = new TestClock();
        await using var leader = new LeaderElector(
            "catalog",
            locks,
            new LeadershipOptions(),
            clock
        );
        await using var leasing = new NodeIdElector(
            "catalog",
            locks,
            new NodeLeasingOptions { PoolSize = 1 },
            clock
        );
        Task Wait() =>
            node
                ? leasing.WaitForAssignmentAsync().AsTask()
                : leader.WaitForLeadershipAsync().AsTask();
        var pending = Wait();
        if (operation != "before-start")
        {
            if (node)
                await leasing.StartAsync(TestContext.Current.CancellationToken);
            else
                await leader.StartAsync(TestContext.Current.CancellationToken);
        }
        if (operation == "dispose")
        {
            if (node)
                await leasing.DisposeAsync();
            else
                await leader.DisposeAsync();
        }
        else
        {
            if (node)
                await leasing.StopAsync(TestContext.Current.CancellationToken);
            else
                await leader.StopAsync(TestContext.Current.CancellationToken);
        }
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            pending.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken)
        );
        if (operation == "dispose")
            await Assert.ThrowsAsync<ObjectDisposedException>(Wait);
        else
            await Assert.ThrowsAnyAsync<OperationCanceledException>(Wait);
    }

    [Fact]
    public void A_prior_singleton_event_handler_is_rejected_without_poisoning_registration()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Subscriber>();
        var builder = services.AddHostLoom().UseInMemory();
        var error = Assert.Throws<InvalidOperationException>(() =>
            builder.AddSubscriber<Placed, Subscriber>("orders", "audit")
        );
        Assert.Contains("singleton", error.Message, StringComparison.OrdinalIgnoreCase);
        services.RemoveAll<Subscriber>();
        services.AddScoped<Subscriber>();
        builder.AddSubscriber<Placed, Subscriber>("orders", "audit");
    }

    [Fact]
    public async Task A_stream_only_host_is_not_ready_until_its_listener_starts()
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHealthChecks()
            .AddStreamHandler<ListCatalog, int, Listing>("catalog");
        using var host = builder.Build();
        var health = host.Services.GetRequiredService<HealthCheckService>();
        Assert.Equal(
            HealthStatus.Unhealthy,
            (
                await health.CheckHealthAsync(
                    r => r.Tags.Contains("ready"),
                    TestContext.Current.CancellationToken
                )
            ).Status
        );
        await host.StartAsync(TestContext.Current.CancellationToken);
        Assert.Equal(
            HealthStatus.Healthy,
            (
                await health.CheckHealthAsync(
                    r => r.Tags.Contains("ready"),
                    TestContext.Current.CancellationToken
                )
            ).Status
        );
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Replacing_an_assignable_payload_updates_compatible_keys_and_removes_stale_ones(
        bool differentType
    )
    {
        var context = new PipeContext();
        var old = context.GetOrAddPayload(() => new Stamp(1));
        IStamp replacement = differentType ? new OtherStamp(2) : new Stamp(2);
        Assert.Same(
            replacement,
            context.AddOrUpdatePayload<IStamp>(
                () => throw new InvalidOperationException(),
                existing =>
                {
                    Assert.Same(old, existing);
                    return replacement;
                }
            )
        );
        Assert.True(context.TryGetPayload<IStamp>(out var read));
        Assert.Same(replacement, read);
        Assert.Equal(!differentType, context.TryGetPayload<Stamp>(out var concrete));
        if (!differentType)
            Assert.Same(replacement, concrete);
    }

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(double.NegativeInfinity)]
    public void Retry_policies_refuse_nonfinite_factors_and_jitter(double number)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            RetryPolicy.Exponential(
                1,
                TimeSpan.FromMilliseconds(1),
                TimeSpan.FromSeconds(1),
                number
            )
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            LockRetryPolicy.Exponential(
                1,
                TimeSpan.FromMilliseconds(1),
                TimeSpan.FromSeconds(1),
                number
            )
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            RetryPolicy.Immediate(1).WithJitter(number)
        );
    }

    [Fact]
    public void Coupon_validation_rejects_excessive_input_without_normalizing_or_allocating_it()
    {
        var generator = new CouponCodeGenerator();
        var profile = new CouponCodeProfile();
        var input = new string('a', 1_000_000);
        var before = GC.GetAllocatedBytesForCurrentThread();
        Assert.False(generator.TryValidate(input, profile, out var normalized));
        Assert.Null(normalized);
        Assert.True(GC.GetAllocatedBytesForCurrentThread() - before < 16 * 1024);
    }

    [Fact]
    public async Task Node_keys_are_checked_against_the_composed_lock_limit()
    {
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "catalog", MaxKeyLength = 8 },
            new InMemoryLockProvider()
        );
        var error = Assert.Throws<ArgumentException>(() =>
            new NodeIdElector("catalog", locks, new NodeLeasingOptions { PoolSize = 1 })
        );
        Assert.Contains("MaxKeyLength", error.Message, StringComparison.Ordinal);
    }

    private interface IStamp
    {
        int Count { get; }
    }

    private sealed record Stamp(int Count) : IStamp;

    private sealed record OtherStamp(int Count) : IStamp;

    public sealed record Placed : IEvent;

    public sealed class Subscriber : IEventHandler<Placed>
    {
        public ValueTask HandleAsync(Placed @event, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;
    }

    public sealed record ListCatalog : IStreamRequest<int>;

    public sealed class Listing : IStreamRequestHandler<ListCatalog, int>
    {
        public async IAsyncEnumerable<int> HandleAsync(
            ListCatalog request,
            [EnumeratorCancellation] CancellationToken cancellationToken
        )
        {
            await Task.CompletedTask;
            yield return 1;
        }
    }
}
