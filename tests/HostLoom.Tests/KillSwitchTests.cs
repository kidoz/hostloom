using HostLoom;
using HostLoom.Conformance;
using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.Tests;

public sealed class KillSwitchTests
{
    [Fact]
    public async Task The_ratio_pauses_the_next_delivery_until_the_restart_timeout()
    {
        var clock = new ManualTimeProvider();
        var state = new State();
        using var host = await StartAsync(clock, state, new KillSwitchPolicy(
            activationThreshold: 2,
            tripThreshold: 1,
            trackingPeriod: TimeSpan.FromMinutes(1),
            restartTimeout: TimeSpan.FromMinutes(1)
        ));
        var client = host.Services.GetRequiredService<IRequestClient<Work, Done>>();

        await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await client.GetResponseAsync("work", new Work(), timeout: TimeSpan.FromMinutes(10), cancellationToken: TestContext.Current.CancellationToken)
        );
        await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await client.GetResponseAsync("work", new Work(), timeout: TimeSpan.FromMinutes(10), cancellationToken: TestContext.Current.CancellationToken)
        );
        Assert.Equal(2, state.Attempts);

        var third = client.GetResponseAsync(
            "work",
            new Work(),
            timeout: TimeSpan.FromMinutes(10),
            cancellationToken: TestContext.Current.CancellationToken
        );
        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.Equal(2, state.Attempts);
        clock.Advance(TimeSpan.FromMinutes(1));
        var done = await third;
        Assert.Equal("ok", done.Value);
        Assert.Equal(3, state.Attempts);
    }

    [Fact]
    public async Task One_failure_below_the_activation_count_does_not_pause()
    {
        var clock = new ManualTimeProvider();
        var state = new State();
        using var host = await StartAsync(clock, state, new KillSwitchPolicy(
            activationThreshold: 2,
            tripThreshold: 1,
            trackingPeriod: TimeSpan.FromMinutes(1),
            restartTimeout: TimeSpan.FromMinutes(1)
        ));
        var client = host.Services.GetRequiredService<IRequestClient<Work, Done>>();
        await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await client.GetResponseAsync("work", new Work(), timeout: TimeSpan.FromMinutes(10), cancellationToken: TestContext.Current.CancellationToken)
        );
        state.Allow = true;
        var done = await client.GetResponseAsync(
            "work",
            new Work(),
            timeout: TimeSpan.FromMinutes(10),
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.Equal("ok", done.Value);
        Assert.Equal(2, state.Attempts);
    }

    [Fact]
    public async Task An_ignored_exception_does_not_trip()
    {
        var clock = new ManualTimeProvider();
        var state = new State();
        using var host = await StartAsync(
            clock,
            state,
            new KillSwitchPolicy(
                activationThreshold: 1,
                tripThreshold: 1,
                trackingPeriod: TimeSpan.FromMinutes(1),
                restartTimeout: TimeSpan.FromMinutes(1),
                shouldCount: exception => exception is not InvalidOperationException
            )
        );
        var client = host.Services.GetRequiredService<IRequestClient<Work, Done>>();
        await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await client.GetResponseAsync("work", new Work(), timeout: TimeSpan.FromMinutes(10), cancellationToken: TestContext.Current.CancellationToken)
        );
        state.Allow = true;
        var done = await client.GetResponseAsync(
            "work",
            new Work(),
            timeout: TimeSpan.FromMinutes(10),
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.Equal("ok", done.Value);
    }

    [Fact]
    public async Task A_paused_event_publish_stays_in_the_outbox()
    {
        var clock = new ManualTimeProvider();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<TimeProvider>(clock);
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .UseInMemoryOutbox()
            .AddSubscriber<Notice, NoticeHandler>("notices");
        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        host.Services.GetRequiredService<ReceiveAdmission>().Suspend(
            new ReceiveDestination("notices", "default"),
            clock.GetUtcNow().AddMinutes(5)
        );

        await host.Services.GetRequiredService<IPublishEndpoint>().PublishAsync(
            "notices",
            new Notice("n1"),
            TestContext.Current.CancellationToken
        );

        var store = host.Services.GetRequiredService<InMemoryOutboxStore>();
        var held = false;
        for (var attempt = 0; attempt < 50 && !held; attempt++)
        {
            held = store.Pending.Count == 1
                && store.Pending[0].Attempts >= 1
                && store.Published.Count == 0;
            if (!held)
            {
                await Task.Delay(20, TestContext.Current.CancellationToken);
            }
        }

        Assert.True(held);
    }

    [Fact]
    public async Task The_probe_reports_the_kill_switch()
    {
        var clock = new ManualTimeProvider();
        using var host = await StartAsync(clock, new State(), new KillSwitchPolicy(
            2,
            0.5,
            TimeSpan.FromSeconds(30),
            TimeSpan.FromSeconds(10)
        ));
        var probe = host.Services.GetRequiredService<HostLoomProbe>().ReceivePipeline(
            TestContext.Current.CancellationToken
        );
        var filter = Assert.Single(probe.Children, child => child.Name == "kill-switch");
        Assert.Equal(2, filter.Properties["activationThreshold"]);
        Assert.Equal(0, filter.Properties["suspended"]);
    }

    private static async Task<IHost> StartAsync(ManualTimeProvider clock, State state, KillSwitchPolicy policy)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<TimeProvider>(clock);
        builder.Services.AddSingleton(state);
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .ConfigureReceivePipeline(pipe => pipe.UseKillSwitch(policy, clock))
            .AddHandler<Work, Done, WorkHandler>("work");
        var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        return host;
    }

    public sealed record Work : IRequest<Done>;

    public sealed record Done(string Value);

    public sealed record Notice(string Reference) : IEvent;

    public sealed class State
    {
        public int Attempts { get; set; }

        public bool Allow { get; set; }
    }

    public sealed class WorkHandler(State state) : IRequestHandler<Work, Done>
    {
        public ValueTask<Done> HandleAsync(Work request, CancellationToken cancellationToken)
        {
            state.Attempts++;
            if (!state.Allow && state.Attempts <= 2)
            {
                throw new InvalidOperationException("no");
            }

            return ValueTask.FromResult(new Done("ok"));
        }
    }

    public sealed class NoticeHandler : IEventHandler<Notice>
    {
        public ValueTask HandleAsync(Notice @event, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;
    }
}
