using System.Diagnostics;
using HostLoom.Actors;
using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace HostLoom.Benchmarks;

internal static class ActorInboxChecks
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan Window = ActorInboxPilot.Window;

    // Stop one store operation after it began but before it wrote the claim. Only the
    // inbox expiry clock is controlled. Caller deadlines and actor scheduling use real time.
    private sealed class ClaimClock : TimeProvider
    {
        private readonly TaskCompletionSource _resume = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private readonly TaskCompletionSource _entered = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private int _armed;
        private long _ticks;
        public Task Entered => _entered.Task;

        public void Arm() => Volatile.Write(ref _armed, 1);

        public void Resume() => _resume.TrySetResult();

        public void Expire() => Interlocked.Add(ref _ticks, Window.Ticks);

        public override DateTimeOffset GetUtcNow()
        {
            if (Interlocked.Exchange(ref _armed, 0) == 1)
            {
                _entered.SetResult();
                if (!_resume.Task.Wait(TimeSpan.FromSeconds(10)))
                    throw new TimeoutException("Pilot did not resume the inbox operation.");
            }
            return DateTimeOffset.UnixEpoch.AddTicks(Volatile.Read(ref _ticks));
        }
    }

    public static async Task RunAsync()
    {
        foreach (var actor in new[] { false, true })
        {
            var clock = new ClaimClock();
            await ActorInboxPilot
                .WithStoreAsync(actor, clock, store => CheckClaimsAsync(store, clock, actor))
                .ConfigureAwait(false);
        }
        await CheckFilterAndDrainAsync().ConfigureAwait(false);
    }

    private static async Task CheckClaimsAsync(IInboxStore store, ClaimClock clock, bool actor)
    {
        var expired = Guid.NewGuid();
        var owner = Guid.NewGuid();
        Require(await store.TryRecordAsync("orders:expiry", expired, Window).ConfigureAwait(false));
        clock.Expire();
        Require(await store.TryRecordAsync("orders:expiry", owner, Window).ConfigureAwait(false));
        await store.ReleaseAsync("orders:expiry", expired).ConfigureAwait(false);
        Require(
            !await store
                .TryRecordAsync("orders:expiry", Guid.NewGuid(), Window)
                .ConfigureAwait(false)
        );
        await store.ReleaseAsync("orders:expiry", owner).ConfigureAwait(false);
        Require(
            await store
                .TryRecordAsync("orders:expiry", Guid.NewGuid(), Window)
                .ConfigureAwait(false)
        );

        var cancelled = new CancellationToken(canceled: true);
        try
        {
            await store
                .TryRecordAsync("orders:before", owner, Window, cancelled)
                .ConfigureAwait(false);
            throw new InvalidOperationException("Pre-cancelled claim returned normally.");
        }
        catch (OperationCanceledException) when (cancelled.IsCancellationRequested) { }
        Require(await store.TryRecordAsync("orders:before", owner, Window).ConfigureAwait(false));

        using var during = new CancellationTokenSource();
        clock.Arm();
        var pending = Task.Run(async () =>
            await store
                .TryRecordAsync("orders:during", owner, Window, during.Token)
                .ConfigureAwait(false)
        );
        try
        {
            await clock.Entered.WaitAsync(Bound).ConfigureAwait(false);
            await during.CancelAsync().ConfigureAwait(false);
        }
        finally
        {
            clock.Resume();
        }
        var reported = false;
        try
        {
            reported = await pending.WaitAsync(Bound).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (during.IsCancellationRequested) { }
        var retry = await store
            .TryRecordAsync("orders:during", Guid.NewGuid(), Window)
            .ConfigureAwait(false);
        Console.WriteLine(
            $"# {(actor ? "actor" : "current")} expiry/stale-release/pre-cancel passed; during-claim cancellation: ownership reported={reported}, retry won={retry}, unreported claim={!reported && !retry}"
        );
    }

    internal sealed record Observation(int Items) : IEvent;

    private sealed class Counts
    {
        internal int Handled,
            Skipped,
            Duplicates;
    }

    private sealed class Observer(Counts counts) : IEventHandler<Observation>
    {
        public ValueTask HandleAsync(Observation message, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref counts.Handled);
            return ValueTask.CompletedTask;
        }
    }

    private static async Task CheckFilterAndDrainAsync()
    {
        var clock = new ClaimClock();
        var store = new ActorInboxStore(clock, 1);
        await using var ownedStore = store.ConfigureAwait(false);
        var counts = new Counts();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(counts);
        services
            .AddHostLoom()
            .UseInMemory()
            .ConfigureReceivePipeline(pipe =>
                pipe.Use(
                    async (context, next) =>
                    {
                        await next.SendAsync(context).ConfigureAwait(false);
                        if (
                            context.TryGetPayload<InboxSkipped>(out var skipped)
                            && skipped?.Failure
                                is ActorAdmissionException { Reason: ActorAdmission.Full }
                        )
                            Interlocked.Increment(ref counts.Skipped);
                        if (context.TryGetPayload<InboxDuplicate>(out _))
                            Interlocked.Increment(ref counts.Duplicates);
                    },
                    "pilot"
                )
            )
            .UseInbox(_ => store, Window)
            .AddSubscriber<Observation, Observer>("orders", "audit");
        var provider = services.BuildServiceProvider();
        await using var ownedProvider = provider.ConfigureAwait(false);
        var endpoint = provider.GetServices<IHostedService>().Single();
        await endpoint.StartAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            var broker = (IEventBroker)provider.GetRequiredService<IRequestBroker>();
            byte[]? frame = null;
            var subscription = await broker
                .SubscribeAsync(
                    "orders",
                    "capture",
                    (bytes, _) =>
                    {
                        frame = bytes.ToArray();
                        return ValueTask.CompletedTask;
                    },
                    CancellationToken.None
                )
                .ConfigureAwait(false);
            await using var capture = subscription.ConfigureAwait(false);
            await provider
                .GetRequiredService<IPublishEndpoint>()
                .PublishAsync("orders", new Observation(1))
                .ConfigureAwait(false);
            var original = frame ?? throw new InvalidOperationException("Event capture failed.");
            await broker
                .PublishAsync("orders", original, CancellationToken.None)
                .ConfigureAwait(false);
            Require(counts.Handled == 1 && counts.Duplicates == 1);
            clock.Arm();
            var active = store.TryRecordAsync("orders:active", Guid.NewGuid(), Window).AsTask();
            Task<bool> queued;
            Task stop;
            long started;
            try
            {
                await clock.Entered.WaitAsync(Bound).ConfigureAwait(false);
                queued = store.TryRecordAsync("orders:queued", Guid.NewGuid(), Window).AsTask();
                await broker
                    .PublishAsync("orders", original, CancellationToken.None)
                    .ConfigureAwait(false);
                Require(counts.Handled == 2 && counts.Skipped == 1);
                stop = store.DisposeAsync().AsTask();
                Require(!stop.IsCompleted);
                try
                {
                    await store
                        .TryRecordAsync("orders:closed", Guid.NewGuid(), Window)
                        .ConfigureAwait(false);
                    throw new InvalidOperationException("Shutdown admitted new work.");
                }
                catch (ActorAdmissionException error) when (error.Reason == ActorAdmission.Stopped)
                { }
            }
            finally
            {
                started = Stopwatch.GetTimestamp();
                clock.Resume();
            }
            await stop.WaitAsync(Bound).ConfigureAwait(false);
            Require(await active.ConfigureAwait(false) && await queued.ConfigureAwait(false));
            var drain = Stopwatch.GetElapsedTime(started);
            Require(drain <= Bound);
            Console.WriteLine(
                FormattableString.Invariant(
                    $"# actor full: same event delivered 3 times, handler ran {counts.Handled}, duplicate={counts.Duplicates}, inbox skipped(full)={counts.Skipped}; shutdown admitted work settled=2/2, new send stopped, drain after unblock {drain.TotalMilliseconds:F2} ms"
                )
            );
        }
        finally
        {
            await endpoint.StopAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    private static void Require(
        bool condition,
        [System.Runtime.CompilerServices.CallerArgumentExpression(nameof(condition))]
            string? expression = null
    )
    {
        if (!condition)
            throw new InvalidOperationException($"Inbox pilot check failed: {expression}");
    }
}
