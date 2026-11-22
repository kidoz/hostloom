using System.Collections.Concurrent;
using HostLoom.Actors;
using HostLoom.Actors.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.Tests;

public sealed class ActorSystemTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private static TaskCompletionSource Signal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class Worker(
        Func<int, CancellationToken, ValueTask<int>> turn,
        Action? dispose = null
    ) : IActor<int, int>, IAsyncDisposable
    {
        public ValueTask<int> ReceiveAsync(int message, CancellationToken stopping) =>
            turn(message, stopping);

        public ValueTask DisposeAsync()
        {
            dispose?.Invoke();
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task BoundedActor_OwnsStateAcrossAwait_AndDrainsAdmittedWork()
    {
        await using var system = new ActorSystem(2, 2, Bound);
        var entered = Signal();
        var release = Signal();
        var seen = new List<int>();
        var first = system.Spawn<int, int>(() =>
            new Worker(
                async (value, _) =>
                {
                    seen.Add(value);
                    if (value == 1)
                    {
                        entered.SetResult();
                        await release.Task.WaitAsync(Bound, Token);
                    }
                    return seen.Count;
                }
            )
        );
        Assert.Equal(ActorAdmission.Accepted, first.Ref.TryTell(1));
        await entered.Task.WaitAsync(Bound, Token);
        var reply = first.Ref.AskAsync(2, Bound, Token);
        Assert.Equal(ActorAdmission.Accepted, first.Ref.TryTell(3));
        Assert.Equal(ActorAdmission.Full, first.Ref.TryTell(4));
        var rejected = await Assert.ThrowsAsync<ActorAdmissionException>(async () =>
            await first.Ref.AskAsync(5, Bound, Token)
        );
        Assert.Equal(ActorAdmission.Full, rejected.Reason);
        Assert.Equal(2, system.Snapshot.Queued);
        Assert.Equal(1, system.Snapshot.Executing);

        var other = system.Spawn<int, int>(() =>
            new Worker((value, _) => ValueTask.FromResult(value))
        );
        Assert.Equal(6, await other.Ref.AskAsync(6, Bound, Token));
        var drain = first.StopAsync(Token);
        Assert.False(drain.IsCompleted);
        Assert.Equal(ActorAdmission.Stopped, first.Ref.TryTell(7));
        release.SetResult();
        Assert.Equal(2, await reply);
        await drain.WaitAsync(Bound, Token);
        Assert.Equal([1, 2, 3], seen);
        var replacement = system.Spawn<int, int>(() =>
            new Worker((value, _) => ValueTask.FromResult(value))
        );
        Assert.Equal(ActorAdmission.Stopped, first.Ref.TryTell(8));
        Assert.Equal(9, await replacement.Ref.AskAsync(9, Bound, Token));
    }

    [Fact]
    public async Task CancelledAndTimedOutAsks_KeepAdmittedCommands()
    {
        var clock = new TestClock();
        await using var system = new ActorSystem(1, 3, Bound, clock);
        var entered = Signal();
        var release = Signal();
        var seen = new List<int>();
        var actor = system.Spawn<int, int>(() =>
            new Worker(
                async (value, _) =>
                {
                    seen.Add(value);
                    if (value == 1)
                    {
                        entered.SetResult();
                        await release.Task.WaitAsync(Bound, Token);
                    }
                    return value;
                }
            )
        );
        using var cancellation = new CancellationTokenSource();
        var active = actor.Ref.AskAsync(1, Bound, cancellation.Token).AsTask();
        await entered.Task.WaitAsync(Bound, Token);
        var queued = actor.Ref.AskAsync(2, Bound, cancellation.Token).AsTask();
        var expires = actor.Ref.AskAsync(3, Bound, Token).AsTask();
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => active);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queued);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await actor.Ref.AskAsync(4, Bound, cancellation.Token)
        );
        clock.Advance(Bound);
        await Assert.ThrowsAsync<TimeoutException>(() =>
            expires.WaitAsync(TimeSpan.FromSeconds(2), Token)
        );
        Assert.Equal(1, system.Snapshot.Timeouts);
        release.SetResult();
        await actor.StopAsync(Token).WaitAsync(Bound, Token);
        Assert.Equal([1, 2, 3], seen);
        Assert.Equal(0, clock.PendingTimers);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Fault_StopsOnlyItsIncarnation_AndDisposesOnce(bool ask)
    {
        var enter = Signal();
        var release = Signal();
        int disposed = 0;
        await using var system = new ActorSystem(2, 2, Bound);
        var peer = system
            .Spawn<int, int>(() =>
            {
                var count = 0;
                return new Worker((delta, _) => ValueTask.FromResult(count += delta));
            })
            .Ref;
        Assert.Equal(7, await peer.AskAsync(7, Bound, Token));
        var actor = system.Spawn<int, int>(() =>
            new Worker(
                async (_, _) =>
                {
                    enter.SetResult();
                    await release.Task.WaitAsync(Bound, Token);
                    throw new FormatException();
                },
                () => Interlocked.Increment(ref disposed)
            )
        );
        Task<int>? failing = null;
        if (ask)
            failing = actor.Ref.AskAsync(1, Bound, Token).AsTask();
        else
            Assert.Equal(ActorAdmission.Accepted, actor.Ref.TryTell(1));
        await enter.Task.WaitAsync(Bound, Token);
        var queued = actor.Ref.AskAsync(2, Bound, Token).AsTask();
        release.SetResult();
        if (failing is not null)
            await Assert.ThrowsAsync<ActorFaultException>(() => failing);
        await Assert.ThrowsAsync<ActorTerminatedException>(() => queued);
        var fault = await Assert.ThrowsAsync<ActorFaultException>(() =>
            actor.Completion.WaitAsync(Bound, Token)
        );
        Assert.IsType<FormatException>(fault.InnerException);
        Assert.Equal(1, disposed);
        Assert.Equal(1, system.Snapshot.Discarded);
        Assert.Equal(1, system.Snapshot.Faults);
        Assert.Equal(7, await peer.AskAsync(0, Bound, Token));
        Assert.Equal(8, await peer.AskAsync(1, Bound, Token));
        var live = system.Spawn<int, int>(() =>
            new Worker((value, _) => ValueTask.FromResult(value))
        );
        Assert.Equal(3, await live.Ref.AskAsync(3, Bound, Token));
    }

    [Fact]
    public async Task GraceExpiry_DiscardsQueue_ButRetainsActiveActorUntilActualCompletion()
    {
        var clock = new TestClock();
        await using var system = new ActorSystem(1, 1, Bound, clock);
        var entered = Signal();
        var cancelled = Signal();
        var release = Signal();
        var disposed = Signal();
        var actor = system.Spawn<int, int>(() =>
            new Worker(
                async (value, stopping) =>
                {
                    using var registered = stopping.Register(() => cancelled.TrySetResult());
                    entered.SetResult();
                    await release.Task.WaitAsync(Bound, Token);
                    return value;
                },
                disposed.SetResult
            )
        );
        var active = actor.Ref.AskAsync(1, TimeSpan.FromMinutes(1), Token);
        await entered.Task.WaitAsync(Bound, Token);
        var pending = actor.Ref.AskAsync(2, TimeSpan.FromMinutes(1), Token).AsTask();
        var stop = system.StopAsync(Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            system.StopAsync(new CancellationToken(true))
        );
        clock.Advance(Bound);
        await cancelled.Task.WaitAsync(Bound, Token);
        await Assert.ThrowsAsync<ActorTerminatedException>(() => pending.WaitAsync(Bound, Token));
        Assert.False(disposed.Task.IsCompleted);
        Assert.False(stop.IsCompleted);
        release.SetResult();
        Assert.Equal(1, await active);
        await stop.WaitAsync(Bound, Token);
        Assert.True(disposed.Task.IsCompletedSuccessfully);
        Assert.Equal(0, system.Snapshot.Actors);
    }

    [Fact]
    public async Task StopDuringConstruction_JoinsReservedActorAndDisposal()
    {
        await using var system = new ActorSystem(1, 1, TimeSpan.Zero);
        var entered = Signal();
        using var release = new ManualResetEventSlim();
        var disposed = Signal();
        var spawn = Task.Run(
            () =>
                system.Spawn<int, int>(() =>
                {
                    entered.SetResult();
                    Assert.True(release.Wait(Bound, Token));
                    return new Worker(
                        (value, _) => ValueTask.FromResult(value),
                        disposed.SetResult
                    );
                }),
            Token
        );
        await entered.Task.WaitAsync(Bound, Token);
        Assert.Throws<InvalidOperationException>(() =>
            system.Spawn<int, int>(() => throw new FormatException())
        );
        var stop = system.StopAsync(Token);
        Assert.False(stop.IsCompleted);
        release.Set();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await spawn);
        await stop.WaitAsync(Bound, Token);
        Assert.True(disposed.Task.IsCompletedSuccessfully);
        Assert.Equal(0, system.Snapshot.Actors);
    }

    [Fact]
    public async Task SendersRacingStop_ExecuteExactlyTheAdmittedSet()
    {
        await using var system = new ActorSystem(1, 32, Bound);
        var seen = new List<int>();
        var actor = system.Spawn<int, int>(() =>
            new Worker(
                (value, _) =>
                {
                    seen.Add(value);
                    return ValueTask.FromResult(value);
                }
            )
        );
        var racing = Signal();
        var ready = Enumerable.Range(0, 4).Select(_ => Signal()).ToArray();
        var accepted = new ConcurrentBag<int>();
        var senders = Enumerable
            .Range(0, 4)
            .Select(producer =>
                Task.Run(
                    async () =>
                    {
                        // All four producers are live and have admitted work before the close race.
                        var first = producer * 128;
                        Assert.Equal(ActorAdmission.Accepted, actor.Ref.TryTell(first));
                        accepted.Add(first);
                        ready[producer].SetResult();
                        await racing.Task.WaitAsync(Bound, Token);
                        for (var n = 1; n < 128; n++)
                        {
                            var value = first + n;
                            if (actor.Ref.TryTell(value) == ActorAdmission.Accepted)
                                accepted.Add(value);
                        }
                    },
                    Token
                )
            )
            .ToArray();
        await Task.WhenAll(ready.Select(signal => signal.Task)).WaitAsync(Bound, Token);
        racing.SetResult();
        await actor.StopAsync(Token).WaitAsync(Bound, Token);
        await Task.WhenAll(senders).WaitAsync(Bound, Token);
        Assert.Equal(accepted.Order(), seen.Order());
        Assert.Equal(ActorAdmission.Stopped, actor.Ref.TryTell(-1));
        Assert.Equal(0, system.Snapshot.Queued);
    }

    [Fact]
    public async Task FailedAsyncFactory_RetainsReservationThroughCleanup()
    {
        await using var system = new ActorSystem(1, 1, TimeSpan.Zero);
        var entered = Signal();
        var release = Signal();
        var construction = system
            .SpawnAsync<int, int>(async () =>
            {
                entered.SetResult();
                await release.Task.WaitAsync(Bound, Token);
                throw new FormatException();
            })
            .AsTask();
        await entered.Task.WaitAsync(Bound, Token);
        var stop = system.StopAsync(Token);
        Assert.Equal(1, system.Snapshot.Actors);
        Assert.False(stop.IsCompleted);
        release.SetResult();
        await Assert.ThrowsAsync<FormatException>(() => construction.WaitAsync(Bound, Token));
        await stop.WaitAsync(Bound, Token);
        Assert.Equal(0, system.Snapshot.Actors);
    }

    [Fact]
    public async Task HostedReferences_AreIndependentAcrossProviders()
    {
        var registrations = new ServiceCollection();
        registrations.AddScoped<ScopedDependency>();
        registrations
            .AddLocalActorSystem(1, 2, Bound)
            .AddLocalActor<int, ScopedDependency, ScopedWorker>();
        await using var first = registrations.BuildServiceProvider(validateScopes: true);
        await using var second = registrations.BuildServiceProvider(validateScopes: true);
        var a = first.GetRequiredService<IActorRef<int, ScopedDependency>>();
        var b = second.GetRequiredService<IActorRef<int, ScopedDependency>>();
        var firstHost = first.GetRequiredService<IHostedService>();
        var secondHost = second.GetRequiredService<IHostedService>();
        await firstHost.StartAsync(Token);
        await secondHost.StartAsync(Token);
        var firstState = await a.AskAsync(1, Bound, Token);
        var secondState = await b.AskAsync(1, Bound, Token);
        Assert.NotSame(firstState, secondState);
        var owner = first.GetRequiredService<ActorHandle<int, ScopedDependency>>();
        await firstHost.StopAsync(Token);
        Assert.True(owner.Completion.IsCompletedSuccessfully);
        Assert.Equal(ActorAdmission.Stopped, a.TryTell(2));
        Assert.Same(secondState, await b.AskAsync(2, Bound, Token));
        await secondHost.StopAsync(Token);
    }

    [Fact]
    public async Task AbortRacingDequeue_SettlesEveryAskAccordingToItsActualTurn()
    {
        var system = new ActorSystem(1, 32, TimeSpan.Zero);
        await using var owned = system;
        var entered = Signal();
        var release = Signal();
        var seen = new HashSet<int>();
        var actor = system.Spawn<int, int>(() =>
            new Worker(
                async (value, _) =>
                {
                    if (value == 0)
                    {
                        entered.SetResult();
                        await release.Task.WaitAsync(Bound, Token);
                    }
                    Assert.True(seen.Add(value));
                    return value;
                }
            )
        );
        var first = actor.Ref.AskAsync(0, Bound, Token).AsTask();
        await entered.Task.WaitAsync(Bound, Token);
        var pending = Enumerable
            .Range(1, 32)
            .Select(value => actor.Ref.AskAsync(value, Bound, Token).AsTask())
            .ToArray();
        release.SetResult();
        await system.StopAsync(Token).WaitAsync(Bound, Token);
        Assert.Equal(0, await first);
        for (var i = 0; i < pending.Length; i++)
        {
            if (seen.Contains(i + 1))
                Assert.Equal(i + 1, await pending[i]);
            else
                await Assert.ThrowsAsync<ActorTerminatedException>(() => pending[i]);
        }
        Assert.Equal(33 - seen.Count, system.Snapshot.Discarded);
    }

    private sealed class ScopedDependency : IAsyncDisposable
    {
        public int Disposals { get; private set; }

        public ValueTask DisposeAsync()
        {
            Disposals++;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ScopedWorker(ScopedDependency dependency) : IActor<int, ScopedDependency>
    {
        public ValueTask<ScopedDependency> ReceiveAsync(int message, CancellationToken stopping) =>
            ValueTask.FromResult(dependency);
    }

    private sealed class StartupWorker : IActor<string, string>
    {
        public StartupWorker(ScopedDependency dependency, Action<ScopedDependency> construct)
        {
            construct(dependency);
        }

        public ValueTask<string> ReceiveAsync(string message, CancellationToken stopping) =>
            ValueTask.FromResult(message);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Hosting_OwnsActorScopesAndRollsBackPartialStartup(bool fail)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.ConfigureContainer(
            new DefaultServiceProviderFactory(new ServiceProviderOptions { ValidateScopes = true })
        );
        var created = new List<ScopedDependency>();
        builder.Services.AddScoped(_ =>
        {
            var value = new ScopedDependency();
            created.Add(value);
            return value;
        });
        builder
            .Services.AddLocalActorSystem(2, 1, Bound)
            .AddLocalActor<int, ScopedDependency, ScopedWorker>();
        if (fail)
        {
            builder.Services.AddSingleton<Action<ScopedDependency>>(
                (ScopedDependency _) => throw new FormatException()
            );
            builder.Services.AddLocalActor<string, string, StartupWorker>();
        }

        using var host = builder.Build();
        var actor = host.Services.GetRequiredService<IActorRef<int, ScopedDependency>>();
        if (fail)
            await Assert
                .ThrowsAsync<FormatException>(() => host.StartAsync(Token))
                .WaitAsync(Bound, Token);
        else
        {
            await host.StartAsync(Token).WaitAsync(Bound, Token);
            Assert.Same(created.Single(), await actor.AskAsync(1, Bound, Token));
            await host.StopAsync(Token).WaitAsync(Bound, Token);
        }
        Assert.Equal(ActorAdmission.Stopped, actor.TryTell(2));
        Assert.Equal(fail ? 2 : 1, created.Count);
        Assert.All(created, value => Assert.Equal(1, value.Disposals));
        Assert.Equal(0, host.Services.GetRequiredService<ActorSystem>().Snapshot.Actors);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task HostStopDuringSecondActorConstruction_JoinsBothScopes(bool factoryThrows)
    {
        var entered = new TaskCompletionSource<ScopedDependency>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        using var release = new ManualResetEventSlim();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddScoped<ScopedDependency>();
        builder.Services.AddSingleton<Action<ScopedDependency>>(dependency =>
        {
            entered.SetResult(dependency);
            if (!release.Wait(Bound, Token))
                throw new TimeoutException(
                    "The test did not release the second actor constructor."
                );
            if (factoryThrows)
                throw new FormatException();
        });
        builder
            .Services.AddLocalActorSystem(2, 1, Bound)
            .AddLocalActor<int, ScopedDependency, ScopedWorker>()
            .AddLocalActor<string, string, StartupWorker>();

        using var host = builder.Build();
        var system = host.Services.GetRequiredService<ActorSystem>();
        var first = host.Services.GetRequiredService<IActorRef<int, ScopedDependency>>();
        var second = host.Services.GetRequiredService<IActorRef<string, string>>();
        // DI constructors run synchronously. Run startup separately so the test can stop the host.
        var startup = Task.Run(() => host.StartAsync(Token), Token);
        ScopedDependency secondDependency;
        Exception? error;
        Task stop;
        ScopedDependency firstDependency;
        try
        {
            secondDependency = await entered.Task.WaitAsync(Bound, Token);
            firstDependency = await first.AskAsync(1, Bound, Token);
            var firstOwner = host.Services.GetRequiredService<ActorHandle<int, ScopedDependency>>();
            Assert.NotSame(firstDependency, secondDependency);
            Assert.Equal(2, system.Snapshot.Actors);
            Assert.Throws<InvalidOperationException>(() => second.TryTell("pending"));

            stop = host.StopAsync(Token);
            await firstOwner.Completion.WaitAsync(Bound, Token);
            Assert.Equal(ActorAdmission.Stopped, first.TryTell(2));
            Assert.Equal(1, firstDependency.Disposals);
            Assert.Equal(0, secondDependency.Disposals);
            Assert.Equal(1, system.Snapshot.Actors);
            Assert.False(stop.IsCompleted);
            Assert.False(startup.IsCompleted);
        }
        finally
        {
            release.Set();
            error = await Record.ExceptionAsync(() => startup).AsTask().WaitAsync(Bound, Token);
        }

        Assert.IsType(
            factoryThrows ? typeof(FormatException) : typeof(ObjectDisposedException),
            error
        );
        await stop.WaitAsync(Bound, Token);
        Assert.Equal(ActorAdmission.Stopped, first.TryTell(3));
        Assert.Throws<InvalidOperationException>(() => second.TryTell("stopped"));
        Assert.Equal(1, firstDependency.Disposals);
        Assert.Equal(1, secondDependency.Disposals);
        Assert.Equal(0, system.Snapshot.Actors);
    }
}
