using System.Reflection;
using System.Runtime.CompilerServices;
using HostLoom.AspNetCore.WebSockets;
using Xunit;

namespace HostLoom.Tests;

public sealed class SubscriptionLifecycleTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Finished_subscriptions_detach_from_a_live_session(bool stopFirst)
    {
        using var session = new CancellationTokenSource();
        var reference = CreateFinished(stopFirst, session.Token);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        Assert.False(reference.IsAlive);
        GC.KeepAlive(session);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference CreateFinished(bool stopFirst, CancellationToken token)
    {
        var state = new SubscriptionState(Guid.NewGuid(), "catalog", null, 0, token);
        var source = typeof(SubscriptionState)
            .GetField("_snapshotCancellation", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(state)!;
        var reference = new WeakReference(source);
        if (stopFirst)
            state.Stop(static _ => { });
        state.InitializationFinished();
        if (!stopFirst)
            state.Stop(static _ => { });
        return reference;
    }

    [Fact]
    public async Task Stop_cancels_credit_wait_before_disposal_and_keeps_token_readable()
    {
        using var session = new CancellationTokenSource();
        var state = new SubscriptionState(Guid.NewGuid(), "catalog", null, 0, session.Token);
        var waiting = state.WaitForCreditAsync(state.SnapshotCancellationToken).AsTask();
        state.Stop(static _ => { });
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        state.InitializationFinished();
        Assert.True(state.SnapshotCancellationToken.IsCancellationRequested);
        Assert.False(state.TryAddCredit(1, 10));
        state.Stop(static _ => { });
        state.InitializationFinished();
    }
}
