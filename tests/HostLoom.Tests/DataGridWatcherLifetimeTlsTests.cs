using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Runtime;
using HostLoom.DataGrid.Storage;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridWatcherLifetimeTlsTests
{
    [Theory(Timeout = 25000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FullWatcherPoolRefusesOnlyExcessCallerAndReusesCanceledSlots(bool leader)
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        await using (var owner = new Owner())
        {
            var runtime = owner.Runtime;
            await owner.Clock.Started.WaitAsync(timeout.Token);
            using var canceled = new CancellationTokenSource();
            await canceled.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                Wait(runtime, leader, canceled.Token)
            );
            using var callers = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
            var waits = Fill(runtime, leader, callers.Token);
            await runtime.ObserveLeaderAsync(0, timeout.Token); // FIFO registration barrier.
            Assert.All(waits, wait => Assert.False(wait.IsCompleted));
            await Assert.ThrowsAsync<IOException>(() => Wait(runtime, leader, timeout.Token));
            await runtime.ObserveLeaderAsync(0, timeout.Token);
            Assert.Null(runtime.Failure);
            Assert.False(runtime.Completion.IsCompleted);
            await callers.CancelAsync();
            foreach (var wait in waits)
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait);
            var replacement = Fill(runtime, !leader, timeout.Token);
            await runtime.ObserveLeaderAsync(0, timeout.Token);
            Assert.All(replacement, wait => Assert.False(wait.IsCompleted));
            await runtime.DisposeAsync();
            foreach (var wait in replacement)
                await Assert.ThrowsAsync<IOException>(() => wait);
            Assert.Same(runtime.DisposeAsync().AsTask(), runtime.DisposeAsync().AsTask());
            await Assert.ThrowsAsync<IOException>(() => Wait(runtime, leader, timeout.Token));
            Assert.Null(runtime.Failure);
            Assert.True(runtime.Diagnostics.Storage.Closed);
            Assert.Equal(0, owner.Clock.ActiveTimers);
            owner.AssertReopens();
        }
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Fact(Timeout = 25000)]
    public async Task CancellationDuringRegistrationPublicationCannotLeaveAnUnownedWatcher()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        using var gate = new PublicationGate(timeout.Token);
        await using (var owner = new Owner())
        {
            var runtime = owner.Runtime;
            await owner.Clock.Started.WaitAsync(timeout.Token);
            await runtime.ObserveLeaderAsync(0, timeout.Token);
            using var caller = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
            try
            {
                gate.Arm();
                var canceled = runtime.WaitForAppliedAsync(long.MaxValue, caller.Token);
                await gate.Entered.WaitAsync(timeout.Token);
                // The actor added the watcher, but Invoke has not returned its registration result.
                await caller.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceled);
                gate.Release();
                await runtime.ObserveLeaderAsync(0, timeout.Token);
                var waits = Fill(runtime, leader: false, timeout.Token);
                await runtime.ObserveLeaderAsync(0, timeout.Token);
                Assert.All(waits, wait => Assert.False(wait.IsCompleted));
                Assert.Null(runtime.Failure);
                await runtime.DisposeAsync();
                foreach (var wait in waits)
                    await Assert.ThrowsAsync<IOException>(() => wait);
                Assert.Equal(0, owner.Clock.ActiveTimers);
                owner.AssertReopens();
            }
            finally
            {
                gate.Release();
            }
        }
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Fact(Timeout = 25000)]
    public async Task PersistenceFaultTerminatesBothWatcherKindsAndReleasesTimersAndDirectory()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        await using (var owner = new Owner())
        {
            var runtime = owner.Runtime;
            await owner.Clock.Started.WaitAsync(timeout.Token);
            var waits = Enumerable
                .Range(0, 64)
                .Select(i => Wait(runtime, i % 2 == 0, timeout.Token))
                .ToArray();
            await runtime.ObserveLeaderAsync(0, timeout.Token);
            Assert.All(waits, wait => Assert.False(wait.IsCompleted));
            owner.FailPersistence = true;
            // Persistence fencing may be observed after this coordination call returns.
            await Record.ExceptionAsync(() => runtime.ObserveLeaderAsync(10, timeout.Token));
            await runtime.Completion.WaitAsync(timeout.Token);
            foreach (var wait in waits)
                await Assert.ThrowsAsync<IOException>(() => wait);
            Assert.NotNull(runtime.Failure);
            Assert.True(runtime.Diagnostics.Storage.Closed);
            Assert.Equal(0, owner.Clock.ActiveTimers);
            owner.AssertReopens();
        }
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    private static Task Wait(DataGridPeerRuntime runtime, bool leader, CancellationToken token) =>
        leader
            ? runtime.WaitForLeaderAsync(token)
            : runtime.WaitForAppliedAsync(long.MaxValue, token);

    private static Task[] Fill(DataGridPeerRuntime runtime, bool leader, CancellationToken token) =>
        Enumerable.Range(0, 64).Select(_ => Wait(runtime, leader, token)).ToArray();

    private static void RequireLinux() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "File-backed TLS runtime checks require Linux."
        );

    private static CancellationTokenSource Deadline(CancellationToken token)
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(token);
        source.CancelAfter(TimeSpan.FromSeconds(20));
        return source;
    }

    private sealed class Owner : IAsyncDisposable
    {
        private readonly DataGridPeerFixture _fixture = new();
        private readonly string _path = Path.Combine(
            Path.GetTempPath(),
            "hostloom-watchers-" + Guid.NewGuid().ToString("N")
        );
        internal FrozenClock Clock { get; } = new();
        internal DataGridPeerRuntime Runtime { get; }
        internal bool FailPersistence { get; set; }

        [SuppressMessage(
            "Reliability",
            "CA2000:Dispose objects before losing scope",
            Justification = "Successful construction transfers store ownership to the runtime; failed construction disposes it."
        )]
        internal Owner()
        {
            Directory.CreateDirectory(_path);
            using (DataGridFileStore.Bootstrap(_path, _fixture.Configuration, 0)) { }
            var store = DataGridFileStore.Open(
                _path,
                _fixture.Configuration,
                0,
                observe: step =>
                {
                    if (FailPersistence && step == DataGridStorageStep.WalWritten)
                        throw new IOException("Controlled watcher-owner persistence failure.");
                }
            );
            try
            {
                Runtime = new(
                    store,
                    _fixture.Credentials(0),
                    _fixture.Listeners[0],
                    DataGridPeerFixture.Routes,
                    1,
                    Clock
                );
            }
            catch
            {
                store.Dispose();
                _fixture.Dispose();
                Directory.Delete(_path, true);
                throw;
            }
        }

        internal void AssertReopens()
        {
            using var store = DataGridFileStore.Open(_path, _fixture.Configuration, 0);
            Assert.Equal(0, store.Image.CommitIndex);
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await Runtime.DisposeAsync();
            }
            finally
            {
                _fixture.Dispose();
                Directory.Delete(_path, true);
            }
        }
    }

    // Freeze election delivery only; the runtime's own cancellation must still dispose each delay.
    private sealed class FrozenClock : TimeProvider
    {
        private int _active;
        private readonly TaskCompletionSource _started = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        internal Task Started => _started.Task;
        internal int ActiveTimers => Volatile.Read(ref _active);

        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period
        )
        {
            Interlocked.Increment(ref _active);
            _started.TrySetResult();
            return new FrozenTimer(this);
        }

        private sealed class FrozenTimer(FrozenClock owner) : ITimer
        {
            private int _disposed;

            public bool Change(TimeSpan dueTime, TimeSpan period) =>
                Volatile.Read(ref _disposed) == 0;

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) == 0)
                    Interlocked.Decrement(ref owner._active);
            }

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }
        }
    }

    private sealed class PublicationGate : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly ManualResetEventSlim _release = new();
        private readonly TaskCompletionSource _entered = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private int _stage;
        internal Task Entered => _entered.Task;

        internal void Arm() => Interlocked.Exchange(ref _stage, 1);

        internal void Release() => _release.Set();

        internal PublicationGate(CancellationToken token)
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (
                    instrument.Meter.Name == DataGridRuntimeDiagnostics.MeterName
                    && instrument.Name
                        is "hostloom.datagrid.runtime.observation.time"
                            or "hostloom.datagrid.runtime.queue.items"
                )
                    listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<long>(
                (instrument, value, _, _) =>
                {
                    if (instrument.Name == "hostloom.datagrid.runtime.queue.items")
                    {
                        if (value == -1)
                            Interlocked.CompareExchange(ref _stage, 2, 1);
                        return;
                    }
                    if (Interlocked.CompareExchange(ref _stage, 0, 2) != 2)
                        return;
                    _entered.TrySetResult();
                    _release.Wait(token);
                }
            );
            _listener.Start();
        }

        public void Dispose()
        {
            Release();
            _listener.Dispose();
            _release.Dispose();
        }
    }
}
