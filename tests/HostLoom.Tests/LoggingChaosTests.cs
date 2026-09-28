using System.Diagnostics;
using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

// CA1873: the boxing standard ILogger path is what applications write, and what is under test.
// CA2000: sinks are owned by the providers they are handed to.
#pragma warning disable CA1873, CA2000

namespace HostLoom.Tests;

/// <summary>
/// Fault experiments on the logging pipeline. Each test states its hypothesis: the steady state
/// that must hold while a fault is injected, and what recovery must restore once it is removed.
/// Every record logged must end up either written or counted as dropped, never silently lost.
/// </summary>
public sealed class LoggingChaosTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Hypothesis: while the sink is stalled, Information records past the queue are shed and
    /// counted, a Warning waits no longer than EnqueueTimeout, and no call throws; once the sink
    /// resumes, new records are delivered again.
    /// </summary>
    [Fact]
    public async Task A_stalled_sink_sheds_information_bounds_warnings_and_recovers()
    {
        var sink = new ChaosSink();
        var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions
            {
                QueueCapacity = 16,
                BatchSize = 4,
                QueueFullPolicy = QueueFullPolicy.DropBelowWarning,
                EnqueueTimeout = TimeSpan.FromMilliseconds(50),
            }
        );
        var logger = provider.CreateLogger("Chaos");
        sink.Stall();
        var logged = 0;

        logger.LogInformation("first {Index}", logged++);
        Assert.True(
            sink.Stalled.Wait(Bound, TestContext.Current.CancellationToken),
            "the writer never reached the stalled sink"
        );
        for (var i = 0; i < 100; i++)
        {
            logger.LogInformation("shed {Index}", logged++);
        }

        var slowest = TimeSpan.Zero;
        for (var i = 0; i < 5; i++)
        {
            var watch = Stopwatch.StartNew();
            logger.LogWarning("bounded {Index}", logged++);
            slowest = watch.Elapsed > slowest ? watch.Elapsed : slowest;
        }

        Assert.True(provider.Dropped > 0, "nothing was shed while the sink was stalled");
        Assert.True(slowest < TimeSpan.FromSeconds(2), $"a warning waited {slowest}");

        sink.Resume();
        logger.LogWarning("recovered {Index}", logged++);
        await sink.WaitForLine("recovered", Bound);
        await provider.DisposeAsync();

        Assert.Equal(logged, sink.Written + provider.Dropped);
        sink.AssertEveryLineIsJson();
    }

    /// <summary>
    /// Hypothesis: with concurrent producers and a sink that keeps stalling briefly, every record
    /// is either written or counted as dropped, and every written line is valid JSON.
    /// </summary>
    [Fact]
    public async Task Records_are_never_lost_silently_while_the_sink_keeps_stalling()
    {
        var sink = new ChaosSink { StallEvery = 7, StallFor = TimeSpan.FromMilliseconds(2) };
        var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions
            {
                QueueCapacity = 64,
                BatchSize = 8,
                QueueFullPolicy = QueueFullPolicy.DropNewest,
            }
        );
        var logger = provider.CreateLogger("Chaos");
        const int producers = 4;
        const int perProducer = 2_000;

        await RunOnThreads(
            producers,
            _ =>
            {
                for (var i = 0; i < perProducer; i++)
                {
                    logger.LogInformation("order {OrderId} for {Region}", i, "eu");
                }
            }
        );
        await provider.DisposeAsync();

        Assert.Equal(producers * perProducer, sink.Written + provider.Dropped);
        Assert.True(sink.Written > 0);
        sink.AssertEveryLineIsJson();
    }

    /// <summary>
    /// Hypothesis: producers blocked on a full queue behind a stuck sink are released by disposal
    /// within its bound, none of them throws, and their records are counted as dropped.
    /// </summary>
    [Fact]
    public async Task Disposal_releases_producers_blocked_behind_a_stuck_sink()
    {
        var sink = new ChaosSink();
        var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions
            {
                QueueCapacity = 4,
                BatchSize = 1,
                QueueFullPolicy = QueueFullPolicy.Block,
                ShutdownTimeout = TimeSpan.FromMilliseconds(100),
            }
        );
        var logger = provider.CreateLogger("Chaos");
        sink.Stall(ignoreCancellation: true);
        logger.LogInformation("first");
        Assert.True(
            sink.Stalled.Wait(Bound, TestContext.Current.CancellationToken),
            "the writer never reached the stalled sink"
        );

        const int producers = 4;
        const int perProducer = 25;
        var failures = 0;
        var blocked = RunOnThreads(
            producers,
            _ =>
            {
                for (var i = 0; i < perProducer; i++)
                {
                    try
                    {
                        logger.LogInformation("inventory {Index}", i);
                    }
                    catch (Exception)
                    {
                        Interlocked.Increment(ref failures);
                    }
                }
            }
        );

        // The producers are parked once the queue is full; nothing moves until disposal.
        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.False(blocked.IsCompleted, "producers finished although the queue could not drain");

        var watch = Stopwatch.StartNew();
        await provider
            .DisposeAsync()
            .AsTask()
            .WaitAsync(Bound, TestContext.Current.CancellationToken);
        await blocked.WaitAsync(Bound, TestContext.Current.CancellationToken);

        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(5), $"disposal took {watch.Elapsed}");
        Assert.Equal(0, failures);
        Assert.Equal(1 + (producers * perProducer), sink.Written + provider.Dropped);
        sink.Resume();
    }

    /// <summary>
    /// Hypothesis: a stream mixing ordinary records with poisonous ones (a throwing ToString, a
    /// type that cannot be reflected, a cycle, thousands of fields) costs only the poisoned
    /// values: callers never throw, every ordinary record arrives, and every line is valid JSON.
    /// </summary>
    [Fact]
    public async Task Poisoned_records_cost_only_their_poisoned_values()
    {
        var sink = new ChaosSink();
        var provider = new HostLoomLoggerProvider(
            new ClefLogFormatter(),
            sink,
            new HostLoomLoggerOptions
            {
                QueueFullPolicy = QueueFullPolicy.Block,
                AttachMachineName = false,
            }
        );
        var logger = provider.CreateLogger("Chaos");
        var cycle = new Node();
        cycle.Next = cycle;
        var wide = Enumerable
            .Range(0, 5_000)
            .Select(i => new KeyValuePair<string, object?>($"field{i}", i))
            .ToList();
        const int rounds = 200;
        var failures = 0;

        await RunOnThreads(
            2,
            _ =>
            {
                for (var i = 0; i < rounds; i++)
                {
                    try
                    {
                        logger.LogInformation("ordinary {Index}", i);
                        logger.LogInformation("poison {Value}", new ThrowingToString());
                        logger.LogInformation("poison {@Value}", new[] { new Unreflectable() });
                        logger.LogInformation("poison {@Value}", cycle);
                        logger.Log(
                            LogLevel.Information,
                            default,
                            wide,
                            null,
                            static (_, _) => "poison wide"
                        );
                    }
                    catch (Exception)
                    {
                        Interlocked.Increment(ref failures);
                    }
                }
            }
        );
        await provider.DisposeAsync();

        Assert.Equal(0, failures);
        Assert.Equal(0, provider.Dropped);
        Assert.Equal(2 * rounds * 5, sink.Written);
        Assert.Equal(
            2 * rounds,
            sink.Lines()
                .Count(line =>
                    line.Contains("\"@mt\":\"ordinary {Index}\"", StringComparison.Ordinal)
                )
        );
        sink.AssertEveryLineIsJson();
    }

    private static Task RunOnThreads(int count, Action<int> body)
    {
        var threads = Enumerable
            .Range(0, count)
            .Select(index =>
            {
                var done = new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously
                );
                var thread = new Thread(() =>
                {
                    try
                    {
                        body(index);
                        done.TrySetResult();
                    }
                    catch (Exception exception)
                    {
                        done.TrySetException(exception);
                    }
                })
                {
                    IsBackground = true,
                };
                thread.Start();
                return done.Task;
            })
            .ToArray();
        return Task.WhenAll(threads);
    }

    private sealed class Node
    {
        public Node? Next { get; set; }
    }

    private sealed class ThrowingToString
    {
        public override string ToString() => throw new InvalidOperationException("ToString");
    }

    [AttributeUsage(AttributeTargets.Property)]
    private sealed class ThrowingAttribute : Attribute
    {
        public ThrowingAttribute() => throw new InvalidOperationException("attribute");
    }

    private sealed class Unreflectable
    {
        [Throwing]
        public int Value { get; set; } = 1;
    }

    /// <summary>A sink whose writes can be stalled, briefly or until resumed.</summary>
    internal sealed class ChaosSink : ILogSink
    {
        // CA2213: left undisposed on purpose. Tests read the sink after the provider disposed it,
        // and a write the provider abandoned may still be waiting on this gate.
#pragma warning disable CA2213
        private readonly ManualResetEventSlim _open = new(true);
#pragma warning restore CA2213
        private readonly Lock _gate = new();
        private readonly List<string> _lines = [];
        private readonly List<(string Text, TaskCompletionSource Seen)> _waiters = [];

        // Pulsed on every write, for waits that must not need a thread-pool thread.
        private readonly object _arrived = new();
        private bool _ignoreCancellation;
        private long _writes;

        public ManualResetEventSlim Stalled { get; } = new(false);

        public int StallEvery { get; init; }

        public TimeSpan StallFor { get; init; }

        public long Written
        {
            get
            {
                lock (_gate)
                {
                    return _lines.Count;
                }
            }
        }

        public void Stall(bool ignoreCancellation = false)
        {
            _ignoreCancellation = ignoreCancellation;
            _open.Reset();
        }

        public void Resume() => _open.Set();

        public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken)
        {
            if (!_open.IsSet)
            {
                Stalled.Set();
                if (_ignoreCancellation)
                {
                    _open.Wait(CancellationToken.None);
                }
                else
                {
                    WaitHandle.WaitAny([_open.WaitHandle, cancellationToken.WaitHandle]);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            }

            if (StallEvery > 0 && Interlocked.Increment(ref _writes) % StallEvery == 0)
            {
                Thread.Sleep(StallFor);
            }

            var text = Encoding.UTF8.GetString(payload);
            lock (_gate)
            {
                foreach (var line in text.Split('\n', StringSplitOptions.RemoveEmptyEntries))
                {
                    _lines.Add(line);
                    foreach (
                        var waiter in _waiters.Where(waiter =>
                            line.Contains(waiter.Text, StringComparison.Ordinal)
                        )
                    )
                    {
                        waiter.Seen.TrySetResult();
                    }
                }
            }

            lock (_arrived)
            {
                Monitor.PulseAll(_arrived);
            }
        }

        /// <summary>
        /// Blocks the calling thread until a line containing <paramref name="text"/> arrives. Unlike
        /// <see cref="WaitForLine"/>, whose asynchronous continuation needs a thread-pool thread,
        /// this wakes on the writer's own signal, so it works while the pool is starved.
        /// </summary>
        public bool WaitForLineBlocking(string text, TimeSpan timeout)
        {
            var deadline =
                Stopwatch.GetTimestamp() + (long)(timeout.TotalSeconds * Stopwatch.Frequency);
            lock (_arrived)
            {
                while (!Lines().Any(line => line.Contains(text, StringComparison.Ordinal)))
                {
                    var remaining = Stopwatch.GetElapsedTime(Stopwatch.GetTimestamp(), deadline);
                    if (deadline <= Stopwatch.GetTimestamp())
                    {
                        return false;
                    }

                    Monitor.Wait(_arrived, remaining);
                }
            }

            return true;
        }

        public Task WaitForLine(string text, TimeSpan timeout)
        {
            var seen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            lock (_gate)
            {
                if (_lines.Any(line => line.Contains(text, StringComparison.Ordinal)))
                {
                    return Task.CompletedTask;
                }

                _waiters.Add((text, seen));
            }

            return seen.Task.WaitAsync(timeout);
        }

        public string[] Lines()
        {
            lock (_gate)
            {
                return [.. _lines];
            }
        }

        public void AssertEveryLineIsJson()
        {
            foreach (var line in Lines())
            {
                using var document = JsonDocument.Parse(line);
            }
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

/// <summary>
/// Runs alone: it saturates the process-wide thread pool, which would starve every test running
/// beside it, and restores the pool's limits before it returns.
/// </summary>
[CollectionDefinition(nameof(ThreadPoolStarvation), DisableParallelization = true)]
public sealed class ThreadPoolStarvation;

[Collection(nameof(ThreadPoolStarvation))]
public sealed class LoggingThreadPoolStarvationTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Hypothesis: with every thread-pool worker busy and the pool unable to grow, a record still
    /// reaches the sink, a blocked Warning still returns at EnqueueTimeout, and disposal still
    /// returns at ShutdownTimeout. None of these may wait for a pool thread.
    /// </summary>
    [Fact]
    public void Logging_bounds_hold_while_the_thread_pool_is_starved()
    {
        var delivery = new LoggingChaosTests.ChaosSink();
        var deliveryProvider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            delivery,
            new HostLoomLoggerOptions()
        );
        var stuck = new LoggingChaosTests.ChaosSink();
        var stuckProvider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            stuck,
            new HostLoomLoggerOptions
            {
                QueueCapacity = 1,
                BatchSize = 1,
                QueueFullPolicy = QueueFullPolicy.Block,
                EnqueueTimeout = TimeSpan.FromMilliseconds(100),
                ShutdownTimeout = TimeSpan.FromMilliseconds(100),
            }
        );
        var stuckLogger = stuckProvider.CreateLogger("Starved");
        stuck.Stall(ignoreCancellation: true);
        stuckLogger.LogWarning("first");
        Assert.True(
            stuck.Stalled.Wait(Bound, TestContext.Current.CancellationToken),
            "the writer never reached the stalled sink"
        );
        stuckLogger.LogWarning("fills the queue");

        ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);
        ThreadPool.GetMinThreads(out var minWorkers, out var minIo);
        // Never disposed: parked work items the pool has not started by the end of the test call
        // Wait afterwards, and a disposed event throws there, on a pool thread, ending the process.
#pragma warning disable CA2000
        var parked = new ManualResetEventSlim(false);
#pragma warning restore CA2000
        try
        {
            ThreadPool.SetMaxThreads(minWorkers, minIo);
            for (var i = 0; i < minWorkers * 4; i++)
            {
                ThreadPool.UnsafeQueueUserWorkItem(_ => parked.Wait(), null);
            }

            Thread.Sleep(200);

            var deliveryTime = OnOwnThread(() =>
            {
                deliveryProvider.CreateLogger("Starved").LogInformation("delivered while starved");
                return delivery.WaitForLineBlocking("delivered while starved", Bound);
            });
            var enqueueTime = OnOwnThread(() =>
            {
                stuckLogger.LogWarning("blocks");
                return true;
            });
            var shutdownTime = OnOwnThread(() =>
            {
                stuckProvider.Dispose();
                return true;
            });

            Assert.True(deliveryTime < TimeSpan.FromSeconds(2), $"delivery took {deliveryTime}");
            Assert.True(
                enqueueTime < TimeSpan.FromSeconds(2),
                $"the 100 ms enqueue bound took {enqueueTime}"
            );
            Assert.True(
                shutdownTime < TimeSpan.FromSeconds(3),
                $"the 100 ms shutdown bound took {shutdownTime}"
            );
        }
        finally
        {
            parked.Set();
            ThreadPool.SetMaxThreads(maxWorkers, maxIo);
            stuck.Resume();
            deliveryProvider.Dispose();
        }
    }

    /// <summary>Runs <paramref name="action"/> on a thread of its own and times it; a call that
    /// never returns fails at <see cref="Bound"/> instead of hanging the run.</summary>
    private static TimeSpan OnOwnThread(Func<bool> action)
    {
        var succeeded = false;
        var watch = Stopwatch.StartNew();
        var thread = new Thread(() => succeeded = action()) { IsBackground = true };
        thread.Start();
        var returned = thread.Join(Bound);
        watch.Stop();
        Assert.True(returned && succeeded, $"the call did not complete within {Bound}");
        return watch.Elapsed;
    }
}
