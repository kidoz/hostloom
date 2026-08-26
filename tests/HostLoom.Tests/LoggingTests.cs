using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

public sealed class LoggingTests
{
    [Fact]
    public async Task A_logged_message_is_written_as_one_json_object_per_line()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Orders");

        logger.LogFast(LogLevel.Information, $"placed order {42} for {"ada"}");
        await provider.DisposeAsync();

        var line = Assert.Single(sink.Lines());
        using var json = JsonDocument.Parse(line);
        var root = json.RootElement;
        Assert.Equal("INFO", root.GetProperty("log.level").GetString());
        Assert.Equal("Orders", root.GetProperty("log.logger").GetString());
        Assert.Equal("placed order 42 for ada", root.GetProperty("message").GetString());
    }

    [Fact]
    public async Task Interpolation_holes_become_named_structured_fields()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Orders");
        var orderId = 7;
        var customer = "ada";

        logger.LogFast(LogLevel.Warning, $"order {orderId} rejected for {customer}");
        await provider.DisposeAsync();

        using var json = JsonDocument.Parse(Assert.Single(sink.Lines()));
        // The field names come from the expressions at the call site, so the message stays readable
        // and the payload stays queryable without repeating yourself. Values keep their JSON types:
        // a numeric hole is a number a dashboard can range-query, not a quoted string.
        Assert.Equal(7, json.RootElement.GetProperty("orderId").GetInt32());
        Assert.Equal("ada", json.RootElement.GetProperty("customer").GetString());
    }

    [Fact]
    public async Task Holes_keep_their_json_value_kinds()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Typed");
        var count = 42L;
        var ratio = 0.25;
        var active = true;
        var price = 19.99m;

        logger.LogFast(LogLevel.Information, $"state {count} {ratio} {active} {price}");
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal(JsonValueKind.Number, root.GetProperty("count").ValueKind);
        Assert.Equal(42L, root.GetProperty("count").GetInt64());
        Assert.Equal(0.25, root.GetProperty("ratio").GetDouble());
        Assert.Equal(JsonValueKind.True, root.GetProperty("active").ValueKind);
        Assert.Equal(19.99m, root.GetProperty("price").GetDecimal());
    }

    [Fact]
    public async Task A_formatted_hole_renders_in_the_message_but_stays_typed()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Typed");
        var orderId = 42;

        logger.LogFast(LogLevel.Information, $"order {orderId:00000} shipped");
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        // The message keeps the human formatting; the field keeps the machine value. "00042" is
        // not a JSON number, so emitting the rendering as the value would corrupt the type.
        Assert.Equal("order 00042 shipped", root.GetProperty("message").GetString());
        Assert.Equal(42, root.GetProperty("orderId").GetInt32());
    }

    [Fact]
    public async Task A_non_finite_double_degrades_to_text_and_the_line_stays_valid_json()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Typed");
        var bad = double.NaN;

        logger.LogFast(LogLevel.Information, $"ratio {bad}");
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal("NaN", root.GetProperty("bad").GetString());
    }

    [Fact]
    public async Task A_datetimeoffset_hole_defaults_to_iso8601()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Typed");
        var when = new DateTimeOffset(2026, 8, 26, 10, 30, 0, TimeSpan.Zero);

        logger.LogFast(LogLevel.Information, $"seen at {when}");
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal("2026-08-26T10:30:00.0000000+00:00", root.GetProperty("when").GetString());
        Assert.Equal(when, root.GetProperty("when").GetDateTimeOffset());
    }

    [Fact]
    public async Task A_disabled_level_never_evaluates_its_arguments()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = new LevelFilteringLogger(provider.CreateLogger("Orders"), LogLevel.Warning);
        var evaluated = 0;

        logger.LogFast(LogLevel.Debug, $"expensive {Expensive(ref evaluated)}");
        await provider.DisposeAsync();

        // The trap every logging library warns about: an argument that still runs when the level is
        // off. The handler reports shouldAppend=false, so the compiler skips the hole entirely.
        Assert.Equal(0, evaluated);
        Assert.Empty(sink.Lines());
    }

    [Fact]
    public async Task The_hot_path_allocates_nothing_once_the_pool_is_warm()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Bench");

        // Warm the entry pool and the buffers it retains; steady state is what the claim is about.
        const int Warm = 2000;
        for (var i = 0; i < Warm; i++)
        {
            logger.LogFast(LogLevel.Information, $"warm {i} {"x"} {true}");
        }

        // Wait for the queue to drain rather than sleeping a guessed interval. Until the writer has
        // returned the warm entries, Rent() is still constructing new ones and the measurement is
        // of pool growth, not of the hot path.
        var deadline = Stopwatch.StartNew();
        while (sink.Lines().Length < Warm && deadline.Elapsed < TimeSpan.FromSeconds(10))
        {
            await Task.Delay(5, TestContext.Current.CancellationToken);
        }

        Assert.Equal(Warm, sink.Lines().Length);

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 128; i++)
        {
            logger.LogFast(LogLevel.Information, $"measured {i} {"x"} {true}");
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await provider.DisposeAsync();

        Assert.True(
            allocated == 0,
            $"expected zero allocation on the calling thread, saw {allocated} bytes"
        );
    }

    [Fact]
    public async Task A_full_queue_drops_below_warning_and_keeps_the_warnings()
    {
        var sink = NewBlockingSink();
        var options = new HostLoomLoggerOptions
        {
            QueueCapacity = 4,
            QueueFullPolicy = QueueFullPolicy.DropNewest,
        };
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            options
        );
        var logger = provider.CreateLogger("Flood");

        for (var i = 0; i < 500; i++)
        {
            logger.LogFast(LogLevel.Information, $"flood {i}");
        }

        // Overload is reported rather than hidden: a silent drop is indistinguishable from no traffic.
        Assert.True(provider.Dropped > 0, "expected the full queue to report drops");
        sink.Release();
        await provider.DisposeAsync();
    }

    [Fact]
    public async Task A_formatter_failure_faults_the_pipeline_and_never_strands_a_caller()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new ThrowingFormatter(),
            sink,
            new HostLoomLoggerOptions { QueueFullPolicy = QueueFullPolicy.Block }
        );
        var logger = provider.CreateLogger("Faulty");

        logger.LogFast(LogLevel.Information, $"first entry breaks the formatter");

        var deadline = Stopwatch.StartNew();
        while (provider.WriterFault is null && deadline.Elapsed < TimeSpan.FromSeconds(10))
        {
            await Task.Delay(5, TestContext.Current.CancellationToken);
        }

        Assert.IsType<InvalidOperationException>(provider.WriterFault);

        // Block would normally wait on a full queue forever. A faulted pipeline must instead turn
        // every later call into a counted, non-blocking no-op — a dead writer that still accepts
        // blocking callers is a service-wide deadlock.
        var before = provider.Dropped;
        logger.LogFast(LogLevel.Error, $"after the fault");
        Assert.Equal(before + 1, provider.Dropped);

        await provider.DisposeAsync();
        Assert.Empty(sink.Lines());
    }

    [Fact]
    public async Task A_stray_cancellation_faults_the_pipeline_instead_of_vanishing()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new CancellingFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Cancelled");

        logger.LogFast(LogLevel.Information, $"triggers a stray cancellation");

        var deadline = Stopwatch.StartNew();
        while (provider.WriterFault is null && deadline.Elapsed < TimeSpan.FromSeconds(10))
        {
            await Task.Delay(5, TestContext.Current.CancellationToken);
        }

        // An OperationCanceledException nobody asked for is a component failure, not a shutdown.
        // Swallowing it would leave the writer dead while producers still see a running pipeline.
        Assert.IsType<OperationCanceledException>(provider.WriterFault);

        var before = provider.Dropped;
        logger.LogFast(LogLevel.Warning, $"after the stray cancellation");
        Assert.Equal(before + 1, provider.Dropped);
    }

    [Fact]
    public async Task Disposal_bounds_a_sink_that_hangs_while_disposing()
    {
        var sink = NewHangingDisposeSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions { ShutdownTimeout = TimeSpan.FromMilliseconds(200) }
        );
        var logger = provider.CreateLogger("HangingDispose");

        logger.LogFast(LogLevel.Information, $"written cleanly before disposal");

        var elapsed = Stopwatch.StartNew();
        await provider.DisposeAsync();

        Assert.True(
            elapsed.Elapsed < TimeSpan.FromSeconds(5),
            $"disposal took {elapsed.Elapsed} against a 200 ms sink-disposal bound"
        );
        sink.Release();
    }

    [Fact]
    public void Invalid_options_fail_at_provider_construction()
    {
        var formatter = new JsonLogFormatter();
        var sink = NewBufferSink();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new HostLoomLoggerProvider(
                formatter,
                sink,
                new HostLoomLoggerOptions { BatchSize = 0 }
            )
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new HostLoomLoggerProvider(
                formatter,
                sink,
                new HostLoomLoggerOptions { QueueCapacity = 0 }
            )
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new HostLoomLoggerProvider(
                formatter,
                sink,
                new HostLoomLoggerOptions { EnqueueTimeout = TimeSpan.Zero }
            )
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new HostLoomLoggerProvider(
                formatter,
                sink,
                new HostLoomLoggerOptions { QueueFullPolicy = (QueueFullPolicy)99 }
            )
        );
        Assert.Throws<ArgumentException>(() =>
            new HostLoomLoggerProvider(
                formatter,
                sink,
                new HostLoomLoggerOptions { TimeProvider = null! }
            )
        );
    }

    [Fact]
    public async Task Disposal_returns_at_the_deadline_when_the_sink_is_stuck()
    {
        var sink = NewStuckSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions { ShutdownTimeout = TimeSpan.FromMilliseconds(200) }
        );
        var logger = provider.CreateLogger("Stuck");

        logger.LogFast(LogLevel.Information, $"taken into the stuck batch");
        sink.WaitUntilWriting();
        for (var i = 0; i < 5; i++)
        {
            logger.LogFast(LogLevel.Information, $"stranded {i}");
        }

        var elapsed = Stopwatch.StartNew();
        await provider.DisposeAsync();

        // The sink ignores cancellation entirely, so this is the worst case: disposal must still
        // return at the deadline (plus the cooperative grace) and count what it left behind —
        // the five queued records plus the one in flight inside the stuck Write.
        Assert.True(
            elapsed.Elapsed < TimeSpan.FromSeconds(5),
            $"disposal took {elapsed.Elapsed} against a 200 ms deadline"
        );
        Assert.Equal(6, provider.Dropped);
        sink.Release();
    }

    [Fact]
    public async Task A_blocking_enqueue_times_out_instead_of_waiting_forever()
    {
        var sink = NewStuckSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions
            {
                QueueCapacity = 1,
                QueueFullPolicy = QueueFullPolicy.Block,
                EnqueueTimeout = TimeSpan.FromMilliseconds(100),
            }
        );
        var logger = provider.CreateLogger("Bounded");

        logger.LogFast(LogLevel.Information, $"taken by the writer");
        sink.WaitUntilWriting();
        logger.LogFast(LogLevel.Information, $"fills the queue");

        var wait = Stopwatch.StartNew();
        logger.LogFast(LogLevel.Information, $"times out");

        Assert.True(
            wait.Elapsed < TimeSpan.FromSeconds(5),
            $"the bounded wait took {wait.Elapsed} against a 100 ms limit"
        );
        Assert.Equal(1, provider.Dropped);
        sink.Release();
    }

    [Fact]
    public async Task Dropped_records_surface_through_the_meter()
    {
        long observed = 0;
        var reasons = new HashSet<string>();
        var gate = new Lock();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (
                instrument.Meter.Name == "HostLoom.Logging"
                && instrument.Name == "hostloom.logging.records.dropped"
            )
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>(
            (instrument, measurement, tags, state) =>
            {
                lock (gate)
                {
                    observed += measurement;
                    foreach (var tag in tags)
                    {
                        if (tag.Key == "reason" && tag.Value is string reason)
                        {
                            reasons.Add(reason);
                        }
                    }
                }
            }
        );
        listener.Start();

        var sink = NewBlockingSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions
            {
                QueueCapacity = 4,
                QueueFullPolicy = QueueFullPolicy.DropNewest,
            }
        );
        var logger = provider.CreateLogger("Metered");
        for (var i = 0; i < 200; i++)
        {
            logger.LogFast(LogLevel.Information, $"flood {i}");
        }

        sink.Release();
        await provider.DisposeAsync();

        lock (gate)
        {
            Assert.True(observed > 0, "expected the meter to observe dropped records");
            Assert.Contains("queue_full", reasons);
        }
    }

    [Fact]
    public async Task The_current_activity_is_captured_on_the_calling_thread()
    {
        using var source = new ActivitySource("HostLoom.Tests.Logging");
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == "HostLoom.Tests.Logging",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);

        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Traced");

        string expected;
        using (var activity = source.StartActivity("work"))
        {
            expected = activity!.TraceId.ToHexString();
            logger.LogFast(LogLevel.Information, $"inside a span");
        }

        await provider.DisposeAsync();

        // Read on the writer thread this would be empty: Activity.Current is ambient per thread.
        using var json = JsonDocument.Parse(Assert.Single(sink.Lines()));
        Assert.Equal(expected, json.RootElement.GetProperty("trace.id").GetString());
    }

    [Fact]
    public async Task Timestamps_follow_the_clock_through_forward_and_backward_steps()
    {
        var clock = new ManualTimeProvider
        {
            Now = new DateTimeOffset(2026, 8, 26, 12, 0, 0, TimeSpan.Zero),
        };
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions { TimeProvider = clock }
        );
        var logger = provider.CreateLogger("Clock");

        logger.LogFast(LogLevel.Information, $"first");
        clock.Now = clock.Now.AddDays(7);
        logger.LogFast(LogLevel.Information, $"after a week");
        // An NTP correction stepping the clock back must show up as-is: a synthesized monotonic
        // timestamp would disagree with every other service on the box.
        clock.Now = clock.Now.AddSeconds(-30);
        logger.LogFast(LogLevel.Information, $"after a backward step");
        await provider.DisposeAsync();

        var stamps = sink
            .Lines()
            .Select(line =>
                JsonDocument.Parse(line).RootElement.GetProperty("@timestamp").GetDateTimeOffset()
            )
            .ToArray();
        Assert.Equal(new DateTimeOffset(2026, 8, 26, 12, 0, 0, TimeSpan.Zero), stamps[0]);
        Assert.Equal(new DateTimeOffset(2026, 9, 2, 12, 0, 0, TimeSpan.Zero), stamps[1]);
        Assert.Equal(new DateTimeOffset(2026, 9, 2, 11, 59, 30, TimeSpan.Zero), stamps[2]);
    }

    [Fact]
    public async Task Logging_through_the_plain_ILogger_interface_still_works()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Interop");

        // What every third-party library calls. It boxes, and that is the point of the comparison.
#pragma warning disable CA1873 // exercising the boxing interop path on purpose
        logger.LogInformation("plain {Count} message", 3);
#pragma warning restore CA1873
        await provider.DisposeAsync();

        using var json = JsonDocument.Parse(Assert.Single(sink.Lines()));
        Assert.Equal("plain 3 message", json.RootElement.GetProperty("message").GetString());
    }

    [Fact]
    public async Task An_exception_is_recorded_without_losing_the_message()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Faults");

        logger.LogFast(
            LogLevel.Error,
            new InvalidOperationException("boom"),
            $"failed after {3} attempts"
        );
        await provider.DisposeAsync();

        using var json = JsonDocument.Parse(Assert.Single(sink.Lines()));
        Assert.Equal(
            "failed after 3 attempts",
            json.RootElement.GetProperty("message").GetString()
        );
        Assert.Equal(
            "System.InvalidOperationException",
            json.RootElement.GetProperty("error.type").GetString()
        );
        Assert.Equal("boom", json.RootElement.GetProperty("error.message").GetString());
    }

    // CA2000: ownership of the sink transfers to the provider, which disposes it on shutdown.
#pragma warning disable CA2000
    private static BufferSink NewBufferSink() => new();

    private static BlockingSink NewBlockingSink() => new();

    private static StuckSink NewStuckSink() => new();

    private static HangingDisposeSink NewHangingDisposeSink() => new();
#pragma warning restore CA2000

    private static int Expensive(ref int counter)
    {
        counter++;
        return counter;
    }

    private sealed class BufferSink : ILogSink
    {
        private readonly MemoryStream _stream = new();
        private readonly Lock _gate = new();

        public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                _stream.Write(payload);
            }
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public string[] Lines()
        {
            lock (_gate)
            {
                return Encoding
                    .UTF8.GetString(_stream.ToArray())
                    .Split('\n', StringSplitOptions.RemoveEmptyEntries);
            }
        }
    }

    /// <summary>Holds the writer thread so the bounded queue actually fills.</summary>
    private sealed class BlockingSink : ILogSink
    {
        private readonly ManualResetEventSlim _gate = new(false);

        // CA2016: deliberately token-deaf — the test controls release through the gate alone.
#pragma warning disable CA2016
        public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken) =>
            _gate.Wait(TimeSpan.FromSeconds(5));
#pragma warning restore CA2016

        public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public void Release() => _gate.Set();

        public ValueTask DisposeAsync()
        {
            _gate.Set();
            _gate.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ThrowingFormatter : ILogFormatter
    {
        public void Format(in LogRecord record, System.Buffers.IBufferWriter<byte> writer) =>
            throw new InvalidOperationException("the formatter broke");
    }

    /// <summary>Throws a cancellation nobody requested — must fault, not stop silently.</summary>
    private sealed class CancellingFormatter : ILogFormatter
    {
        public void Format(in LogRecord record, System.Buffers.IBufferWriter<byte> writer) =>
            throw new OperationCanceledException("a stray cancellation");
    }

    /// <summary>Writes and flushes cleanly, then hangs inside DisposeAsync.</summary>
    private sealed class HangingDisposeSink : ILogSink
    {
        private readonly TaskCompletionSource _release = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken) { }

        public ValueTask FlushAsync(CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public void Release() => _release.TrySetResult();

        public async ValueTask DisposeAsync() =>
            await _release.Task.WaitAsync(TimeSpan.FromSeconds(30));
    }

    /// <summary>
    /// Blocks inside Write and deliberately ignores the cancellation token — the worst-case sink
    /// the bounded-shutdown guarantees are written against.
    /// </summary>
    private sealed class StuckSink : ILogSink
    {
        private readonly ManualResetEventSlim _entered = new(false);
        private readonly ManualResetEventSlim _release = new(false);

        // CA2016: ignoring the token is the entire point of this double — it models the sink the
        // bounded-shutdown guarantees are written against.
#pragma warning disable CA2016
        public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken)
        {
            _entered.Set();
            _release.Wait(TimeSpan.FromSeconds(30));
        }
#pragma warning restore CA2016

        public ValueTask FlushAsync(CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public void WaitUntilWriting() => _entered.Wait(TimeSpan.FromSeconds(10));

        public void Release() => _release.Set();

        public ValueTask DisposeAsync()
        {
            _release.Set();
            _entered.Dispose();
            _release.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ManualTimeProvider : TimeProvider
    {
        public DateTimeOffset Now { get; set; }

        public override DateTimeOffset GetUtcNow() => Now;
    }

    private sealed class LevelFilteringLogger(ILogger inner, LogLevel minimum) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => logLevel >= minimum;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter
        ) => inner.Log(logLevel, eventId, state, exception, formatter);
    }
}
