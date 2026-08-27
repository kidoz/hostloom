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
            new HostLoomLoggerProvider(formatter, sink, new HostLoomLoggerOptions { BatchSize = 0 })
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
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new HostLoomLoggerProvider(
                formatter,
                sink,
                new HostLoomLoggerOptions { MaxFieldNameLength = 0 }
            )
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new HostLoomLoggerProvider(
                formatter,
                sink,
                new HostLoomLoggerOptions { MaxFieldsPerRecord = 0 }
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
    public async Task Duplicate_field_names_collapse_to_the_last_occurrence()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Collisions");

        // Two holes carrying the same name with different values — buildable through the handler
        // directly, which is also how scope and enricher fields will arrive later.
        var handler = new LogMessageHandler(0, 2, logger, LogLevel.Information, out var enabled);
        Assert.True(enabled);
        handler.AppendFormatted(1, name: "n");
        handler.AppendLiteral(" then ");
        handler.AppendFormatted(2, name: "n");
        logger.LogFast(LogLevel.Information, ref handler);
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        // Exactly one key survives — a SkipValidation writer would happily emit both otherwise —
        // and it is the later occurrence, matching Serilog's last-wins discipline.
        Assert.Equal(1, root.EnumerateObject().Count(p => p.NameEquals("n")));
        Assert.Equal(2, root.GetProperty("n").GetInt32());
    }

    [Fact]
    public async Task A_field_matching_a_formatter_reserved_name_is_dropped_not_duplicated()
    {
        var reasons = new HashSet<string>();
        var gate = new Lock();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (
                instrument.Meter.Name == "HostLoom.Logging"
                && instrument.Name == "hostloom.logging.fields.dropped"
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

        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Reserved");
        var message = "sneaky";

        logger.LogFast(LogLevel.Information, $"hello {message}");
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        // The hole was named "message" by its variable. The formatter owns that key; the field is
        // dropped and counted rather than shadowing the rendered message for some parsers.
        Assert.Equal(1, root.EnumerateObject().Count(p => p.NameEquals("message")));
        Assert.Equal("hello sneaky", root.GetProperty("message").GetString());
        lock (gate)
        {
            Assert.Contains("reserved_name", reasons);
        }
    }

    [Fact]
    public async Task A_leading_at_field_name_is_escaped_instead_of_colliding()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Escaped");
        var @timestamp = 5;

        logger.LogFast(LogLevel.Information, $"value {@timestamp}");
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        // CLEF-style escaping: the user name doubles its first @, so the real @timestamp is
        // untouchable and the user value still ships under a recoverable name.
        Assert.Equal(1, root.EnumerateObject().Count(p => p.NameEquals("@timestamp")));
        Assert.True(root.GetProperty("@timestamp").TryGetDateTimeOffset(out _));
        Assert.Equal(5, root.GetProperty("@@timestamp").GetInt32());
    }

    [Fact]
    public async Task Fields_beyond_the_record_cap_are_dropped_but_the_record_ships()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions { MaxFieldsPerRecord = 2 }
        );
        var logger = provider.CreateLogger("Capped");
        var first = 1;
        var second = 2;
        var third = 3;

        logger.LogFast(LogLevel.Information, $"caps {first} {second} {third}");
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal("caps 1 2 3", root.GetProperty("message").GetString());
        Assert.Equal(1, root.GetProperty("first").GetInt32());
        Assert.Equal(2, root.GetProperty("second").GetInt32());
        Assert.False(root.TryGetProperty("third", out _));
    }

    [Fact]
    public async Task An_oversized_field_name_drops_the_field_not_the_record()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions { MaxFieldNameLength = 4 }
        );
        var logger = provider.CreateLogger("LongNames");
        var id = 7;
        var protracted = 8;

        logger.LogFast(LogLevel.Information, $"kept {id} dropped {protracted}");
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal("kept 7 dropped 8", root.GetProperty("message").GetString());
        Assert.Equal(7, root.GetProperty("id").GetInt32());
        Assert.False(root.TryGetProperty("protracted", out _));
    }

    [Fact]
    public async Task An_empty_field_name_is_dropped()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("EmptyName");

        var handler = new LogMessageHandler(0, 1, logger, LogLevel.Information, out var enabled);
        Assert.True(enabled);
        handler.AppendLiteral("value ");
        handler.AppendFormatted(5, name: "");
        logger.LogFast(LogLevel.Information, ref handler);
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal("value 5", root.GetProperty("message").GetString());
        Assert.DoesNotContain(root.EnumerateObject(), p => p.Name.Length == 0);
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

        var stamps = sink.Lines()
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
        // The template hole is a queryable typed field — the platform's ~6000 ordinary call
        // sites depend on exactly this.
        Assert.Equal(3, json.RootElement.GetProperty("Count").GetInt32());
    }

    [Fact]
    public async Task LoggerMessage_define_call_sites_keep_their_fields()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Defined");
        var log = LoggerMessage.Define<int, string>(
            LogLevel.Warning,
            new EventId(7, "order-rejected"),
            "order {OrderId} rejected for {Customer}"
        );

        log(logger, 42, "ada", null);
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal("order 42 rejected for ada", root.GetProperty("message").GetString());
        Assert.Equal(42, root.GetProperty("OrderId").GetInt32());
        Assert.Equal("ada", root.GetProperty("Customer").GetString());
        Assert.Equal(7, root.GetProperty("event.code").GetInt32());
    }

    [Fact]
    public async Task The_template_is_preserved_and_never_emitted_as_a_field()
    {
        var formatter = new TemplateCapturingFormatter();
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            formatter,
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Templates");

#pragma warning disable CA1873
        logger.LogInformation("plain {Count} message", 3);
#pragma warning restore CA1873
        await provider.DisposeAsync();

        Assert.Equal("plain {Count} message", formatter.Template);
        // {OriginalFormat} is template metadata for CLEF @mt, not an ordinary property.
        Assert.DoesNotContain("OriginalFormat", formatter.FieldNames);
        Assert.Contains("Count", formatter.FieldNames);
    }

    [Fact]
    public async Task Standard_path_values_keep_their_json_kinds()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("TypedInterop");
        var when = new DateTimeOffset(2026, 8, 26, 10, 30, 0, TimeSpan.Zero);

#pragma warning disable CA1873
        logger.LogInformation(
            "state {Missing} {Flag} {Price} {When} {Day}",
            null,
            true,
            19.99m,
            when,
            DayOfWeek.Tuesday
        );
#pragma warning restore CA1873
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal(JsonValueKind.Null, root.GetProperty("Missing").ValueKind);
        Assert.Equal(JsonValueKind.True, root.GetProperty("Flag").ValueKind);
        Assert.Equal(19.99m, root.GetProperty("Price").GetDecimal());
        Assert.Equal(when, root.GetProperty("When").GetDateTimeOffset());
        Assert.Equal("Tuesday", root.GetProperty("Day").GetString());
    }

    [Fact]
    public async Task Template_operators_strip_their_prefix_from_the_emitted_name()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Operators");

#pragma warning disable CA1873
        logger.LogInformation("thing {@Thing} num {$Num}", new Version(1, 2), 5);
#pragma warning restore CA1873
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        // '@' strips to Thing and destructures the non-scalar into nested JSON; '$' forces the
        // invariant string even for a scalar.
        Assert.Equal(JsonValueKind.Object, root.GetProperty("Thing").ValueKind);
        Assert.Equal(1, root.GetProperty("Thing").GetProperty("Major").GetInt32());
        Assert.Equal(2, root.GetProperty("Thing").GetProperty("Minor").GetInt32());
        Assert.Equal("5", root.GetProperty("Num").GetString());
        Assert.False(root.TryGetProperty("@Thing", out _));
        Assert.False(root.TryGetProperty("$Num", out _));
    }

    [Fact]
    public async Task Custom_enumerable_state_is_captured()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("CustomState");
        var state = new Dictionary<string, object?> { ["UserId"] = 7, ["Region"] = "eu" };

        logger.Log(LogLevel.Information, default, state, null, static (_, _) => "custom state");
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal("custom state", root.GetProperty("message").GetString());
        Assert.Equal(7, root.GetProperty("UserId").GetInt32());
        Assert.Equal("eu", root.GetProperty("Region").GetString());
    }

    [Fact]
    public async Task LogFast_through_a_wrapped_logger_preserves_field_names()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        // The shape of every dependency-injected ILogger<T>: a wrapper, not HostLoom's logger.
        var logger = new LevelFilteringLogger(provider.CreateLogger("Wrapped"), LogLevel.Trace);
        var orderId = 7;

        logger.LogFast(LogLevel.Information, $"order {orderId} shipped");
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal("order 7 shipped", root.GetProperty("message").GetString());
        // Through the fallback the value travels as a string, but the name survives — before,
        // the field vanished entirely.
        Assert.Equal("7", root.GetProperty("orderId").GetString());
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

    /// <summary>Records what the pipeline handed it, so template plumbing is observable.</summary>
    private sealed class TemplateCapturingFormatter : ILogFormatter
    {
        private readonly Lock _gate = new();
        private readonly List<string> _fieldNames = [];

        public string? Template { get; private set; }

        public IReadOnlyList<string> FieldNames
        {
            get
            {
                lock (_gate)
                {
                    return [.. _fieldNames];
                }
            }
        }

        public void Format(in LogRecord record, System.Buffers.IBufferWriter<byte> writer)
        {
            lock (_gate)
            {
                Template = record.Template;
                for (var i = 0; i < record.FieldCount; i++)
                {
                    record.GetField(i, out var name, out _, out _);
                    _fieldNames.Add(Encoding.UTF8.GetString(name));
                }
            }
        }
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

        public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

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

        public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

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
