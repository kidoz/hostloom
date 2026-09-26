using System.Buffers;
using System.Collections;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

// The ordinary MEL path and dynamic invalid formats are intentional regression inputs.
#pragma warning disable CA1873

namespace HostLoom.Tests;

public sealed class LoggingProductionReadinessTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void Fast_formattables_have_bounded_buffers_even_after_the_message_is_full(
        bool neverFits,
        bool messageFull
    )
    {
        var entry = new LogEntry();
        entry.ApplyCaps(
            new HostLoomLoggerOptions { MaxMessageLength = 32, MaxTextFieldLength = 32 }
        );
        if (messageFull)
        {
            entry.AppendLiteral(new string('x', 64));
        }
        var value = new OversizedFormattable(neverFits);
        var before = GC.GetAllocatedBytesForCurrentThread();
        entry.AppendFormattable(value, null, "Value", LogFieldKind.Text);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.InRange(value.LargestBuffer, 1, 128);
        Assert.InRange(value.Attempts, 1, 8);
        Assert.True(allocated < 64 * 1024, $"Capture allocated {allocated} bytes.");
        Assert.True(entry.Message.Length <= 35);
        entry.GetField(0, out _, out var field, out var kind);
        Assert.Equal(LogFieldKind.Text, kind);
        Assert.Equal("…", Encoding.UTF8.GetString(field));
    }

    [Fact]
    public void Oversized_numeric_formats_keep_the_complete_canonical_number()
    {
        var entry = new LogEntry();
        entry.ApplyCaps(new HostLoomLoggerOptions { MaxMessageLength = 4, MaxTextFieldLength = 1 });
        var before = GC.GetAllocatedBytesForCurrentThread();
        entry.AppendFormattable(12345, "D1000000", "Count", LogFieldKind.Number);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(allocated < 64 * 1024, $"Capture allocated {allocated} bytes.");
        Assert.Equal("…", Encoding.UTF8.GetString(entry.Message));
        entry.GetField(0, out _, out var field, out var kind);
        Assert.Equal(LogFieldKind.Number, kind);
        Assert.Equal("12345", Encoding.UTF8.GetString(field));
    }

    [Theory]
    [InlineData("text", false)]
    [InlineData("text", true)]
    [InlineData("formattable", false)]
    [InlineData("utf8", false)]
    [InlineData("utf8", true)]
    public async Task Fast_formatting_failures_keep_the_event_and_are_counted(
        string mode,
        bool wrapped
    )
    {
        using var metrics = new ProviderMetrics();
        await using var sink = new CollectingSink();
        await using var provider = metrics.Create(new JsonLogFormatter(), sink, new());
        using var factory = LoggerFactory.Create(builder => builder.AddProvider(provider));
        var logger = wrapped ? factory.CreateLogger("Capture") : provider.CreateLogger("Capture");
        object value = mode switch
        {
            "formattable" => new ThrowingFormattable(),
            "utf8" => new ThrowingUtf8(),
            _ => new ThrowingText(),
        };

        logger.LogFast(LogLevel.Information, $"before {value} after");
        Assert.True(provider.Flush(TimeSpan.FromSeconds(5)));
        using var json = JsonDocument.Parse(Assert.Single(sink.Lines()));
        Assert.Equal(
            "before [DestructuringFailed] after",
            json.RootElement.GetProperty("message").GetString()
        );
        Assert.Equal("[DestructuringFailed]", json.RootElement.GetProperty("value").GetString());
        Assert.Equal(1, metrics.CaptureFailures);
        Assert.Equal(0, provider.Dropped);
        Assert.Null(provider.WriterFault);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Invalid_display_formats_preserve_canonical_numbers(bool clef, bool wrapped)
    {
        using var metrics = new ProviderMetrics();
        await using var sink = new CollectingSink();
        await using var provider = metrics.Create(
            clef ? new ClefLogFormatter() : new JsonLogFormatter(),
            sink,
            new()
        );
        using var factory = LoggerFactory.Create(builder => builder.AddProvider(provider));
        var logger = wrapped ? factory.CreateLogger("Capture") : provider.CreateLogger("Capture");
        var count = 42;
        var next = 17;

        logger.LogFast(LogLevel.Information, $"before {count:Q} after {next}");
        Assert.True(provider.Flush(TimeSpan.FromSeconds(5)));
        using var json = JsonDocument.Parse(Assert.Single(sink.Lines()));
        if (wrapped)
        {
            // The standard-interface handoff transports all LogFast fields as strings.
            Assert.Equal("42", json.RootElement.GetProperty("count").GetString());
            Assert.Equal("17", json.RootElement.GetProperty("next").GetString());
        }
        else
        {
            Assert.Equal(42, json.RootElement.GetProperty("count").GetInt32());
            Assert.Equal(17, json.RootElement.GetProperty("next").GetInt32());
        }
        Assert.Equal(
            "before [DestructuringFailed] after 17",
            json.RootElement.GetProperty(clef ? "@m" : "message").GetString()
        );
        Assert.Equal(1, metrics.CaptureFailures);
        Assert.Equal(0, provider.Dropped);
        Assert.Null(provider.WriterFault);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Nested_scalar_failures_keep_complete_valid_json(bool dictionary, bool clef)
    {
        await using var sink = new CollectingSink();
        await using var provider = new HostLoomLoggerProvider(
            clef ? new ClefLogFormatter() : new JsonLogFormatter(),
            sink,
            new()
        );
        object value = dictionary
            ? new Hashtable { ["Broken"] = new ThrowingUri(), ["Region"] = "eu" }
            : new[] { new { Broken = new ThrowingUri(), Region = "eu" } };

        provider.CreateLogger("Capture").LogInformation("captured {@Value}", value);
        Assert.True(provider.Flush(TimeSpan.FromSeconds(5)));
        using var json = JsonDocument.Parse(Assert.Single(sink.Lines()));
        var captured = json.RootElement.GetProperty("Value");
        if (!dictionary)
            captured = captured[0];
        Assert.Equal("[DestructuringFailed]", captured.GetProperty("Broken").GetString());
        Assert.Equal("eu", captured.GetProperty("Region").GetString());
        Assert.Equal(0, provider.Dropped);
    }

    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 3)]
    [InlineData(true, 0)]
    [InlineData(true, 3)]
    public async Task Abandonment_counts_records_in_formatting_and_excludes_flush_markers(
        bool holdFormatter,
        int flushes
    )
    {
        using var gate = new HeldOperation();
        using var metrics = new ProviderMetrics();
        await using var sink = new HeldSink(gate, !holdFormatter);
        var formatter = new HeldFormatter(gate, holdFormatter);
        await using var provider = metrics.Create(
            formatter,
            sink,
            new()
            {
                QueueCapacity = 16,
                BatchSize = 1,
                ShutdownTimeout = TimeSpan.FromMilliseconds(20),
            }
        );
        var logger = provider.CreateLogger("Shutdown");
        try
        {
            logger.LogInformation("held record");
            await gate.Entered.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken
            );
            for (var i = 0; i < 3; i++)
                logger.LogInformation("queued {Index}", i);
            for (var i = 0; i < flushes; i++)
                Assert.False(provider.Flush(TimeSpan.Zero));

            await provider
                .DisposeAsync()
                .AsTask()
                .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.Equal(4, provider.Dropped);
            Assert.Equal(4, metrics.Dropped);
        }
        finally
        {
            gate.Release.Set();
        }

        // A closed pipeline waits for the writer to finish before returning false. Its late
        // cancellation cleanup must neither leak records nor count the abandoned ones twice.
        Assert.False(provider.Flush(TimeSpan.FromSeconds(5)));
        Assert.Equal(4, provider.Dropped);
        logger.LogInformation("after disposal");
        Assert.Equal(5, provider.Dropped);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Unreadable_nested_memory_preserves_sibling_members(bool dictionary)
    {
        using var memory = new UnreadableMemory();
        await using var sink = new CollectingSink();
        await using var provider = new HostLoomLoggerProvider(new JsonLogFormatter(), sink, new());
        object value = dictionary
            ? new Hashtable { ["Payload"] = memory.Value, ["Region"] = "eu" }
            : new[] { new { Payload = memory.Value, Region = "eu" } };
        var logger = provider.CreateLogger("Capture");

        logger.LogInformation("captured {@Value}", value);
        logger.LogInformation("next {@Value}", new { Region = "eu" });
        Assert.True(provider.Flush(TimeSpan.FromSeconds(5)));
        var lines = sink.Lines();
        Assert.Equal(2, lines.Length);
        using var failed = JsonDocument.Parse(lines[0]);
        var captured = failed.RootElement.GetProperty("Value");
        if (!dictionary)
            captured = captured[0];
        Assert.Equal("[DestructuringFailed]", captured.GetProperty("Payload").GetString());
        Assert.Equal("eu", captured.GetProperty("Region").GetString());
        using var next = JsonDocument.Parse(lines[1]);
        Assert.Equal("eu", next.RootElement.GetProperty("Value").GetProperty("Region").GetString());
        Assert.Equal(0, provider.Dropped);
        Assert.Null(provider.WriterFault);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task One_unreadable_memory_member_preserves_all_collection_items(
        bool readOnly,
        bool clef
    )
    {
        using var memory = new UnreadableMemory();
        using var metrics = new ProviderMetrics();
        await using var sink = new CollectingSink();
        var options = new HostLoomLoggerOptions();
        options.Destructuring.MaxCollectionItems = 100;
        await using var provider = metrics.Create(
            clef ? new ClefLogFormatter() : new JsonLogFormatter(),
            sink,
            options
        );
        object broken = readOnly ? (ReadOnlyMemory<byte>)memory.Value : (object)memory.Value;
        var items = Enumerable
            .Range(0, 100)
            .Select(i => new
            {
                Index = i,
                Payload = i == 50 ? broken : new byte[] { (byte)i },
                Region = "eu",
            })
            .ToArray();

        provider.CreateLogger("Capture").LogInformation("captured {@Items}", (object)items);
        Assert.True(provider.Flush(TimeSpan.FromSeconds(5)));
        using var json = JsonDocument.Parse(Assert.Single(sink.Lines()));
        var captured = json.RootElement.GetProperty("Items");
        Assert.Equal(100, captured.GetArrayLength());
        for (var i = 0; i < 100; i++)
        {
            Assert.Equal(i, captured[i].GetProperty("Index").GetInt32());
            Assert.Equal(
                i == 50 ? "[DestructuringFailed]" : Convert.ToHexString(new byte[] { (byte)i }),
                captured[i].GetProperty("Payload").GetString()
            );
            Assert.Equal("eu", captured[i].GetProperty("Region").GetString());
        }
        Assert.Equal(1, metrics.DestructuringFailures);
        Assert.Equal(0, provider.Dropped);
        Assert.Null(provider.WriterFault);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Unreadable_scalar_memory_preserves_later_fields(bool readOnly)
    {
        using var memory = new UnreadableMemory();
        using var metrics = new ProviderMetrics();
        await using var sink = new CollectingSink();
        await using var provider = metrics.Create(new JsonLogFormatter(), sink, new());
        object broken = readOnly ? (ReadOnlyMemory<byte>)memory.Value : (object)memory.Value;

        provider.CreateLogger("Capture").LogInformation("captured {@Payload} {Count}", broken, 42);
        Assert.True(provider.Flush(TimeSpan.FromSeconds(5)));
        using var json = JsonDocument.Parse(Assert.Single(sink.Lines()));
        Assert.Equal("[DestructuringFailed]", json.RootElement.GetProperty("Payload").GetString());
        Assert.Equal(42, json.RootElement.GetProperty("Count").GetInt32());
        Assert.Equal(1, metrics.DestructuringFailures);
        Assert.Equal(0, provider.Dropped);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Failed_flush_still_disposes_owned_streams_but_leaves_borrowed_streams_open(
        bool leaveOpen
    )
    {
        await using var stream = new FlushFailingStream();
        // The provider owns the sink, while the stream remains test-owned for final cleanup.
#pragma warning disable CA2000
        var sink = new StreamLogSink(stream, leaveOpen);
#pragma warning restore CA2000
        await using var provider = new HostLoomLoggerProvider(new JsonLogFormatter(), sink, new());
        provider.CreateLogger("Stream").LogInformation("record");
        await provider.DisposeAsync();
        Assert.Equal(!leaveOpen, stream.WasDisposed);
    }

    private sealed class OversizedFormattable(bool neverFits) : IUtf8SpanFormattable
    {
        public int LargestBuffer { get; private set; }
        public int Attempts { get; private set; }

        public bool TryFormat(
            Span<byte> destination,
            out int bytesWritten,
            ReadOnlySpan<char> format,
            IFormatProvider? provider
        )
        {
            Attempts++;
            LargestBuffer = Math.Max(LargestBuffer, destination.Length);
            bytesWritten = 0;
            // Fail the test safely if a regression starts allocating enormous buffers.
            if (Attempts > 32 || destination.Length > 4096)
                throw new InvalidOperationException("unbounded formatting");
            if (neverFits || destination.Length < 1024 * 1024)
                return false;
            destination[..(1024 * 1024)].Fill((byte)'x');
            bytesWritten = 1024 * 1024;
            return true;
        }
    }

    private sealed class ThrowingText
    {
        public override string ToString() => throw new InvalidOperationException("text failed");
    }

    private sealed class ThrowingFormattable : IFormattable
    {
        public string ToString(string? format, IFormatProvider? formatProvider) =>
            throw new FormatException("format failed");
    }

    private sealed class ThrowingUtf8 : IUtf8SpanFormattable
    {
        public bool TryFormat(
            Span<byte> utf8Destination,
            out int bytesWritten,
            ReadOnlySpan<char> format,
            IFormatProvider? provider
        ) => throw new FormatException("format failed");
    }

    private sealed class ThrowingUri() : Uri("https://example.com")
    {
        public override string ToString() => throw new InvalidOperationException("uri failed");
    }

    private sealed class UnreadableMemory : MemoryManager<byte>
    {
        public Memory<byte> Value => CreateMemory(1);

        public override Span<byte> GetSpan() =>
            throw new InvalidOperationException("memory unavailable");

        public override MemoryHandle Pin(int elementIndex = 0) => throw new NotSupportedException();

        public override void Unpin() { }

        protected override void Dispose(bool disposing) { }
    }

    private sealed class CollectingSink : ILogSink
    {
        private readonly ConcurrentQueue<string> _lines = new();

        public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken)
        {
            foreach (
                var line in Encoding
                    .UTF8.GetString(payload)
                    .Split('\n', StringSplitOptions.RemoveEmptyEntries)
            )
                _lines.Enqueue(line);
        }

        public string[] Lines() => _lines.ToArray();

        public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class HeldOperation : IDisposable
    {
        public TaskCompletionSource Entered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ManualResetEventSlim Release { get; } = new(false);

        public void Wait()
        {
            Entered.TrySetResult();
            if (!Release.Wait(TimeSpan.FromSeconds(10)))
                throw new TimeoutException("test did not release operation");
        }

        public void Dispose() => Release.Dispose();
    }

    private sealed class HeldFormatter(HeldOperation gate, bool hold) : ILogFormatter
    {
        private readonly JsonLogFormatter _inner = new();

        public void Format(in LogRecord record, IBufferWriter<byte> writer)
        {
            if (hold)
                gate.Wait();
            _inner.Format(record, writer);
        }
    }

    private sealed class HeldSink(HeldOperation gate, bool hold) : ILogSink
    {
        public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken)
        {
            if (hold)
                gate.Wait();
            cancellationToken.ThrowIfCancellationRequested();
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FlushFailingStream : MemoryStream
    {
        public bool WasDisposed { get; private set; }

        public override Task FlushAsync(CancellationToken cancellationToken) =>
            Task.FromException(new IOException("flush failed"));

        protected override void Dispose(bool disposing)
        {
            WasDisposed = true;
            base.Dispose(disposing);
        }
    }

    private sealed class ProviderMetrics : IDisposable
    {
        private readonly MeterListener _listener = new();
        private int _constructingThread = -1;
        private long _captureFailures;
        private long _destructuringFailures;
        private long _dropped;
        public long CaptureFailures => Interlocked.Read(ref _captureFailures);
        public long DestructuringFailures => Interlocked.Read(ref _destructuringFailures);
        public long Dropped => Interlocked.Read(ref _dropped);

        public ProviderMetrics()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (
                    instrument.Meter.Name == "HostLoom.Logging"
                    && Environment.CurrentManagedThreadId == Volatile.Read(ref _constructingThread)
                )
                    listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<long>(
                (instrument, count, tags, _) =>
                {
                    if (instrument.Name == "hostloom.logging.records.dropped")
                        Interlocked.Add(ref _dropped, count);
                    if (instrument.Name != "hostloom.logging.failures")
                        return;
                    foreach (var tag in tags)
                    {
                        if (tag.Key == "component" && tag.Value is "capture")
                            Interlocked.Add(ref _captureFailures, count);
                        if (tag.Key == "component" && tag.Value is "destructurer")
                            Interlocked.Add(ref _destructuringFailures, count);
                    }
                }
            );
            _listener.Start();
        }

        public HostLoomLoggerProvider Create(
            ILogFormatter formatter,
            ILogSink sink,
            HostLoomLoggerOptions options
        )
        {
            Volatile.Write(ref _constructingThread, Environment.CurrentManagedThreadId);
            try
            {
                return new HostLoomLoggerProvider(formatter, sink, options);
            }
            finally
            {
                Volatile.Write(ref _constructingThread, -1);
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
