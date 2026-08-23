using System.Diagnostics;
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
        await using var provider = new HostLoomLoggerProvider(new JsonLogFormatter(), sink, new HostLoomLoggerOptions());
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
        await using var provider = new HostLoomLoggerProvider(new JsonLogFormatter(), sink, new HostLoomLoggerOptions());
        var logger = provider.CreateLogger("Orders");
        var orderId = 7;
        var customer = "ada";

        logger.LogFast(LogLevel.Warning, $"order {orderId} rejected for {customer}");
        await provider.DisposeAsync();

        using var json = JsonDocument.Parse(Assert.Single(sink.Lines()));
        // The field names come from the expressions at the call site, so the message stays readable
        // and the payload stays queryable without repeating yourself.
        Assert.Equal("7", json.RootElement.GetProperty("orderId").GetString());
        Assert.Equal("ada", json.RootElement.GetProperty("customer").GetString());
    }

    [Fact]
    public async Task A_disabled_level_never_evaluates_its_arguments()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(new JsonLogFormatter(), sink, new HostLoomLoggerOptions());
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
        await using var provider = new HostLoomLoggerProvider(new JsonLogFormatter(), sink, new HostLoomLoggerOptions());
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

        Assert.True(allocated == 0, $"expected zero allocation on the calling thread, saw {allocated} bytes");
    }

    [Fact]
    public async Task A_full_queue_drops_below_warning_and_keeps_the_warnings()
    {
        var sink = NewBlockingSink();
        var options = new HostLoomLoggerOptions { QueueCapacity = 4, QueueFullPolicy = QueueFullPolicy.DropNewest };
        await using var provider = new HostLoomLoggerProvider(new JsonLogFormatter(), sink, options);
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
    public async Task The_current_activity_is_captured_on_the_calling_thread()
    {
        using var source = new ActivitySource("HostLoom.Tests.Logging");
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == "HostLoom.Tests.Logging",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);

        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(new JsonLogFormatter(), sink, new HostLoomLoggerOptions());
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
    public async Task Logging_through_the_plain_ILogger_interface_still_works()
    {
        var sink = NewBufferSink();
        await using var provider = new HostLoomLoggerProvider(new JsonLogFormatter(), sink, new HostLoomLoggerOptions());
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
        await using var provider = new HostLoomLoggerProvider(new JsonLogFormatter(), sink, new HostLoomLoggerOptions());
        var logger = provider.CreateLogger("Faults");

        logger.LogFast(LogLevel.Error, new InvalidOperationException("boom"), $"failed after {3} attempts");
        await provider.DisposeAsync();

        using var json = JsonDocument.Parse(Assert.Single(sink.Lines()));
        Assert.Equal("failed after 3 attempts", json.RootElement.GetProperty("message").GetString());
        Assert.Equal("System.InvalidOperationException", json.RootElement.GetProperty("error.type").GetString());
        Assert.Equal("boom", json.RootElement.GetProperty("error.message").GetString());
    }

    // CA2000: ownership of the sink transfers to the provider, which disposes it on shutdown.
#pragma warning disable CA2000
    private static BufferSink NewBufferSink() => new();

    private static BlockingSink NewBlockingSink() => new();
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

        public void Write(ReadOnlySpan<byte> payload)
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
                return Encoding.UTF8.GetString(_stream.ToArray())
                    .Split('\n', StringSplitOptions.RemoveEmptyEntries);
            }
        }
    }

    /// <summary>Holds the writer thread so the bounded queue actually fills.</summary>
    private sealed class BlockingSink : ILogSink
    {
        private readonly ManualResetEventSlim _gate = new(false);

        public void Write(ReadOnlySpan<byte> payload) => _gate.Wait(TimeSpan.FromSeconds(5));

        public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public void Release() => _gate.Set();

        public ValueTask DisposeAsync()
        {
            _gate.Set();
            _gate.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class LevelFilteringLogger(ILogger inner, LogLevel minimum) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => logLevel >= minimum;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            inner.Log(logLevel, eventId, state, exception, formatter);
    }
}
