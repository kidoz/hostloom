using System.Diagnostics;
using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// What a process gets out when it ends without disposing its logging provider, and the flush
/// its exit events run. The writer is a background thread, so without that flush the queued
/// records died with the process: the last lines before a crash were the likeliest to be lost.
/// </summary>
public sealed class LoggingProcessExitTests
{
    [Theory(Timeout = 60_000)]
    [InlineData("exit")]
    [InlineData("return")]
    [InlineData("unhandled")]
    public async Task A_process_that_ends_without_disposing_the_provider_writes_its_queue(
        string ending
    )
    {
        const int records = 2000;
        var cancellation = TestContext.Current.CancellationToken;
        using var process = new Process();
        process.StartInfo = new ProcessStartInfo("dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        process.StartInfo.ArgumentList.Add(
            Path.Combine(AppContext.BaseDirectory, "HostLoom.Logging.ExitProbe.dll")
        );
        process.StartInfo.ArgumentList.Add(ending);
        process.StartInfo.ArgumentList.Add(
            records.ToString(System.Globalization.CultureInfo.InvariantCulture)
        );

        Assert.True(process.Start(), "the probe did not start");
        var output = process.StandardOutput.ReadToEndAsync(cancellation);
        var errors = process.StandardError.ReadToEndAsync(cancellation);
        await process.WaitForExitAsync(cancellation);

        // The probe's sink takes a millisecond per 16-record batch, so hundreds of records are
        // still queued when it ends; every one of them must arrive, in order and intact.
        var lines = (await output).Split('\n', StringSplitOptions.RemoveEmptyEntries);
        var messages = lines.Select(Message).ToArray();
        Assert.True(
            messages.Length == records,
            $"{messages.Length} of {records} records arrived; stderr: {await errors}"
        );
        Assert.Equal(Enumerable.Range(0, records).Select(i => $"record {i}"), messages);
    }

    [Fact]
    public async Task Flush_returns_once_every_record_before_it_is_written_and_flushed()
    {
        await using var sink = new SlowRecordingSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions { QueueFullPolicy = QueueFullPolicy.Block, BatchSize = 1 }
        );
        var logger = provider.CreateLogger("Flushed");
        for (var i = 0; i < 200; i++)
        {
            logger.LogFast(LogLevel.Information, $"record {i}");
        }

        Assert.True(provider.Flush(TimeSpan.FromSeconds(10)), "the flush did not complete");

        // Nothing is awaited between the flush and these reads: it returned only once the last
        // record was written and the sink flushed after it.
        var events = sink.Events();
        Assert.Equal(200, events.Count(e => e.StartsWith("record", StringComparison.Ordinal)));
        Assert.Equal("record 199", events.Last(e => e != "flush"));
        Assert.Equal("flush", events[^1]);

        // The flush leaves the pipeline running, and its marker is never counted as a record.
        logger.LogFast(LogLevel.Information, $"after the flush");
        await provider.DisposeAsync();
        Assert.Equal(0, provider.Dropped);
        Assert.Contains("after the flush", sink.Events());
        Assert.True(provider.Flush(TimeSpan.FromSeconds(1)), "a drained provider has nothing left");
    }

    [Fact]
    public async Task Flush_gives_up_at_its_deadline_when_the_writer_is_stuck()
    {
        using var sink = new GatedSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Stuck");
        logger.LogFast(LogLevel.Information, $"holds the writer");
        Assert.True(sink.WaitUntilEntered(TimeSpan.FromSeconds(10)), "the writer never wrote");
        logger.LogFast(LogLevel.Information, $"queued behind it");

        // A process exit must not hang on a sink that stopped: the flush is bounded.
        var elapsed = Stopwatch.StartNew();
        Assert.False(provider.Flush(TimeSpan.FromMilliseconds(200)));
        Assert.InRange(elapsed.Elapsed, TimeSpan.FromMilliseconds(150), TimeSpan.FromSeconds(5));

        sink.Release();
        await provider.DisposeAsync();
        Assert.Equal(0, provider.Dropped);
    }

    private static string Message(string line)
    {
        using var document = JsonDocument.Parse(line);
        return document.RootElement.GetProperty("message").GetString()!;
    }

    /// <summary>Records each written message and each flush, in order, one record per batch.</summary>
    private sealed class SlowRecordingSink : ILogSink
    {
        private readonly List<string> _events = [];
        private readonly Lock _gate = new();

        public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken)
        {
            Thread.Sleep(1);
            var line = Encoding.UTF8.GetString(payload).TrimEnd('\n');
            lock (_gate)
            {
                _events.Add(Message(line));
            }
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                _events.Add("flush");
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public IReadOnlyList<string> Events()
        {
            lock (_gate)
            {
                return [.. _events];
            }
        }
    }

    /// <summary>Holds the writer inside its first write until released.</summary>
    private sealed class GatedSink : ILogSink, IDisposable
    {
        private readonly ManualResetEventSlim _entered = new(false);
        private readonly ManualResetEventSlim _released = new(false);

        public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken)
        {
            _entered.Set();
            _released.Wait(TimeSpan.FromSeconds(10), cancellationToken);
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public bool WaitUntilEntered(TimeSpan timeout) => _entered.Wait(timeout);

        public void Release() => _released.Set();

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public void Dispose()
        {
            _released.Set();
            _entered.Dispose();
            _released.Dispose();
        }
    }
}
