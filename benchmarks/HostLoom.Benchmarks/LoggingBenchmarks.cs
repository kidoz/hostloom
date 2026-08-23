using BenchmarkDotNet.Attributes;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting.Json;

// CA1707: underscored benchmark names are how the results table stays readable.
// CA2000: the sink's lifetime belongs to the provider, disposed in GlobalCleanup.
#pragma warning disable CA1707, CA2000

namespace HostLoom.Benchmarks;

/// <summary>
/// Compares the cost a log call imposes on the calling thread. Both loggers render structured JSON
/// and discard the bytes, so the measurement is the pipeline, not the I/O.
/// </summary>
[MemoryDiagnoser]
public class LoggingBenchmarks
{
    private HostLoomLoggerProvider _provider = null!;
    private Microsoft.Extensions.Logging.ILogger _hostLoom = null!;
    private Microsoft.Extensions.Logging.ILogger _hostLoomViaInterface = null!;
    private Serilog.Core.Logger _serilog = null!;

    private readonly int _orderId = 42;
    private readonly string _customer = "ada";

    [GlobalSetup]
    public void Setup()
    {
        _provider = new HostLoomLoggerProvider(new JsonLogFormatter(), new NullSink(), new HostLoomLoggerOptions
        {
            QueueCapacity = 1 << 16
        });
        _hostLoom = _provider.CreateLogger("Bench");
        _hostLoomViaInterface = _hostLoom;
        _serilog = new LoggerConfiguration()
            .MinimumLevel.Information()
            .WriteTo.Sink(new NullSerilogSink())
            .CreateLogger();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _provider.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _serilog.Dispose();
    }

    /// <summary>The fast path: interpolated handler, rendered straight to UTF-8.</summary>
    [Benchmark(Baseline = true)]
    public void HostLoom_Interpolated() =>
        _hostLoom.LogFast(LogLevel.Information, $"order {_orderId} for {_customer}");

    /// <summary>What a third-party library costs you: boxed state through the ILogger interface.</summary>
    [Benchmark]
    public void HostLoom_ILoggerInterface() =>
#pragma warning disable CA1873
        _hostLoomViaInterface.LogInformation("order {OrderId} for {Customer}", _orderId, _customer);
#pragma warning restore CA1873

    [Benchmark]
    public void Serilog_MessageTemplate() =>
        _serilog.Information("order {OrderId} for {Customer}", _orderId, _customer);

    /// <summary>Disabled levels: the number that decides whether debug logging can stay in hot code.</summary>
    [Benchmark]
    public void HostLoom_Disabled() =>
        _hostLoom.LogFast(LogLevel.None, $"order {_orderId} for {_customer}");

    [Benchmark]
    public void Serilog_Disabled() =>
        _serilog.Debug("order {OrderId} for {Customer}", _orderId, _customer);

    private sealed class NullSink : ILogSink
    {
        public void Write(ReadOnlySpan<byte> payload)
        {
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Formats to JSON like the HostLoom path does, then discards, so the work is comparable.</summary>
    private sealed class NullSerilogSink : ILogEventSink
    {
        private readonly JsonFormatter _formatter = new();
        private readonly TextWriter _writer = TextWriter.Null;

        public void Emit(LogEvent logEvent) => _formatter.Format(logEvent, _writer);
    }
}
