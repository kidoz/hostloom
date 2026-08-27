using System.Buffers;
using BenchmarkDotNet.Attributes;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Context;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting.Compact;
using Serilog.Parsing;

// CA1707: underscored benchmark names are how the results table stays readable.
// CA2000: sink and provider lifetimes are owned by GlobalSetup/GlobalCleanup.
// CA1873: the boxing standard ILogger path is one of the shapes under measurement.
#pragma warning disable CA1707, CA2000, CA1873

namespace HostLoom.Benchmarks;

/// <summary>
/// Compares the cost a log call imposes on the calling thread against the deployed Serilog
/// shape (synchronous console sink with CompactJsonFormatter, here formatting to a discarded
/// writer), plus formatter-only throughput for the background half. HostLoom defers formatting
/// to its writer thread, so its producer-side numbers measure capture and enqueue; Serilog's
/// deployed pipeline formats on the calling thread, so its numbers include formatting — that
/// asymmetry is the deployment reality being compared, not an unfairness.
/// </summary>
[MemoryDiagnoser]
public class LoggingBenchmarks
{
    private static readonly TraceContext Ambient = new();

    private HostLoomLoggerProvider _provider = null!;
    private HostLoomLoggerProvider _enrichedProvider = null!;
    private Microsoft.Extensions.Logging.ILogger _hostLoom = null!;
    private Microsoft.Extensions.Logging.ILogger _hostLoomEnriched = null!;
    private Logger _serilog = null!;
    private Logger _serilogEnriched = null!;
    private Logger _serilogContext = null!;

    private readonly int _orderId = 42;
    private readonly string _customer = "ada";
    private readonly OrderContract _order = new();
    private readonly Dictionary<string, object?> _scope = new()
    {
        ["TenantId"] = 42,
        ["Region"] = "eu",
    };

    private LogEntry _entry = null!;
    private JsonLogFormatter _jsonFormatter = null!;
    private ClefLogFormatter _clefFormatter = null!;
    private ArrayBufferWriter<byte> _formatBuffer = null!;
    private CompactJsonFormatter _compactFormatter = null!;
    private LogEvent _logEvent = null!;
    private StringWriter _compactOutput = null!;

    [GlobalSetup]
    public void Setup()
    {
        _provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            new NullSink(),
            new HostLoomLoggerOptions { QueueCapacity = 1 << 16, AttachMachineName = false }
        );
        _hostLoom = _provider.CreateLogger("Bench");

        _enrichedProvider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            new NullSink(),
            new HostLoomLoggerOptions
            {
                QueueCapacity = 1 << 16,
                AttachMachineName = false,
                Enrichers = { new HostLoomTraceEnricher() },
            }
        );
        _hostLoomEnriched = _enrichedProvider.CreateLogger("Bench");

        _serilog = new LoggerConfiguration()
            .MinimumLevel.Information()
            .WriteTo.Sink(new NullCompactSink())
            .CreateLogger();
        _serilogEnriched = new LoggerConfiguration()
            .MinimumLevel.Information()
            .Enrich.With(new SerilogTraceEnricher())
            .WriteTo.Sink(new NullCompactSink())
            .CreateLogger();
        _serilogContext = new LoggerConfiguration()
            .MinimumLevel.Information()
            .Enrich.FromLogContext()
            .WriteTo.Sink(new NullCompactSink())
            .CreateLogger();

        _jsonFormatter = new JsonLogFormatter();
        _clefFormatter = new ClefLogFormatter();
        _formatBuffer = new ArrayBufferWriter<byte>(4 * 1024);
        _entry = LogEntryPool.Rent();
        _entry.Level = Microsoft.Extensions.Logging.LogLevel.Information;
        _entry.Category = "Bench";
        _entry.Timestamp = DateTimeOffset.UtcNow;
        _entry.ThreadId = 7;
        _entry.AppendLiteral("order ");
        _entry.AppendFormattable(_orderId, null, "OrderId", LogFieldKind.Number);
        _entry.AppendLiteral(" for ");
        _entry.AppendText(_customer, "Customer");
        _entry.NormalizeFields(128, 64, _jsonFormatter, null);

        _compactFormatter = new CompactJsonFormatter();
        _compactOutput = new StringWriter();
        var template = new MessageTemplateParser().Parse("order {OrderId} for {Customer}");
        _logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Information,
            null,
            template,
            [
                new LogEventProperty("OrderId", new ScalarValue(_orderId)),
                new LogEventProperty("Customer", new ScalarValue(_customer)),
                new LogEventProperty("SourceContext", new ScalarValue("Bench")),
                new LogEventProperty("ThreadId", new ScalarValue(7)),
            ]
        );
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        LogEntryPool.Return(_entry);
        _compactOutput.Dispose();
        _provider.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _enrichedProvider.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _serilog.Dispose();
        _serilogEnriched.Dispose();
        _serilogContext.Dispose();
    }

    // -- Template plus two scalar holes -------------------------------------------------------

    /// <summary>The no-box fast path: interpolated handler, rendered straight to UTF-8.</summary>
    [Benchmark(Baseline = true)]
    public void HostLoom_FastPath_TwoScalars() =>
        _hostLoom.LogFast(
            Microsoft.Extensions.Logging.LogLevel.Information,
            $"order {_orderId} for {_customer}"
        );

    /// <summary>What every third-party library pays: boxed state through the interface.</summary>
    [Benchmark]
    public void HostLoom_Interface_TwoScalars() =>
        _hostLoom.LogInformation("order {OrderId} for {Customer}", _orderId, _customer);

    [Benchmark]
    public void Serilog_TwoScalars() =>
        _serilog.Information("order {OrderId} for {Customer}", _orderId, _customer);

    // -- Destructured mid-size contract -------------------------------------------------------

    [Benchmark]
    public void HostLoom_DestructuredContract() =>
        _hostLoom.LogInformation("processing {@Order}", _order);

    [Benchmark]
    public void Serilog_DestructuredContract() =>
        _serilog.Information("processing {@Order}", _order);

    // -- Enriched event with all 11 trace properties ------------------------------------------

    [Benchmark]
    public void HostLoom_Enriched11() =>
        _hostLoomEnriched.LogInformation("order {OrderId} for {Customer}", _orderId, _customer);

    [Benchmark]
    public void Serilog_Enriched11() =>
        _serilogEnriched.Information("order {OrderId} for {Customer}", _orderId, _customer);

    // -- Scoped event -------------------------------------------------------------------------

    [Benchmark]
    public void HostLoom_Scoped()
    {
        using (_hostLoom.BeginScope(_scope))
        {
            _hostLoom.LogInformation("order {OrderId}", _orderId);
        }
    }

    [Benchmark]
    public void Serilog_LogContextScoped()
    {
        using (LogContext.PushProperty("TenantId", 42))
        using (LogContext.PushProperty("Region", "eu"))
        {
            _serilogContext.Information("order {OrderId}", _orderId);
        }
    }

    // -- Disabled level -----------------------------------------------------------------------

    /// <summary>The number that decides whether debug logging can stay in hot code.</summary>
    [Benchmark]
    public void HostLoom_Disabled() =>
        _hostLoom.LogFast(
            Microsoft.Extensions.Logging.LogLevel.None,
            $"order {_orderId} for {_customer}"
        );

    [Benchmark]
    public void Serilog_Disabled() =>
        _serilog.Debug("order {OrderId} for {Customer}", _orderId, _customer);

    // -- Formatter-only: the background half of the pipeline ----------------------------------

    [Benchmark]
    public void HostLoomJson_FormatOnly()
    {
        _formatBuffer.ResetWrittenCount();
        _jsonFormatter.Format(new LogRecord(_entry), _formatBuffer);
    }

    [Benchmark]
    public void HostLoomClef_FormatOnly()
    {
        _formatBuffer.ResetWrittenCount();
        _clefFormatter.Format(new LogRecord(_entry), _formatBuffer);
    }

    [Benchmark]
    public void SerilogCompact_FormatOnly()
    {
        _compactOutput.GetStringBuilder().Clear();
        _compactFormatter.Format(_logEvent, _compactOutput);
    }

    // -- Shared shapes ------------------------------------------------------------------------

    /// <summary>A mid-size Kafka-style contract: eight members including a small collection.</summary>
    private sealed class OrderContract
    {
        public Guid Id { get; set; } = Guid.Parse("a2b8f9e0-1234-4cde-9f00-56789abcdef0");

        public string Customer { get; set; } = "ada-lovelace";

        public string Currency { get; set; } = "EUR";

        public decimal Total { get; set; } = 1234.56m;

        public int Items { get; set; } = 3;

        public bool Express { get; set; } = true;

        public DateTimeOffset PlacedAt { get; set; } = new(2026, 8, 26, 10, 0, 0, TimeSpan.Zero);

        public IReadOnlyList<string> Tags { get; } = ["priority", "gift", "eu"];
    }

    /// <summary>The ambient state both enrichers read, mirroring the platform's TraceContext.</summary>
    private sealed class TraceContext
    {
        public string CorrelationId { get; } = "corr-0001";

        public string RequestId { get; } = "req-0002";

        public string CausationId { get; } = "cause-0003";

        public string TraceId { get; } = "trace-0004";

        public string SpanId { get; } = "span-0005";

        public string SourceType { get; } = "kafka";

        public string SourceService { get; } = "checkout";

        public string SourceOperation { get; } = "order-created";

        public string OperatorId { get; } = "op-0006";

        public string TriggeringEntityType { get; } = "order";

        public string TriggeringEntityId { get; } = "ord-0007";
    }

    private sealed class HostLoomTraceEnricher : ILogEnricher
    {
        public void Enrich(ref LogEntryWriter writer)
        {
            writer.Add("CorrelationId", Ambient.CorrelationId);
            writer.Add("RequestId", Ambient.RequestId);
            writer.Add("CausationId", Ambient.CausationId);
            writer.Add("TraceId", Ambient.TraceId);
            writer.Add("SpanId", Ambient.SpanId);
            writer.Add("SourceType", Ambient.SourceType);
            writer.Add("SourceService", Ambient.SourceService);
            writer.Add("SourceOperation", Ambient.SourceOperation);
            writer.Add("OperatorId", Ambient.OperatorId);
            writer.Add("TriggeringEntityType", Ambient.TriggeringEntityType);
            writer.Add("TriggeringEntityId", Ambient.TriggeringEntityId);
        }
    }

    private sealed class SerilogTraceEnricher : ILogEventEnricher
    {
        public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
        {
            logEvent.AddPropertyIfAbsent(
                propertyFactory.CreateProperty("CorrelationId", Ambient.CorrelationId)
            );
            logEvent.AddPropertyIfAbsent(
                propertyFactory.CreateProperty("RequestId", Ambient.RequestId)
            );
            logEvent.AddPropertyIfAbsent(
                propertyFactory.CreateProperty("CausationId", Ambient.CausationId)
            );
            logEvent.AddPropertyIfAbsent(
                propertyFactory.CreateProperty("TraceId", Ambient.TraceId)
            );
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("SpanId", Ambient.SpanId));
            logEvent.AddPropertyIfAbsent(
                propertyFactory.CreateProperty("SourceType", Ambient.SourceType)
            );
            logEvent.AddPropertyIfAbsent(
                propertyFactory.CreateProperty("SourceService", Ambient.SourceService)
            );
            logEvent.AddPropertyIfAbsent(
                propertyFactory.CreateProperty("SourceOperation", Ambient.SourceOperation)
            );
            logEvent.AddPropertyIfAbsent(
                propertyFactory.CreateProperty("OperatorId", Ambient.OperatorId)
            );
            logEvent.AddPropertyIfAbsent(
                propertyFactory.CreateProperty("TriggeringEntityType", Ambient.TriggeringEntityType)
            );
            logEvent.AddPropertyIfAbsent(
                propertyFactory.CreateProperty("TriggeringEntityId", Ambient.TriggeringEntityId)
            );
        }
    }

    private sealed class NullSink : ILogSink
    {
        public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken) { }

        public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Formats with the deployed CompactJsonFormatter, then discards — the calling
    /// thread pays for formatting exactly as it does on the platform's synchronous console sink.</summary>
    private sealed class NullCompactSink : ILogEventSink
    {
        private readonly CompactJsonFormatter _formatter = new();
        private readonly TextWriter _writer = TextWriter.Null;

        public void Emit(LogEvent logEvent) => _formatter.Format(logEvent, _writer);
    }
}
