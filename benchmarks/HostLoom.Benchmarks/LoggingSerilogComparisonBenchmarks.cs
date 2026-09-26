using BenchmarkDotNet.Attributes;
using Destructurama;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Core;
using Serilog.Events;
using Serilog.Extensions.Logging;
using Serilog.Formatting.Compact;
using MelLogLevel = Microsoft.Extensions.Logging.LogLevel;

// CA1707: underscored benchmark names are how the results table stays readable.
// CA2000: logger factories, providers, and sinks are owned by GlobalSetup/GlobalCleanup.
// CA1848/CA1873: the standard boxing ILogger calls are exactly what applications write.
#pragma warning disable CA1707, CA2000, CA1848, CA1873

namespace HostLoom.Benchmarks;

/// <summary>
/// Both libraries behind <see cref="Microsoft.Extensions.Logging.ILogger"/>, the way an
/// application built on <c>ILogger&lt;T&gt;</c> reaches them, each configured like a
/// JSON-to-stdout deployment:
/// <list type="bullet">
/// <item>HostLoom: <see cref="ClefLogFormatter"/>, <c>$type</c> tags, properties only, the
/// machine name attached, and <see cref="QueueFullPolicy.Block"/>, so every measured call is a
/// record the writer thread actually formatted and handed to the sink, never a dropped one.</item>
/// <item>Serilog: Serilog.Extensions.Logging, <see cref="CompactJsonFormatter"/>, and
/// Destructurama attribute masking, writing through a sink that does per event what
/// Serilog.Sinks.Console does with a formatter configured: format into a fresh 256-character
/// <see cref="StringWriter"/>, then write the string under a lock.</item>
/// <item>Serilog async: the same pipeline behind Serilog.Sinks.Async, blocking when full.</item>
/// </list>
/// Both sinks discard their output, so stdout I/O is excluded on both sides. Serilog's
/// synchronous pipeline formats on the calling thread; HostLoom and the async variant format on
/// a background thread, whose time is not in these per-call numbers but whose allocations are:
/// the memory diagnoser counts every thread.
/// </summary>
[MemoryDiagnoser]
public class LoggingSerilogComparisonBenchmarks
{
    private static readonly Exception Failure = Capture();

    private readonly OrderPlaced _order = new();
    private readonly LoginRequest _login = new();
    private readonly List<long> _ids = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
    private readonly Dictionary<string, object?> _scope = new()
    {
        ["TenantId"] = 42,
        ["Region"] = "eu",
    };

    private Pipelines _plain = null!;
    private Pipelines _enriched = null!;

    [GlobalSetup]
    public void Setup()
    {
        _plain = new Pipelines(enrich: false);
        _enriched = new Pipelines(enrich: true);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _plain.Dispose();
        _enriched.Dispose();
    }

    // -- Template with two scalar holes: the most common call --------------------------------

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("TwoScalars")]
    public void Serilog_TwoScalars() => TwoScalars(_plain.Serilog);

    [Benchmark]
    [BenchmarkCategory("TwoScalars")]
    public void SerilogAsync_TwoScalars() => TwoScalars(_plain.SerilogAsync);

    [Benchmark]
    [BenchmarkCategory("TwoScalars")]
    public void HostLoom_TwoScalars() => TwoScalars(_plain.HostLoom);

    // -- A destructured Kafka-style contract --------------------------------------------------

    [Benchmark]
    [BenchmarkCategory("Contract")]
    public void Serilog_DestructuredContract() => Contract(_plain.Serilog);

    [Benchmark]
    [BenchmarkCategory("Contract")]
    public void SerilogAsync_DestructuredContract() => Contract(_plain.SerilogAsync);

    [Benchmark]
    [BenchmarkCategory("Contract")]
    public void HostLoom_DestructuredContract() => Contract(_plain.HostLoom);

    // -- A destructured DTO with masked members -----------------------------------------------

    [Benchmark]
    [BenchmarkCategory("Masked")]
    public void Serilog_MaskedDto() => Masked(_plain.Serilog);

    [Benchmark]
    [BenchmarkCategory("Masked")]
    public void SerilogAsync_MaskedDto() => Masked(_plain.SerilogAsync);

    [Benchmark]
    [BenchmarkCategory("Masked")]
    public void HostLoom_MaskedDto() => Masked(_plain.HostLoom);

    // -- Eleven ambient trace properties added by an enricher ---------------------------------

    [Benchmark]
    [BenchmarkCategory("Enriched")]
    public void Serilog_Enriched11() => TwoScalars(_enriched.Serilog);

    [Benchmark]
    [BenchmarkCategory("Enriched")]
    public void SerilogAsync_Enriched11() => TwoScalars(_enriched.SerilogAsync);

    [Benchmark]
    [BenchmarkCategory("Enriched")]
    public void HostLoom_Enriched11() => TwoScalars(_enriched.HostLoom);

    // -- A structured scope -------------------------------------------------------------------

    [Benchmark]
    [BenchmarkCategory("Scoped")]
    public void Serilog_Scoped() => Scoped(_plain.Serilog);

    [Benchmark]
    [BenchmarkCategory("Scoped")]
    public void SerilogAsync_Scoped() => Scoped(_plain.SerilogAsync);

    [Benchmark]
    [BenchmarkCategory("Scoped")]
    public void HostLoom_Scoped() => Scoped(_plain.HostLoom);

    // -- An exception with its stack trace ----------------------------------------------------

    [Benchmark]
    [BenchmarkCategory("Exception")]
    public void Serilog_Exception() => Error(_plain.Serilog);

    [Benchmark]
    [BenchmarkCategory("Exception")]
    public void SerilogAsync_Exception() => Error(_plain.SerilogAsync);

    [Benchmark]
    [BenchmarkCategory("Exception")]
    public void HostLoom_Exception() => Error(_plain.HostLoom);

    // -- A collection in a plain hole ---------------------------------------------------------

    [Benchmark]
    [BenchmarkCategory("Collection")]
    public void Serilog_Collection() => Collection(_plain.Serilog);

    [Benchmark]
    [BenchmarkCategory("Collection")]
    public void SerilogAsync_Collection() => Collection(_plain.SerilogAsync);

    [Benchmark]
    [BenchmarkCategory("Collection")]
    public void HostLoom_Collection() => Collection(_plain.HostLoom);

    // -- A disabled level: the cost of leaving Debug calls in hot code ------------------------

    [Benchmark]
    [BenchmarkCategory("Disabled")]
    public void Serilog_DisabledDebug() => Disabled(_plain.Serilog);

    [Benchmark]
    [BenchmarkCategory("Disabled")]
    public void SerilogAsync_DisabledDebug() => Disabled(_plain.SerilogAsync);

    [Benchmark]
    [BenchmarkCategory("Disabled")]
    public void HostLoom_DisabledDebug() => Disabled(_plain.HostLoom);

    private static void TwoScalars(Microsoft.Extensions.Logging.ILogger logger) =>
        logger.LogInformation("Order {OrderId} placed by {Customer}", 42, "ada");

    private void Contract(Microsoft.Extensions.Logging.ILogger logger) =>
        logger.LogInformation("Processing message: {@Message}", _order);

    private void Masked(Microsoft.Extensions.Logging.ILogger logger) =>
        logger.LogInformation("Process request: {@Request}", _login);

    private void Scoped(Microsoft.Extensions.Logging.ILogger logger)
    {
        using (logger.BeginScope(_scope))
        {
            logger.LogInformation("Order {OrderId}", 42);
        }
    }

    private static void Error(Microsoft.Extensions.Logging.ILogger logger) =>
        logger.LogError(Failure, "Handler {Handler} failed", "Fulfillment");

    private void Collection(Microsoft.Extensions.Logging.ILogger logger) =>
        logger.LogInformation("Ids {Ids}", _ids);

    private static void Disabled(Microsoft.Extensions.Logging.ILogger logger) =>
        logger.LogDebug("Order {OrderId} placed by {Customer}", 42, "ada");

    private static InvalidOperationException Capture()
    {
        try
        {
            throw new InvalidOperationException(
                "Order state is invalid",
                new ArgumentException("Quantity below minimum", "quantity")
            );
        }
        catch (InvalidOperationException exception)
        {
            return exception;
        }
    }

    /// <summary>One logger per library, built the way an application's container builds them.</summary>
    internal sealed class Pipelines : IDisposable
    {
        private readonly ILoggerFactory _serilogFactory;
        private readonly ILoggerFactory _serilogAsyncFactory;
        private readonly ILoggerFactory _hostLoomFactory;
        private readonly HostLoomLoggerProvider _hostLoomProvider;

        public Pipelines(bool enrich)
        {
            var serilog = SerilogConfiguration(enrich)
                .WriteTo.Sink(new ConsoleShapedSink())
                .CreateLogger();
            _serilogFactory = LoggerFactory.Create(builder =>
                builder.AddSerilog(serilog, dispose: true)
            );
            Serilog = _serilogFactory.CreateLogger("Bench.Serilog");

            var serilogAsync = SerilogConfiguration(enrich)
                .WriteTo.Async(sink => sink.Sink(new ConsoleShapedSink()), blockWhenFull: true)
                .CreateLogger();
            _serilogAsyncFactory = LoggerFactory.Create(builder =>
                builder.AddSerilog(serilogAsync, dispose: true)
            );
            SerilogAsync = _serilogAsyncFactory.CreateLogger("Bench.SerilogAsync");

            var options = new HostLoomLoggerOptions { QueueFullPolicy = QueueFullPolicy.Block };
            options.Destructuring.TypeTags = true;
            options.Destructuring.IncludeFields = false;
            if (enrich)
            {
                options.Enrichers.Add(new HostLoomTraceEnricher());
            }

            _hostLoomProvider = new HostLoomLoggerProvider(
                new ClefLogFormatter(),
                new DiscardingSink(),
                options
            );
            _hostLoomFactory = LoggerFactory.Create(builder =>
                builder.SetMinimumLevel(MelLogLevel.Information).AddProvider(_hostLoomProvider)
            );
            HostLoom = _hostLoomFactory.CreateLogger("Bench.HostLoom");
        }

        public Microsoft.Extensions.Logging.ILogger Serilog { get; }

        public Microsoft.Extensions.Logging.ILogger SerilogAsync { get; }

        public Microsoft.Extensions.Logging.ILogger HostLoom { get; }

        public void Dispose()
        {
            _serilogFactory.Dispose();
            _serilogAsyncFactory.Dispose();
            _hostLoomFactory.Dispose();
            _hostLoomProvider.Dispose();
            // Block never drops; a drop here would mean the numbers timed discarded records.
            if (_hostLoomProvider.Dropped != 0)
            {
                throw new InvalidOperationException(
                    $"HostLoom dropped {_hostLoomProvider.Dropped} records."
                );
            }
        }

        private static LoggerConfiguration SerilogConfiguration(bool enrich)
        {
            var configuration = new LoggerConfiguration()
                .MinimumLevel.Information()
                .Destructure.UsingAttributes();
            return enrich ? configuration.Enrich.With(new SerilogTraceEnricher()) : configuration;
        }
    }

    /// <summary>A mid-size message contract: nested object, collection, dictionary, enum, dates.</summary>
    internal sealed class OrderPlaced
    {
        public long OrderId { get; init; } = 9_000_000_001;

        public Guid ClientId { get; init; } = Guid.Parse("5f1d7c2e-0c43-4a53-9d1e-2b8f6f0a9e11");

        public decimal Amount { get; init; } = 125.50m;

        public string Currency { get; init; } = "EUR";

        public DayOfWeek Day { get; init; } = DayOfWeek.Friday;

        public DateTime CreatedAt { get; init; } = new(2026, 9, 26, 13, 45, 0, DateTimeKind.Utc);

        public DateTimeOffset? ConfirmedAt { get; init; }

        public string? Comment { get; init; }

        public bool IsLive { get; init; } = true;

        public double Rate { get; init; } = 1.85;

        public Address Address { get; init; } = new();

        public IReadOnlyList<OrderLine> Lines { get; init; } =
        [new OrderLine { LineId = 1, Price = 10m }, new OrderLine { LineId = 2, Price = 20.25m }];

        public IReadOnlyDictionary<string, int> Limits { get; init; } =
            new Dictionary<string, int> { ["daily"] = 500, ["weekly"] = 2000 };
    }

    internal sealed class Address
    {
        public string Country { get; init; } = "DE";

        public string City { get; init; } = "Berlin";
    }

    internal sealed class OrderLine
    {
        public int LineId { get; init; }

        public decimal Price { get; init; }
    }

    /// <summary>Masked with Destructurama's attributes, which HostLoom honors by name.</summary>
    internal sealed class LoginRequest
    {
        public string Login { get; init; } = "alice";

        [Destructurama.Attributed.NotLogged]
        public string Password { get; init; } = "p@ssw0rd";

        [Destructurama.Attributed.LogMasked(Text = "***")]
        public string Token { get; init; } = "eyJhbGciOiJIUzI1NiJ9.secret";

        public IReadOnlyList<Credential> Credentials { get; init; } =
        [
            new Credential { Kind = "otp", Secret = "123456" },
            new Credential { Kind = "pin", Secret = "0000" },
        ];
    }

    internal sealed class Credential
    {
        public string Kind { get; init; } = "";

        [Destructurama.Attributed.NotLogged]
        public string Secret { get; init; } = "";
    }

    /// <summary>The ambient values both enrichers read, standing in for an AsyncLocal context.</summary>
    private static class Ambient
    {
        public const string CorrelationId = "corr-0001";
        public const string RequestId = "req-0002";
        public const string CausationId = "cause-0003";
        public const string TraceId = "0af7651916cd43dd8448eb211c80319c";
        public const string SpanId = "b7ad6b7169203331";
        public const string SourceType = "Manual";
        public const string SourceService = "billing";
        public const string SourceOperation = "approve-invoice";
        public const string OperatorId = "operator-9";
        public const string TriggeringEntityType = "invoice";
        public const string TriggeringEntityId = "inv-100";
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
            Add(logEvent, propertyFactory, "CorrelationId", Ambient.CorrelationId);
            Add(logEvent, propertyFactory, "RequestId", Ambient.RequestId);
            Add(logEvent, propertyFactory, "CausationId", Ambient.CausationId);
            Add(logEvent, propertyFactory, "TraceId", Ambient.TraceId);
            Add(logEvent, propertyFactory, "SpanId", Ambient.SpanId);
            Add(logEvent, propertyFactory, "SourceType", Ambient.SourceType);
            Add(logEvent, propertyFactory, "SourceService", Ambient.SourceService);
            Add(logEvent, propertyFactory, "SourceOperation", Ambient.SourceOperation);
            Add(logEvent, propertyFactory, "OperatorId", Ambient.OperatorId);
            Add(logEvent, propertyFactory, "TriggeringEntityType", Ambient.TriggeringEntityType);
            Add(logEvent, propertyFactory, "TriggeringEntityId", Ambient.TriggeringEntityId);
        }

        private static void Add(
            LogEvent logEvent,
            ILogEventPropertyFactory propertyFactory,
            string name,
            string value
        ) => logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty(name, value));
    }

    /// <summary>What Serilog.Sinks.Console 6.1.1 does per event when a formatter is configured,
    /// with the console replaced by a discarding writer.</summary>
    internal sealed class ConsoleShapedSink(Completion? completion = null) : ILogEventSink
    {
        private readonly CompactJsonFormatter _formatter = new();
        private readonly TextWriter _output = TextWriter.Null;
        private readonly Lock _syncRoot = new();

        public long Written { get; private set; }

        public void Emit(LogEvent logEvent)
        {
            var buffer = new StringWriter(new System.Text.StringBuilder(256));
            _formatter.Format(logEvent, buffer);
            var text = buffer.ToString();
            lock (_syncRoot)
            {
                _output.Write(text);
                _output.Flush();
                Written++;
                completion?.Observe(Written);
            }
        }
    }

    internal sealed class DiscardingSink(Completion? completion = null) : ILogSink
    {
        public long Written { get; private set; }

        public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken)
        {
            Written += payload.Count((byte)'\n');
            completion?.Observe(Written);
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Signals when a sink has written a target number of records.</summary>
    internal sealed class Completion : IDisposable
    {
        private readonly ManualResetEventSlim _reached = new();
        private long _target = long.MaxValue;

        public void Expect(long target)
        {
            _reached.Reset();
            Volatile.Write(ref _target, target);
        }

        public void Observe(long written)
        {
            if (written >= Volatile.Read(ref _target))
            {
                _reached.Set();
            }
        }

        public void Wait() => _reached.Wait();

        public void Dispose() => _reached.Dispose();
    }
}

/// <summary>
/// End-to-end throughput: <see cref="Threads"/> callers log <see cref="PerThread"/> records each,
/// and an operation ends only when the sink has received every one of them, so background
/// formatting, queue hand-off, and lock contention between callers are all inside the timing.
/// Results are per record.
/// </summary>
[MemoryDiagnoser]
public class LoggingSerilogThroughputBenchmarks
{
    private const int PerThread = 2_000;

    private readonly LoggingSerilogComparisonBenchmarks.Completion _hostLoomDone = new();
    private readonly LoggingSerilogComparisonBenchmarks.Completion _serilogDone = new();
    private readonly LoggingSerilogComparisonBenchmarks.Completion _serilogAsyncDone = new();
    private LoggingSerilogComparisonBenchmarks.DiscardingSink _hostLoomSink = null!;
    private LoggingSerilogComparisonBenchmarks.ConsoleShapedSink _serilogSink = null!;
    private LoggingSerilogComparisonBenchmarks.ConsoleShapedSink _serilogAsyncSink = null!;
    private HostLoomLoggerProvider _hostLoomProvider = null!;
    private ILoggerFactory _hostLoomFactory = null!;
    private ILoggerFactory _serilogFactory = null!;
    private ILoggerFactory _serilogAsyncFactory = null!;
    private Microsoft.Extensions.Logging.ILogger _hostLoom = null!;
    private Microsoft.Extensions.Logging.ILogger _serilog = null!;
    private Microsoft.Extensions.Logging.ILogger _serilogAsync = null!;

    [Params(1, 8)]
    public int Threads { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var options = new HostLoomLoggerOptions { QueueFullPolicy = QueueFullPolicy.Block };
        options.Destructuring.TypeTags = true;
        options.Destructuring.IncludeFields = false;
        _hostLoomSink = new LoggingSerilogComparisonBenchmarks.DiscardingSink(_hostLoomDone);
        _hostLoomProvider = new HostLoomLoggerProvider(
            new ClefLogFormatter(),
            _hostLoomSink,
            options
        );
        _hostLoomFactory = LoggerFactory.Create(builder =>
            builder.SetMinimumLevel(MelLogLevel.Information).AddProvider(_hostLoomProvider)
        );
        _hostLoom = _hostLoomFactory.CreateLogger("Bench.HostLoom");

        _serilogSink = new LoggingSerilogComparisonBenchmarks.ConsoleShapedSink(_serilogDone);
        var serilog = new LoggerConfiguration()
            .MinimumLevel.Information()
            .Destructure.UsingAttributes()
            .WriteTo.Sink(_serilogSink)
            .CreateLogger();
        _serilogFactory = LoggerFactory.Create(builder =>
            builder.AddSerilog(serilog, dispose: true)
        );
        _serilog = _serilogFactory.CreateLogger("Bench.Serilog");

        _serilogAsyncSink = new LoggingSerilogComparisonBenchmarks.ConsoleShapedSink(
            _serilogAsyncDone
        );
        var serilogAsync = new LoggerConfiguration()
            .MinimumLevel.Information()
            .Destructure.UsingAttributes()
            .WriteTo.Async(sink => sink.Sink(_serilogAsyncSink), blockWhenFull: true)
            .CreateLogger();
        _serilogAsyncFactory = LoggerFactory.Create(builder =>
            builder.AddSerilog(serilogAsync, dispose: true)
        );
        _serilogAsync = _serilogAsyncFactory.CreateLogger("Bench.SerilogAsync");
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _serilogFactory.Dispose();
        _serilogAsyncFactory.Dispose();
        _hostLoomFactory.Dispose();
        _hostLoomProvider.Dispose();
        if (_hostLoomProvider.Dropped != 0)
        {
            throw new InvalidOperationException(
                $"HostLoom dropped {_hostLoomProvider.Dropped} records."
            );
        }

        _hostLoomDone.Dispose();
        _serilogDone.Dispose();
        _serilogAsyncDone.Dispose();
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = PerThread)]
    public void Serilog_Throughput() => Run(_serilog, _serilogSink.Written, _serilogDone);

    [Benchmark(OperationsPerInvoke = PerThread)]
    public void SerilogAsync_Throughput() =>
        Run(_serilogAsync, _serilogAsyncSink.Written, _serilogAsyncDone);

    [Benchmark(OperationsPerInvoke = PerThread)]
    public void HostLoom_Throughput() => Run(_hostLoom, _hostLoomSink.Written, _hostLoomDone);

    private void Run(
        Microsoft.Extensions.Logging.ILogger logger,
        long written,
        LoggingSerilogComparisonBenchmarks.Completion done
    )
    {
        done.Expect(written + (long)Threads * PerThread);
        Parallel.For(
            0,
            Threads,
            new ParallelOptions { MaxDegreeOfParallelism = Threads },
            _ =>
            {
                for (var i = 0; i < PerThread; i++)
                {
                    logger.LogInformation("Order {OrderId} placed by {Customer}", i, "ada");
                }
            }
        );
        done.Wait();
    }
}
