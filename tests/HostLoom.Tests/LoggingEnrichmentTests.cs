using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

public sealed class LoggingEnrichmentTests
{
    private static readonly AsyncLocal<string?> AmbientCorrelation = new();

    [Fact]
    public async Task Enrichers_capture_ambient_state_in_order_with_later_wins()
    {
        AmbientCorrelation.Value = "corr-42";
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions
            {
                Enrichers =
                {
                    new DelegateEnricher(
                        (ref LogEntryWriter writer) =>
                        {
                            writer.Add("CorrelationId", AmbientCorrelation.Value);
                            writer.Add("Stage", 1);
                            writer.Add("Sampled", true);
                        }
                    ),
                    new DelegateEnricher(
                        (ref LogEntryWriter writer) => writer.Add("Stage", 2)
                    ),
                },
            }
        );
        var logger = provider.CreateLogger("Enriched");

        logger.LogFast(LogLevel.Information, $"an enriched event");
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        // The AsyncLocal value is visible because enrichment ran on the calling thread; the
        // writer thread would have seen nothing.
        Assert.Equal("corr-42", root.GetProperty("CorrelationId").GetString());
        Assert.Equal(JsonValueKind.True, root.GetProperty("Sampled").ValueKind);
        // Registration order, later wins, one key.
        Assert.Equal(1, root.EnumerateObject().Count(p => p.NameEquals("Stage")));
        Assert.Equal(2, root.GetProperty("Stage").GetInt32());
    }

    [Fact]
    public async Task A_throwing_enricher_is_isolated_and_counted()
    {
        long failures = 0;
        var gate = new Lock();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (
                instrument.Meter.Name == "HostLoom.Logging"
                && instrument.Name == "hostloom.logging.failures"
            )
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>(
            (instrument, measurement, tags, state) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == "component" && (tag.Value as string) == "enricher")
                    {
                        lock (gate)
                        {
                            failures += measurement;
                        }
                    }
                }
            }
        );
        listener.Start();

        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions
            {
                Enrichers =
                {
                    new DelegateEnricher(
                        (ref LogEntryWriter _) =>
                            throw new InvalidOperationException("broken enricher")
                    ),
                    new DelegateEnricher(
                        (ref LogEntryWriter writer) => writer.Add("Survivor", "yes")
                    ),
                },
            }
        );
        var logger = provider.CreateLogger("Faulty");

        logger.LogFast(LogLevel.Information, $"still ships");
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal("still ships", root.GetProperty("message").GetString());
        Assert.Equal("yes", root.GetProperty("Survivor").GetString());
        lock (gate)
        {
            Assert.True(failures > 0, "expected the enricher failure to be counted");
        }
    }

    [Fact]
    public async Task Holes_outrank_enrichers_and_enrichers_outrank_statics()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions
            {
                ServiceName = "checkout",
                Enrichers =
                {
                    new DelegateEnricher(
                        (ref LogEntryWriter writer) =>
                        {
                            writer.Add("orderId", "from-enricher");
                            writer.Add("ServiceName", "from-enricher");
                        }
                    ),
                },
            }
        );
        var logger = provider.CreateLogger("Ranked");
        var orderId = 7;

        logger.LogFast(LogLevel.Information, $"order {orderId}");
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        // The event hole beats the enricher; the enricher beats the static field.
        Assert.Equal(1, root.EnumerateObject().Count(p => p.NameEquals("orderId")));
        Assert.Equal(7, root.GetProperty("orderId").GetInt32());
        Assert.Equal(1, root.EnumerateObject().Count(p => p.NameEquals("ServiceName")));
        Assert.Equal("from-enricher", root.GetProperty("ServiceName").GetString());
    }

    [Fact]
    public async Task Machine_and_service_names_attach_as_static_fields()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions { ServiceName = "checkout" }
        );
        var logger = provider.CreateLogger("Static");

        logger.LogFast(LogLevel.Information, $"who am i");
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal(Environment.MachineName, root.GetProperty("MachineName").GetString());
        Assert.Equal("checkout", root.GetProperty("ServiceName").GetString());
    }

    [Fact]
    public async Task Machine_name_can_be_turned_off()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions { AttachMachineName = false }
        );
        var logger = provider.CreateLogger("Bare");

        logger.LogFast(LogLevel.Information, $"anonymous");
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.False(root.TryGetProperty("MachineName", out _));
    }

    // CA2000: sink ownership transfers to the provider.
#pragma warning disable CA2000
    private static CollectingSink NewSink() => new();
#pragma warning restore CA2000

    private delegate void Enrich(ref LogEntryWriter writer);

    private sealed class DelegateEnricher(Enrich enrich) : ILogEnricher
    {
        public void Enrich(ref LogEntryWriter writer) => enrich(ref writer);
    }

    private sealed class CollectingSink : ILogSink
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

        public ValueTask FlushAsync(CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

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
}
