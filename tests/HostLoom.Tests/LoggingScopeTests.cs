using System.Collections;
using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

// CA1873: the boxing standard ILogger path is what several of these tests exercise on purpose.
#pragma warning disable CA1873

namespace HostLoom.Tests;

public sealed class LoggingScopeTests
{
    [Fact]
    public async Task Structured_scopes_flatten_into_typed_fields()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Scoped");

        using (logger.BeginScope(new Dictionary<string, object?> { ["TenantId"] = 42 }))
        using (logger.BeginScope("operation {OperationId}", 7))
        {
            logger.LogFast(LogLevel.Information, $"inside both scopes");
        }

        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal(42, root.GetProperty("TenantId").GetInt32());
        Assert.Equal(7, root.GetProperty("OperationId").GetInt32());
        Assert.False(root.TryGetProperty("{OriginalFormat}", out _));
    }

    [Fact]
    public async Task Inner_scopes_override_outer_scopes_and_holes_override_both()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Ranked");
        var orderId = 7;

        using (logger.BeginScope(new Dictionary<string, object?> { ["TenantId"] = 1, ["orderId"] = "outer" }))
        using (logger.BeginScope(new Dictionary<string, object?> { ["TenantId"] = 2 }))
        {
            logger.LogFast(LogLevel.Information, $"order {orderId}");
        }

        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal(1, root.EnumerateObject().Count(p => p.NameEquals("TenantId")));
        Assert.Equal(2, root.GetProperty("TenantId").GetInt32());
        Assert.Equal(1, root.EnumerateObject().Count(p => p.NameEquals("orderId")));
        Assert.Equal(7, root.GetProperty("orderId").GetInt32());
    }

    [Fact]
    public async Task Scope_texts_are_preserved_outer_to_inner()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Texts");

        using (logger.BeginScope("outer-operation"))
        using (logger.BeginScope("step {Step}", 3))
        {
            logger.LogFast(LogLevel.Information, $"working");
        }

        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        var scope = root.GetProperty("Scope").EnumerateArray().Select(e => e.GetString()).ToArray();
        // Non-structured and templated scopes keep their rendered text, outermost first, while
        // the templated scope's hole still landed as the typed Step field.
        Assert.Equal(["outer-operation", "step 3"], scope);
        Assert.Equal(3, root.GetProperty("Step").GetInt32());
    }

    [Fact]
    public async Task A_throwing_scope_is_counted_and_costs_nothing_else()
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
                    if (tag.Key == "component" && (tag.Value as string) == "scope")
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
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Broken");

        using (logger.BeginScope(new ThrowingScope()))
        using (logger.BeginScope(new Dictionary<string, object?> { ["Survivor"] = "yes" }))
        {
            logger.LogFast(LogLevel.Information, $"still ships");
        }

        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal("still ships", root.GetProperty("message").GetString());
        Assert.Equal("yes", root.GetProperty("Survivor").GetString());
        lock (gate)
        {
            Assert.True(failures > 0, "expected the scope failure to be counted");
        }
    }

    [Fact]
    public async Task An_externally_supplied_scope_provider_is_used()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        // What a LoggerFactory does when the provider implements ISupportExternalScope: hand it
        // the factory-wide scope provider so scopes flow across providers.
        var external = new LoggerExternalScopeProvider();
        ((ISupportExternalScope)provider).SetScopeProvider(external);
        var logger = provider.CreateLogger("External");

        using (external.Push(new Dictionary<string, object?> { ["FromFactory"] = true }))
        {
            logger.LogFast(LogLevel.Information, $"through the external provider");
        }

        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal(JsonValueKind.True, root.GetProperty("FromFactory").ValueKind);
    }

    // CA2000: sink ownership transfers to the provider.
#pragma warning disable CA2000
    private static CollectingSink NewSink() => new();
#pragma warning restore CA2000

    /// <summary>A structured-looking scope whose enumeration explodes.</summary>
    private sealed class ThrowingScope : IEnumerable<KeyValuePair<string, object?>>
    {
        public IEnumerator<KeyValuePair<string, object?>> GetEnumerator() =>
            throw new InvalidOperationException("unreadable scope");

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
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
