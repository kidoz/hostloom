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

        using (
            logger.BeginScope(
                new Dictionary<string, object?> { ["TenantId"] = 1, ["orderId"] = "outer" }
            )
        )
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
    public async Task A_destructured_scope_value_is_masked_in_its_field_and_its_text()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Protected");
        var user = new ScopedUser("ada", "hunter2");

        using (logger.BeginScope("user {@User}", user))
        {
            logger.LogFast(LogLevel.Information, $"inside");
        }

        await provider.DisposeAsync();

        var line = Assert.Single(sink.Lines());
        var root = JsonDocument.Parse(line).RootElement;
        var logged = root.GetProperty("User");
        Assert.Equal(JsonValueKind.Object, logged.ValueKind);
        Assert.Equal("ada", logged.GetProperty("Name").GetString());
        Assert.Equal("***", logged.GetProperty("Token").GetString());
        // The scope text is rendered from the captured representation, not from the record's
        // generated ToString(), which would print the masked member.
        var scope = root.GetProperty("Scope").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.Equal(["user {\"Name\":\"ada\",\"Token\":\"***\"}"], scope);
        Assert.DoesNotContain("hunter2", line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_plain_scope_hole_never_stringifies_a_protected_object()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Protected");
        var user = new ScopedUser("ada", "hunter2");

        using (logger.BeginScope("user {User}", user))
        {
            logger.LogInformation("inside");
        }

        await provider.DisposeAsync();

        var line = Assert.Single(sink.Lines());
        var root = JsonDocument.Parse(line).RootElement;
        // No caller formatter renders a scope, so a non-scalar scope value is destructured under
        // the protection policy even without '@'; ToString() would leak the masked member.
        var logged = root.GetProperty("User");
        Assert.Equal(JsonValueKind.Object, logged.ValueKind);
        Assert.Equal("***", logged.GetProperty("Token").GetString());
        var scope = root.GetProperty("Scope").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.Equal(["user {\"Name\":\"ada\",\"Token\":\"***\"}"], scope);
        Assert.DoesNotContain("hunter2", line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Scope_values_share_the_record_destructuring_budget()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions { Destructuring = { MaxEncodedBytesPerRecord = 16 } }
        );
        var logger = provider.CreateLogger("Budgeted");

        using (logger.BeginScope("first {@First}", new { Filler = new string('x', 64) }))
        using (logger.BeginScope("second {@Second}", new { Value = 1 }))
        {
            logger.LogInformation("inside");
        }

        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        // The outer scope spent the record's budget; the inner one degrades to the sentinel in
        // its field and in its text rather than growing the record without bound.
        Assert.Equal(JsonValueKind.Object, root.GetProperty("First").ValueKind);
        Assert.Equal("…", root.GetProperty("Second").GetString());
        var scope = root.GetProperty("Scope").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.Equal("second …", scope[1]);
    }

    [Fact]
    public async Task A_scope_template_hole_resolves_against_that_scope_only()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        var logger = provider.CreateLogger("Shadowed");
        var orderId = "event";

        using (logger.BeginScope("outer {OrderId}", "outer"))
        using (logger.BeginScope("inner {OrderId} at {When}", "inner", null))
        {
            logger.LogFast(LogLevel.Information, $"order {orderId}");
        }

        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        var scope = root.GetProperty("Scope").EnumerateArray().Select(e => e.GetString()).ToArray();
        // Each text renders its own scope's values even though the flattened OrderId field is
        // won by the inner scope, and a null hole renders as the literal null token.
        Assert.Equal(["outer outer", "inner inner at null"], scope);
        Assert.Equal("inner", root.GetProperty("OrderId").GetString());
        Assert.Equal("event", root.GetProperty("orderId").GetString());
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

    /// <summary>A record: its generated ToString() prints every member, masked or not.</summary>
    private sealed record ScopedUser(string Name, [property: LogMasked] string Token);

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
}
