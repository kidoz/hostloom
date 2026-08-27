using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

// CA1873: the boxing standard ILogger path is what several of these tests exercise on purpose.
#pragma warning disable CA1873

namespace HostLoom.Tests;

public sealed class LoggingClefFormatterTests
{
    [Fact]
    public async Task A_templated_event_emits_mt_and_typed_fields()
    {
        var (root, _) = await LogAsync(logger => logger.LogInformation("order {OrderId}", 42));

        Assert.True(root.GetProperty("@t").TryGetDateTimeOffset(out _));
        Assert.Equal("order {OrderId}", root.GetProperty("@mt").GetString());
        // @mt and @m are mutually exclusive; @l is omitted for Information; @i never appears.
        Assert.False(root.TryGetProperty("@m", out _));
        Assert.False(root.TryGetProperty("@l", out _));
        Assert.False(root.TryGetProperty("@i", out _));
        Assert.Equal(42, root.GetProperty("OrderId").GetInt32());
        Assert.Equal("Clef", root.GetProperty("SourceContext").GetString());
        Assert.Equal(JsonValueKind.Number, root.GetProperty("ThreadId").ValueKind);
        Assert.False(root.TryGetProperty("EventId", out _));
    }

    [Fact]
    public async Task A_fast_path_event_without_a_template_emits_m()
    {
        var orderId = 7;
        var (root, _) = await LogAsync(logger =>
            logger.LogFast(LogLevel.Information, $"order {orderId} shipped")
        );

        Assert.Equal("order 7 shipped", root.GetProperty("@m").GetString());
        Assert.False(root.TryGetProperty("@mt", out _));
        Assert.Equal(7, root.GetProperty("orderId").GetInt32());
    }

    [Fact]
    public async Task Level_names_match_serilog()
    {
        var (roots, _) = await LogManyAsync(logger =>
        {
            logger.LogFast(LogLevel.Trace, $"a");
            logger.LogFast(LogLevel.Warning, $"b");
            logger.LogFast(LogLevel.Critical, $"c");
        });

        Assert.Equal("Verbose", roots[0].GetProperty("@l").GetString());
        Assert.Equal("Warning", roots[1].GetProperty("@l").GetString());
        Assert.Equal("Fatal", roots[2].GetProperty("@l").GetString());
    }

    [Fact]
    public async Task The_exception_chain_is_complete()
    {
        var failure = new InvalidOperationException(
            "outer failure",
            new ArgumentException("inner failure")
        );
        var (root, _) = await LogAsync(logger =>
            logger.LogFast(LogLevel.Error, failure, $"it broke")
        );

        var text = root.GetProperty("@x").GetString();
        Assert.NotNull(text);
        Assert.Contains("outer failure", text, StringComparison.Ordinal);
        Assert.Contains("inner failure", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_long_exception_is_truncated_with_a_marker()
    {
        var failure = new InvalidOperationException(new string('x', 500));
        var (root, _) = await LogAsync(
            logger => logger.LogFast(LogLevel.Error, failure, $"capped"),
            new ClefLogFormatter(maxExceptionLength: 40)
        );

        var text = root.GetProperty("@x").GetString();
        Assert.NotNull(text);
        Assert.Equal(41, text.Length);
        Assert.EndsWith("…", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Formatted_fast_path_holes_surface_their_renderings()
    {
        var orderId = 42;
        var (root, _) = await LogAsync(logger =>
            logger.LogFast(LogLevel.Information, $"order {orderId:00000}")
        );

        var renderings = root.GetProperty("@r").EnumerateArray().Select(e => e.GetString());
        Assert.Equal(["00042"], renderings);
        Assert.Equal(42, root.GetProperty("orderId").GetInt32());
    }

    [Fact]
    public async Task An_event_id_uses_the_ordinary_property_shape()
    {
        var (root, _) = await LogAsync(logger =>
            logger.LogFast(LogLevel.Information, new EventId(7, "order-rejected"), $"rejected")
        );

        var eventId = root.GetProperty("EventId");
        Assert.Equal(7, eventId.GetProperty("Id").GetInt32());
        Assert.Equal("order-rejected", eventId.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task Clef_reserves_its_core_property_names()
    {
        var SourceContext = "sneaky";
        var (root, _) = await LogAsync(logger =>
            logger.LogFast(LogLevel.Information, $"hi {SourceContext}")
        );

        // The hole was named SourceContext by its variable; the formatter owns that key.
        Assert.Equal(1, root.EnumerateObject().Count(p => p.NameEquals("SourceContext")));
        Assert.Equal("Clef", root.GetProperty("SourceContext").GetString());
    }

    private static async Task<(JsonElement Root, string Line)> LogAsync(
        Action<ILogger> log,
        ClefLogFormatter? formatter = null
    )
    {
        var (roots, lines) = await LogManyAsync(log, formatter);
        return (Assert.Single(roots), Assert.Single(lines));
    }

    private static async Task<(JsonElement[] Roots, string[] Lines)> LogManyAsync(
        Action<ILogger> log,
        ClefLogFormatter? formatter = null
    )
    {
        // CA2000: sink ownership transfers to the provider.
#pragma warning disable CA2000
        var sink = new CollectingSink();
#pragma warning restore CA2000
        await using var provider = new HostLoomLoggerProvider(
            formatter ?? new ClefLogFormatter(),
            sink,
            new HostLoomLoggerOptions { AttachMachineName = false }
        );
        log(provider.CreateLogger("Clef"));
        await provider.DisposeAsync();

        var lines = sink.Lines();
        var roots = lines.Select(line => JsonDocument.Parse(line).RootElement.Clone()).ToArray();
        return (roots, lines);
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
