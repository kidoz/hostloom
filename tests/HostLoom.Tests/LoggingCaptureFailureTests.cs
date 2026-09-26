using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

// CA1873: the boxing standard ILogger path is what these tests exercise on purpose.
#pragma warning disable CA1873
// CA2017/CA2254: a template that does not match its arguments is one of the failures under test.
#pragma warning disable CA2017

namespace HostLoom.Tests;

/// <summary>
/// Caller code that throws while an event is captured: a value's ToString(), a type whose members
/// cannot be reflected, MEL's own formatter. The log call must return normally and every line
/// must stay valid JSON.
/// </summary>
public sealed class LoggingCaptureFailureTests
{
    [Fact]
    public async Task A_throwing_ToString_in_a_plain_hole_costs_only_that_field()
    {
        var (root, _) = await LogAsync(logger =>
            logger.LogInformation("value {Value} next {Next}", new ThrowingToString(), 5)
        );

        Assert.Equal("[DestructuringFailed]", root.GetProperty("Value").GetString());
        Assert.Equal(5, root.GetProperty("Next").GetInt32());
        Assert.Equal("value {Value} next {Next}", root.GetProperty("@mt").GetString());
    }

    [Fact]
    public async Task A_throwing_ToString_through_the_logger_factory_does_not_reach_the_caller()
    {
        // CA2000: sink ownership transfers to the provider.
#pragma warning disable CA2000
        var sink = new CollectingSink();
#pragma warning restore CA2000
        await using var provider = new HostLoomLoggerProvider(
            new ClefLogFormatter(),
            sink,
            new HostLoomLoggerOptions { AttachMachineName = false }
        );
        using var factory = LoggerFactory.Create(builder => builder.AddProvider(provider));

        var exception = Record.Exception(() =>
            factory.CreateLogger("Capture").LogWarning("value {Value}", new ThrowingToString())
        );
        await provider.DisposeAsync();

        Assert.Null(exception);
        Assert.Single(sink.Lines());
    }

    [Fact]
    public async Task A_template_with_too_few_arguments_still_ships_the_event()
    {
        var (root, _) = await LogAsync(logger => logger.LogInformation("{First} and {Second}", 1));

        // MEL's state throws on the missing argument; what was captured before it still ships,
        // and the template is recovered because MEL lists it last and serves it independently.
        Assert.Equal(1, root.GetProperty("First").GetInt32());
        Assert.Equal("{First} and {Second}", root.GetProperty("@mt").GetString());
    }

    [Fact]
    public async Task A_throwing_formatter_without_a_template_ships_a_marked_message()
    {
        var (root, _) = await LogAsync(logger =>
            logger.Log(
                LogLevel.Information,
                default,
                "state",
                null,
                static (_, _) => throw new InvalidOperationException("formatter")
            )
        );

        Assert.Equal("[MessageUnavailable]", root.GetProperty("@m").GetString());
    }

    [Fact]
    public void The_bootstrap_logger_keeps_the_event_when_a_ToString_throws()
    {
        using var output = new MemoryStream();
        using (
            var logger = new HostLoomBootstrapLogger(
                new HostLoomLoggerOptions { AttachMachineName = false },
                output: output
            )
        )
        {
            logger.LogInformation("value {Value}", new ThrowingToString());
        }

        var root = JsonDocument.Parse(output.ToArray()).RootElement;
        Assert.Equal("[DestructuringFailed]", root.GetProperty("Value").GetString());
    }

    [Fact]
    public async Task A_throwing_ToString_inside_a_plain_dictionary_hole_keeps_the_json_valid()
    {
        var (root, _) = await LogAsync(logger =>
            logger.LogInformation(
                "map {Map}",
                new Dictionary<string, object> { ["bad"] = new ThrowingToString(), ["good"] = 2 }
            )
        );

        var map = root.GetProperty("Map");
        Assert.Equal("[DestructuringFailed]", map.GetProperty("bad").GetString());
        Assert.Equal(2, map.GetProperty("good").GetInt32());
    }

    [Fact]
    public async Task An_unreflectable_type_inside_a_collection_keeps_the_json_valid()
    {
        var (root, _) = await LogAsync(logger =>
            logger.LogInformation(
                "items {@Items}",
                new List<object> { 1, new WithThrowingAttribute(), 3 }
            )
        );

        var items = root.GetProperty("Items").EnumerateArray().ToArray();
        Assert.Equal(3, items.Length);
        Assert.Equal("[DestructuringFailed]", items[1].GetString());
        Assert.Equal(3, items[2].GetInt32());
    }

    [Fact]
    public async Task An_unreflectable_type_as_a_member_keeps_the_json_valid()
    {
        var (root, _) = await LogAsync(logger =>
            logger.LogInformation(
                "holder {@Holder}",
                new Dictionary<string, object>
                {
                    ["inner"] = new WithThrowingAttribute(),
                    ["after"] = true,
                }
            )
        );

        var holder = root.GetProperty("Holder");
        Assert.Equal("[DestructuringFailed]", holder.GetProperty("inner").GetString());
        Assert.True(holder.GetProperty("after").GetBoolean());
    }

    private static async Task<(JsonElement Root, string Line)> LogAsync(Action<ILogger> log)
    {
        // CA2000: sink ownership transfers to the provider.
#pragma warning disable CA2000
        var sink = new CollectingSink();
#pragma warning restore CA2000
        await using var provider = new HostLoomLoggerProvider(
            new ClefLogFormatter(),
            sink,
            new HostLoomLoggerOptions { AttachMachineName = false }
        );
        log(provider.CreateLogger("Capture"));
        await provider.DisposeAsync();

        var line = Assert.Single(sink.Lines());
        // Parsing is the assertion that the line is valid JSON.
        return (JsonDocument.Parse(line).RootElement.Clone(), line);
    }

    private sealed class ThrowingToString
    {
        public override string ToString() => throw new InvalidOperationException("ToString");
    }

    [AttributeUsage(AttributeTargets.Property)]
    private sealed class ThrowingAttribute : Attribute
    {
        public ThrowingAttribute() => throw new InvalidOperationException("attribute");
    }

    private sealed class WithThrowingAttribute
    {
        [Throwing]
        public int Value { get; set; } = 1;
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
