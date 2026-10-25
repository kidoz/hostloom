using System.Text;
using HostLoom.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

public sealed class LoggingReviewRegressionTests
{
    [Theory]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    public void Extreme_alignment_is_bounded_before_padding(int alignment)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        var text = EventCapture.RenderToken(7, "D2", alignment, maxBytes: 64);
        Assert.True(GC.GetAllocatedBytesForCurrentThread() - before < 64 * 1024);
        Assert.InRange(Encoding.UTF8.GetByteCount(text), 1, 64);
    }

    [Fact]
    public void Extreme_numeric_precision_is_bounded_before_formatting()
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        var text = EventCapture.RenderToken(7, "D999999999", null, maxBytes: 64);
        Assert.True(GC.GetAllocatedBytesForCurrentThread() - before < 64 * 1024);
        Assert.InRange(Encoding.UTF8.GetByteCount(text), 1, 64);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Renderings_obey_both_count_and_aggregate_byte_limits(bool countLimit)
    {
        var options = new HostLoomLoggerOptions
        {
            MaxFieldsPerRecord = countLimit ? 4 : 64,
            MaxMessageLength = countLimit ? 512 : 128,
            MaxTextFieldLength = 64,
        };
        var value = new FormattedValue();
        KeyValuePair<string, object?>[] state =
        [
            new("Value", value),
            new(
                "{OriginalFormat}",
                string.Concat(Enumerable.Repeat("{Value:N2}", countLimit ? 20 : 8))
            ),
        ];
        var capture = new EventCapture(
            options,
            new Destructurer(options.Destructuring, null),
            null
        );
        var entry = new LogEntry();
        entry.ApplyCaps(options);
        capture.CaptureEvent(entry, state, null, static (_, _) => "message");
        Assert.NotNull(entry.TemplateRenderings);
        Assert.Equal(countLimit ? 4 : 2, entry.TemplateRenderings.Count);
        Assert.True(
            entry.TemplateRenderings.Sum(Encoding.UTF8.GetByteCount) <= options.MaxMessageLength
        );
    }

    [Fact]
    public async Task Json_output_does_not_capture_unused_CLEF_renderings()
    {
        var value = new FormattedValue();
        KeyValuePair<string, object?>[] state =
        [
            new("Value", value),
            new("{OriginalFormat}", "{Value:N2}"),
        ];
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            NewSink(),
            new HostLoomLoggerOptions()
        );
        provider
            .CreateLogger("catalog")
            .Log(LogLevel.Information, new EventId(1), state, null, static (_, _) => "message");
        Assert.Equal(0, value.Formatted);
    }

    [Fact]
    public async Task Sharing_one_sink_across_containers_is_rejected_without_disposing_its_owner()
    {
        var sink = NewSink();
        var services = new ServiceCollection();
        services.AddLogging(logging => logging.AddHostLoomLogging(sink));
        await using var first = services.BuildServiceProvider();
        await using var second = services.BuildServiceProvider();
        first.GetRequiredService<ILoggerFactory>().CreateLogger("catalog");
        var error = Assert.Throws<InvalidOperationException>(() =>
            second.GetRequiredService<ILoggerFactory>()
        );
        Assert.Contains("sink", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, sink.Disposals);
        await first.DisposeAsync();
        Assert.Equal(1, sink.Disposals);
    }

    private sealed class FormattedValue : IFormattable
    {
        public int Formatted { get; private set; }

        public string ToString(string? format, IFormatProvider? formatProvider)
        {
            if (format is not null)
                Formatted++;
            return new string('x', 64);
        }

        public override string ToString() => "value";
    }

    private static Sink NewSink() => new();

    private sealed class Sink : ILogSink
    {
        public int Disposals { get; private set; }

        public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken) { }

        public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync()
        {
            Disposals++;
            return ValueTask.CompletedTask;
        }
    }
}
