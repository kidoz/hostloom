using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

public sealed class LoggingFindingTests
{
    public static TheoryData<object> Numbers =>
        new()
        {
            (byte)7,
            (sbyte)-7,
            (short)-7,
            (ushort)7,
            (uint)7,
            (ulong)7,
            (float)1.5,
            (Half)1.5,
            (Int128)7,
            (UInt128)7,
            (nint)7,
            (nuint)7,
        };

    [Theory]
    [MemberData(nameof(Numbers))]
    public async Task LogFast_preserves_numeric_JSON_types_for_generic_numeric_values(object value)
    {
        await using var sink = CreateSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions()
        );
        provider.CreateLogger("catalog").LogFast(LogLevel.Information, $"count {value}");
        await provider.DisposeAsync();
        using var json = JsonDocument.Parse(sink.Text);
        Assert.Equal(JsonValueKind.Number, json.RootElement.GetProperty("value").ValueKind);
    }

    [Fact]
    public void Bootstrap_flush_obeys_the_failure_policy()
    {
        using var stream = new FailingFlushStream();
        using (var quiet = new HostLoomBootstrapLogger(output: stream))
            quiet.Flush();
        using var loud = new HostLoomBootstrapLogger(output: stream, failFast: true);
        Assert.Throws<IOException>(loud.Flush);
        Assert.Throws<IOException>(loud.Dispose);
    }

    private sealed class FailingFlushStream : MemoryStream
    {
        public override void Flush() => throw new IOException("flush failed");
    }

    private static BufferSink CreateSink() => new();

    private sealed class BufferSink : ILogSink
    {
        public string Text { get; private set; } = "";

        public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken) =>
            Text += Encoding.UTF8.GetString(payload);

        public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
