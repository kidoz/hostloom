using System.Collections;
using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

// CA1873: the boxing standard ILogger path is what these tests exercise on purpose.
#pragma warning disable CA1873

namespace HostLoom.Tests
{
    /// <summary>
    /// Behaviours that code written against Serilog relies on, checked against what Serilog 4.4
    /// with Serilog.Extensions.Logging writes for the same call.
    /// </summary>
    public sealed class LoggingSerilogParityTests
    {
        [Fact]
        public async Task A_sequence_in_a_plain_hole_is_enumerated_once()
        {
            var sequence = new CountingSequence();
            var (root, _) = await LogAsync(logger =>
                logger.LogInformation("items {Items}", sequence)
            );

            Assert.Equal(1, sequence.Enumerations);
            Assert.Equal(3, root.GetProperty("Items").GetArrayLength());
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
            log(provider.CreateLogger("Parity"));
            await provider.DisposeAsync();

            var line = Assert.Single(sink.Lines());
            return (JsonDocument.Parse(line).RootElement.Clone(), line);
        }

        private sealed class CountingSequence : IEnumerable<int>
        {
            public int Enumerations { get; private set; }

            public IEnumerator<int> GetEnumerator()
            {
                Enumerations++;
                return new List<int> { 1, 2, 3 }.GetEnumerator();
            }

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
}
