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

        [Fact]
        public async Task A_property_without_a_public_getter_is_not_read()
        {
            var (root, _) = await LogAsync(logger =>
                logger.LogInformation(
                    "user {@User}",
                    new WriteOnlySecret { Name = "ada", Password = "hunter2" }
                )
            );

            Assert.Equal("ada", root.GetProperty("User").GetProperty("Name").GetString());
            Assert.False(root.GetProperty("User").TryGetProperty("Password", out _));
        }

        [Fact]
        public async Task Delegates_and_reflection_types_are_written_as_their_names()
        {
            var secret = "PASSWORD123";
            var value = new WithDelegateAndType
            {
                Id = 7,
                OnDone = () => GC.KeepAlive(secret),
                Kind = typeof(CardHolder),
            };
            var (root, line) = await LogAsync(logger => logger.LogInformation("job {@Job}", value));

            var job = root.GetProperty("Job");
            Assert.DoesNotContain("PASSWORD123", line, StringComparison.Ordinal);
            Assert.Equal("System.Action", job.GetProperty("OnDone").GetString());
            Assert.Equal(typeof(CardHolder).ToString(), job.GetProperty("Kind").GetString());
            Assert.Equal(7, job.GetProperty("Id").GetInt32());
        }

        [Fact]
        public async Task A_destructured_exception_stays_small()
        {
            var exception = Throw();
            var (root, line) = await LogAsync(logger =>
                logger.LogError("failed {@Error}", exception)
            );

            Assert.Equal(
                JsonValueKind.String,
                root.GetProperty("Error").GetProperty("TargetSite").ValueKind
            );
            Assert.True(line.Length < 16 * 1024, $"line is {line.Length} bytes");
        }

        [Fact]
        public async Task Byte_memory_is_written_as_hex()
        {
            var (root, _) = await LogAsync(logger =>
                logger.LogInformation(
                    "bytes {Bytes} memory {@Memory}",
                    new ReadOnlyMemory<byte>([1, 0xAB]),
                    new Memory<byte>([2])
                )
            );

            Assert.Equal("01AB", root.GetProperty("Bytes").GetString());
            Assert.Equal("02", root.GetProperty("Memory").GetString());
        }

        [Fact]
        public async Task A_hiding_property_is_written_once_with_the_derived_value()
        {
            var (root, line) = await LogAsync(logger =>
                logger.LogInformation("hidden {@Value}", new HidingDerived { Id = "derived" })
            );

            Assert.Equal("derived", root.GetProperty("Value").GetProperty("Id").GetString());
            Assert.Equal(1, Occurrences(line, "\"Id\":"));
        }

        [Fact]
        public async Task Dictionary_keys_that_render_alike_are_written_once()
        {
            var map = new Dictionary<object, int> { [new SameText()] = 1, [new SameText()] = 2 };
            var (_, line) = await LogAsync(logger => logger.LogInformation("map {@Map}", map));

            Assert.Equal(1, Occurrences(line, "\"K\":"));
        }

        private static int Occurrences(string text, string value)
        {
            var count = 0;
            for (
                var index = text.IndexOf(value, StringComparison.Ordinal);
                index >= 0;
                index = text.IndexOf(value, index + 1, StringComparison.Ordinal)
            )
            {
                count++;
            }

            return count;
        }

        private static InvalidOperationException Throw()
        {
            try
            {
                throw new InvalidOperationException("boom");
            }
            catch (InvalidOperationException exception)
            {
                return exception;
            }
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

        private sealed record CardHolder
        {
            public int Id { get; init; }

            [LogMasked]
            public string Pan { get; init; } = "";
        }

        private sealed class WriteOnlySecret
        {
            public string Name { get; set; } = "";

            public string Password { private get; set; } = "";

            public int PasswordLength => Password.Length;
        }

        private sealed class WithDelegateAndType
        {
            public int Id { get; set; }

            public Action? OnDone { get; set; }

            public Type? Kind { get; set; }
        }

        private class HidingBase
        {
            public int Id { get; set; } = 7;
        }

        private sealed class HidingDerived : HidingBase
        {
            public new string Id { get; set; } = "";
        }

        private sealed class SameText
        {
            public override string ToString() => "K";
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
