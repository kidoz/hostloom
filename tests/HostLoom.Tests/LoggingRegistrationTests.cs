using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

public sealed class LoggingRegistrationTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Disposing_the_container_drains_the_pipeline_and_releases_the_sink(bool async)
    {
#pragma warning disable CA2000 // Ownership transfers to the provider, which disposes the sink.
        var sink = new RecordingSink();
#pragma warning restore CA2000
        var services = new ServiceCollection();
        services.AddLogging(logging => logging.AddHostLoomLogging(sink));
        var provider = services.BuildServiceProvider();

        provider
            .GetRequiredService<ILogger<LoggingRegistrationTests>>()
            .LogCritical("Host terminated.");
        if (async)
        {
            await provider.DisposeAsync();
        }
        else
        {
#pragma warning disable CA1849 // The synchronous path is the one under test.
            provider.Dispose();
#pragma warning restore CA1849
        }

        Assert.Contains(
            sink.Lines(),
            line => line.Contains("Host terminated.", StringComparison.Ordinal)
        );
        Assert.True(sink.Flushed);
        Assert.True(sink.Disposed);
    }

    [Fact]
    public async Task An_exception_whose_message_throws_is_logged_without_faulting_the_writer()
    {
#pragma warning disable CA2000 // Ownership transfers to the provider, which disposes the sink.
        var sink = new RecordingSink();
#pragma warning restore CA2000
        await using (
            var provider = new HostLoomLoggerProvider(
                new JsonLogFormatter(),
                sink,
                new HostLoomLoggerOptions()
            )
        )
        {
            var logger = provider.CreateLogger("Orders");
            logger.LogError(new UnreadableMessageException(), "Order lookup failed.");
            logger.LogInformation("Order lookup recovered.");
            await provider.DisposeAsync();

            Assert.Null(provider.WriterFault);
        }

        var lines = sink.Lines();
        Assert.Equal(2, lines.Length);
        using var failed = JsonDocument.Parse(lines[0]);
        Assert.Equal(
            "[MessageUnavailable]",
            failed.RootElement.GetProperty("error.message").GetString()
        );
        Assert.Contains("Order lookup recovered.", lines[1], StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_exception_message_is_capped_like_the_stack_trace()
    {
#pragma warning disable CA2000 // Ownership transfers to the provider, which disposes the sink.
        var sink = new RecordingSink();
#pragma warning restore CA2000
        await using (
            var provider = new HostLoomLoggerProvider(
                new JsonLogFormatter(maxExceptionLength: 64),
                sink,
                new HostLoomLoggerOptions()
            )
        )
        {
            provider
                .CreateLogger("Orders")
                .LogError(new InvalidOperationException(new string('x', 10_000)), "Import failed.");
        }

        using var record = JsonDocument.Parse(Assert.Single(sink.Lines()));
        var message = record.RootElement.GetProperty("error.message").GetString();
        Assert.Equal(new string('x', 64) + "…", message);
    }

    private sealed class UnreadableMessageException : Exception
    {
        public override string Message =>
            throw new InvalidOperationException("The message depends on state that is gone.");
    }

    private sealed class RecordingSink : ILogSink
    {
        private readonly MemoryStream _stream = new();
        private readonly Lock _gate = new();

        public bool Flushed { get; private set; }

        public bool Disposed { get; private set; }

        public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                _stream.Write(payload);
            }
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken)
        {
            Flushed = true;
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }

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
