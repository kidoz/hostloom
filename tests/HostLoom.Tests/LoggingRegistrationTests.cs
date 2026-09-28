using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.Configuration;
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
    public async Task A_sink_factory_gives_each_container_a_sink_of_its_own()
    {
        var sinks = new List<RecordingSink>();
        var services = new ServiceCollection();
        services.AddLogging(logging =>
            logging.AddHostLoomLogging(_ =>
            {
                var sink = new RecordingSink();
                sinks.Add(sink);
                return sink;
            })
        );

        await using (services.BuildServiceProvider())
        {
            // A container that never resolves logging never creates a sink.
        }

        Assert.Empty(sinks);

        foreach (var category in new[] { "first", "second" })
        {
            await using var provider = services.BuildServiceProvider();
            provider
                .GetRequiredService<ILoggerFactory>()
                .CreateLogger(category)
                .LogInformation("Order placed.");
        }

        Assert.Collection(
            sinks,
            first =>
            {
                Assert.Contains(
                    "\"first\"",
                    Assert.Single(first.Lines()),
                    StringComparison.Ordinal
                );
                Assert.True(first.Disposed);
            },
            second =>
            {
                Assert.Contains(
                    "\"second\"",
                    Assert.Single(second.Lines()),
                    StringComparison.Ordinal
                );
                Assert.True(second.Disposed);
            }
        );
    }

    [Theory]
    [InlineData("00:00:02", 2_000)]
    [InlineData("", null)]
    public void Configuration_sets_the_enqueue_timeout_or_lifts_it_with_an_empty_value(
        string value,
        int? milliseconds
    )
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["EnqueueTimeout"] = value })
            .Build();
        var options = new HostLoomLoggerOptions();
        Assert.Equal(TimeSpan.FromSeconds(1), options.EnqueueTimeout);

        options.Bind(configuration);

        Assert.Equal(
            milliseconds is { } ms ? TimeSpan.FromMilliseconds(ms) : null,
            options.EnqueueTimeout
        );
    }

    [Fact]
    public async Task A_sink_factory_registration_binds_its_options_from_configuration()
    {
        RecordingSink? sink = null;
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["ServiceName"] = "checkout" })
            .Build();
        var services = new ServiceCollection();
        services.AddLogging(logging =>
            logging.AddHostLoomLogging(_ => sink = new RecordingSink(), configuration)
        );

        await using (var provider = services.BuildServiceProvider())
        {
            provider
                .GetRequiredService<ILogger<LoggingRegistrationTests>>()
                .LogInformation("Order placed.");
        }

        Assert.NotNull(sink);
        using var record = JsonDocument.Parse(Assert.Single(sink.Lines()));
        Assert.Equal("checkout", record.RootElement.GetProperty("ServiceName").GetString());
    }

    [Fact]
    public async Task A_sink_factory_that_returns_null_fails_when_logging_is_resolved()
    {
        var services = new ServiceCollection();
        services.AddLogging(logging => logging.AddHostLoomLogging(_ => null!));
        await using var provider = services.BuildServiceProvider();

        var failure = Assert.Throws<InvalidOperationException>(() =>
            provider.GetRequiredService<ILoggerFactory>()
        );
        Assert.Contains("returned null", failure.Message, StringComparison.Ordinal);
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

    [Theory]
    [InlineData("Enrichers:0", "TraceContext")]
    [InlineData("enrichers:0:Name", "TraceContext")]
    [InlineData("Enrichers", "")]
    [InlineData("TimeProvider:LocalTimeZone:Id", "UTC")]
    public void Configuration_cannot_set_the_options_only_code_can_set(string key, string value)
    {
        var configuration = Configuration((key, value));

        var failure = Assert.Throws<InvalidOperationException>(() =>
            new HostLoomLoggerOptions().Bind(configuration)
        );
        Assert.Contains("in code", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Bind_keeps_values_set_before_it_as_defaults_that_configuration_overrides()
    {
        var options = new HostLoomLoggerOptions { ServiceName = "default" };
        options.Destructuring.TypeTags = true;

        var bound = options.Bind(Configuration(("ServiceName", "checkout")));

        Assert.Same(options, bound);
        Assert.Equal("checkout", options.ServiceName);
        Assert.True(options.Destructuring.TypeTags);
    }

    private static IConfiguration Configuration(params (string Key, string Value)[] values) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(
                values.Select(value => KeyValuePair.Create(value.Key, (string?)value.Value))
            )
            .Build();

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
