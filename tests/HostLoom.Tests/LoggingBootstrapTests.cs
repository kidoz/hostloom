using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Xunit;

// CA1873: the boxing standard ILogger path is what several of these tests exercise on purpose.
#pragma warning disable CA1873

namespace HostLoom.Tests;

public sealed class LoggingBootstrapTests
{
    [Fact]
    public void The_bootstrap_logger_emits_the_hosted_event_shape_synchronously()
    {
        using var output = new MemoryStream();
        using var logger = new HostLoomBootstrapLogger(
            new HostLoomLoggerOptions { ServiceName = "checkout" },
            output: output,
            category: "Startup"
        );

        logger.LogInformation("migrating {Step}", 3);
        // Synchronous by design: the line is on the stream before the call returns, so a crash
        // one instruction later loses nothing.
        var root = ParseSingle(output);
        Assert.Equal("migrating {Step}", root.GetProperty("@mt").GetString());
        Assert.Equal(3, root.GetProperty("Step").GetInt32());
        Assert.Equal("Startup", root.GetProperty("SourceContext").GetString());
        Assert.Equal("checkout", root.GetProperty("ServiceName").GetString());
        Assert.Equal(Environment.MachineName, root.GetProperty("MachineName").GetString());
    }

    [Fact]
    public void The_bootstrap_logger_masks_like_the_hosted_provider()
    {
        using var output = new MemoryStream();
        using var logger = new HostLoomBootstrapLogger(output: output);

        logger.LogInformation("creds {@Login}", new Login { User = "ada", Password = "hunter2" });

        var line = ReadAll(output);
        var root = JsonDocument.Parse(line.Split('\n')[0]).RootElement;
        Assert.Equal("ada", root.GetProperty("Login").GetProperty("User").GetString());
        Assert.False(root.GetProperty("Login").TryGetProperty("Password", out _));
        Assert.DoesNotContain("hunter2", line, StringComparison.Ordinal);
    }

    [Fact]
    public void The_minimum_level_filters_before_the_host_exists()
    {
        using var output = new MemoryStream();
        using var logger = new HostLoomBootstrapLogger(output: output);

        Assert.False(logger.IsEnabled(LogLevel.Debug));
        logger.LogDebug("invisible {Value}", 1);
        logger.LogWarning("visible");

        var root = ParseSingle(output);
        Assert.Equal("Warning", root.GetProperty("@l").GetString());
    }

    [Fact]
    public void Bootstrap_failures_are_swallowed_unless_fail_fast()
    {
        using var output = new MemoryStream();
        using var quiet = new HostLoomBootstrapLogger(
            formatter: new ThrowingFormatter(),
            output: output
        );
        quiet.LogInformation("swallowed");

        using var loud = new HostLoomBootstrapLogger(
            formatter: new ThrowingFormatter(),
            output: output,
            failFast: true
        );
        Assert.Throws<InvalidOperationException>(() => loud.LogInformation("thrown"));
    }

    [Fact]
    public void Options_bind_from_configuration_with_the_callback_applying_after()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(
                new Dictionary<string, string?>
                {
                    ["HostLoom:Logging:QueueCapacity"] = "1024",
                    ["HostLoom:Logging:QueueFullPolicy"] = "Block",
                    ["HostLoom:Logging:EnqueueTimeout"] = "00:00:02",
                    ["HostLoom:Logging:ShutdownTimeout"] = "00:00:09",
                    ["HostLoom:Logging:ServiceName"] = "checkout",
                    ["HostLoom:Logging:AttachMachineName"] = "false",
                    ["HostLoom:Logging:Destructuring:MaxDepth"] = "3",
                }
            )
            .Build();

        var options = new HostLoomLoggerOptions();
        HostLoom.Logging.LoggingBuilderExtensions.BindOptions(
            options,
            configuration.GetSection("HostLoom:Logging")
        );
        // The code callback applies after configuration and wins.
        options.ServiceName = "callback-wins";

        Assert.Equal(1024, options.QueueCapacity);
        Assert.Equal(QueueFullPolicy.Block, options.QueueFullPolicy);
        Assert.Equal(TimeSpan.FromSeconds(2), options.EnqueueTimeout);
        Assert.Equal(TimeSpan.FromSeconds(9), options.ShutdownTimeout);
        Assert.Equal("callback-wins", options.ServiceName);
        Assert.False(options.AttachMachineName);
        Assert.Equal(3, options.Destructuring.MaxDepth);
    }

    [Fact]
    public void An_unknown_or_invalid_configuration_key_fails_startup()
    {
        var misspelled = new ConfigurationBuilder()
            .AddInMemoryCollection(
                new Dictionary<string, string?> { ["HostLoom:Logging:QueueCapcity"] = "1024" }
            )
            .Build();
        Assert.Throws<InvalidOperationException>(() =>
            HostLoom.Logging.LoggingBuilderExtensions.BindOptions(
                new HostLoomLoggerOptions(),
                misspelled.GetSection("HostLoom:Logging")
            )
        );

        var invalid = new ConfigurationBuilder()
            .AddInMemoryCollection(
                new Dictionary<string, string?>
                {
                    ["HostLoom:Logging:QueueFullPolicy"] = "NotAPolicy",
                }
            )
            .Build();
        Assert.Throws<InvalidOperationException>(() =>
            HostLoom.Logging.LoggingBuilderExtensions.BindOptions(
                new HostLoomLoggerOptions(),
                invalid.GetSection("HostLoom:Logging")
            )
        );
    }

    private static JsonElement ParseSingle(MemoryStream output)
    {
        var lines = ReadAll(output).Split('\n', StringSplitOptions.RemoveEmptyEntries);
        return JsonDocument.Parse(Assert.Single(lines)).RootElement.Clone();
    }

    private static string ReadAll(MemoryStream output) => Encoding.UTF8.GetString(output.ToArray());

    private sealed class Login
    {
        public string User { get; set; } = "";

        [NotLogged]
        public string Password { get; set; } = "";
    }

    private sealed class ThrowingFormatter : ILogFormatter
    {
        public void Format(in LogRecord record, System.Buffers.IBufferWriter<byte> writer) =>
            throw new InvalidOperationException("the bootstrap formatter broke");
    }
}
