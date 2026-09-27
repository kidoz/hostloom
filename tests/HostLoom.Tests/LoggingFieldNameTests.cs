using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

#pragma warning disable CA1873 // Deliberately exercise standard capture and dynamic template inputs.

namespace HostLoom.Tests;

public sealed class LoggingFieldNameTests
{
    [Theory]
    [InlineData("@", false)]
    [InlineData("$", false)]
    [InlineData("@", true)]
    [InlineData("$", true)]
    public void Prefixed_names_do_not_allocate_input_sized_copies_even_past_the_capture_ceiling(
        string prefix,
        bool scope
    )
    {
        var options = new HostLoomLoggerOptions { MaxFieldNameLength = 16, MaxFieldsPerRecord = 1 };
        var capture = new EventCapture(
            options,
            new Destructurer(options.Destructuring, null),
            null
        );
        var entry = new LogEntry();
        var state = Enumerable
            .Repeat(new KeyValuePair<string, object?>(prefix + new string('x', 1_000_000), 42), 8)
            .ToArray();
        entry.ApplyCaps(options);
        Capture();
        entry.Reset();
        entry.ApplyCaps(options);

        var before = GC.GetAllocatedBytesForCurrentThread();
        Capture();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.True(allocated < 64 * 1024, $"Capture allocated {allocated} bytes.");
        entry.FinalizeCapture();
        Assert.Equal(0, entry.FieldCount);

        void Capture()
        {
            if (scope)
                capture.CaptureScope(state, entry);
            else
                capture.CaptureState(entry, state);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Rejected_fields_remain_available_for_masked_event_and_scope_rendering(
        bool clef
    )
    {
        using var output = new MemoryStream();
#pragma warning disable CA2000 // The provider owns the sink.
        await using var provider = new HostLoomLoggerProvider(
            clef ? new ClefLogFormatter() : new JsonLogFormatter(),
            new StreamLogSink(output, leaveOpen: true),
            new() { MaxFieldNameLength = 8, AttachMachineName = false }
        );
#pragma warning restore CA2000
        var logger = provider.CreateLogger("Capture");
        using (logger.BeginScope("outer {UserProfile}", new Profile("outer", "outer-secret")))
        using (logger.BeginScope("inner {UserProfile}", new Profile("inner", "inner-secret")))
            logger.LogInformation("profile {@UserProfile:j}", new Profile("event", "event-secret"));

        Assert.True(provider.Flush(TimeSpan.FromSeconds(5)));
        var line = Encoding.UTF8.GetString(output.ToArray());
        using var json = JsonDocument.Parse(line);
        Assert.False(json.RootElement.TryGetProperty("UserProfile", out _));
        var rendered = clef
            ? json.RootElement.GetProperty("@r")[0].GetString()!
            : json.RootElement.GetProperty("message").GetString()!["profile ".Length..];
        using var profile = JsonDocument.Parse(rendered);
        Assert.Equal("event", profile.RootElement.GetProperty("Region").GetString());
        Assert.Equal("***", profile.RootElement.GetProperty("Token").GetString());
        var scopes = json.RootElement.GetProperty("Scope");
        Assert.Equal("outer {\"Region\":\"outer\",\"Token\":\"***\"}", scopes[0].GetString());
        Assert.Equal("inner {\"Region\":\"inner\",\"Token\":\"***\"}", scopes[1].GetString());
        Assert.DoesNotContain("secret", line, StringComparison.Ordinal);
        Assert.Equal(0, provider.Dropped);
    }

    [Fact]
    public async Task Rejected_scalar_names_still_render_their_values()
    {
        using var output = new MemoryStream();
#pragma warning disable CA2000 // The provider owns the sink.
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            new StreamLogSink(output, leaveOpen: true),
            new() { MaxFieldNameLength = 4, AttachMachineName = false }
        );
#pragma warning restore CA2000
        provider.CreateLogger("Capture").LogInformation("count {@Count}", 42);
        Assert.True(provider.Flush(TimeSpan.FromSeconds(5)));
        using var json = JsonDocument.Parse(output.ToArray());
        Assert.Equal("count 42", json.RootElement.GetProperty("message").GetString());
        Assert.False(json.RootElement.TryGetProperty("Count", out _));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Wrapped_names_are_validated_by_each_receiving_provider(bool unicode)
    {
        using var smallOutput = new MemoryStream();
        using var largeOutput = new MemoryStream();
#pragma warning disable CA2000 // Each provider owns its sink.
        await using var small = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            new StreamLogSink(smallOutput, leaveOpen: true),
            new() { MaxFieldNameLength = 128, AttachMachineName = false }
        );
        await using var large = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            new StreamLogSink(largeOutput, leaveOpen: true),
            new() { MaxFieldNameLength = 256, AttachMachineName = false }
        );
#pragma warning restore CA2000
        using var factory = LoggerFactory.Create(builder =>
            builder.AddProvider(small).AddProvider(large)
        );
        var name = unicode ? new string('é', 100) : new string('x', 200);
        foreach (
            var logger in new[] { large.CreateLogger("Direct"), factory.CreateLogger("Wrapped") }
        )
        {
            var handler = new LogMessageHandler(0, 1, logger, LogLevel.Information, out _);
            handler.AppendFormatted(42, name: name);
            logger.LogFast(LogLevel.Information, ref handler);
        }
        Assert.True(small.Flush(TimeSpan.FromSeconds(5)));
        Assert.True(large.Flush(TimeSpan.FromSeconds(5)));
        using var rejected = JsonDocument.Parse(smallOutput.ToArray());
        Assert.False(rejected.RootElement.TryGetProperty(name, out _));
        var lines = Encoding
            .UTF8.GetString(largeOutput.ToArray())
            .Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(2, lines.Length);
        foreach (var line in lines)
        {
            using var accepted = JsonDocument.Parse(line);
            Assert.Equal(42, accepted.RootElement.GetProperty(name).GetInt32());
        }
    }

    private sealed record Profile(string Region, [property: LogMasked] string Token);
}
