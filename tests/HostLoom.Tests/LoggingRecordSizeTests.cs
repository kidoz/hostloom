using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

// CA1873: the boxing standard ILogger path is exercised on purpose next to the fast path.
#pragma warning disable CA1873

namespace HostLoom.Tests;

/// <summary>
/// The per-record size caps: message text, plain text fields, and scope texts are bounded in
/// UTF-8 bytes and closed with the same "…" sentinel destructured strings use.
/// </summary>
public sealed class LoggingRecordSizeTests
{
    [Fact]
    public async Task The_message_is_cut_at_MaxMessageLength_and_later_holes_still_ship()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions { MaxMessageLength = 32 }
        );
        var logger = provider.CreateLogger("Capped");
        var filler = new string('x', 100);
        var orderId = 7;
        var express = true;

        logger.LogFast(LogLevel.Information, $"{filler} then {orderId} and {express}");
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal(new string('x', 32) + "…", root.GetProperty("message").GetString());
        // The hole whose bytes were cut keeps its full value as a field, and the holes appended
        // after the message closed still land as typed fields.
        Assert.Equal(filler, root.GetProperty("filler").GetString());
        Assert.Equal(7, root.GetProperty("orderId").GetInt32());
        Assert.True(root.GetProperty("express").GetBoolean());
    }

    [Fact]
    public async Task The_standard_path_message_is_cut_too()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions { MaxMessageLength = 16 }
        );
        var logger = provider.CreateLogger("Capped");

        logger.LogInformation("shipment {ShipmentId} {Filler}", 42, new string('y', 100));
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal("shipment 42 yyyy…", root.GetProperty("message").GetString());
        Assert.Equal(42, root.GetProperty("ShipmentId").GetInt32());
        Assert.Equal(new string('y', 100), root.GetProperty("Filler").GetString());
    }

    [Fact]
    public async Task A_message_cut_lands_on_a_character_boundary()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            // 33 bytes would split the seventeenth two-byte character.
            new HostLoomLoggerOptions { MaxMessageLength = 33 }
        );
        var logger = provider.CreateLogger("Boundary");

        logger.LogInformation("{Accents}", new string('é', 100));
        await provider.DisposeAsync();

        var line = Assert.Single(sink.Lines());
        var root = JsonDocument.Parse(line).RootElement;
        var message = root.GetProperty("message").GetString();
        Assert.Equal(new string('é', 16) + "…", message);
        Assert.DoesNotContain('�', line);
    }

    [Fact]
    public async Task Plain_text_fields_are_cut_at_MaxTextFieldLength_on_both_paths()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions { MaxTextFieldLength = 8 }
        );
        var logger = provider.CreateLogger("Fields");
        var note = new string('n', 100);

        logger.LogInformation("value {Value}", new string('v', 100));
        logger.LogFast(LogLevel.Information, $"note {note}");
        await provider.DisposeAsync();

        var lines = sink.Lines();
        Assert.Equal(2, lines.Length);
        var standard = JsonDocument.Parse(lines[0]).RootElement;
        // The standard path's message is the caller's own rendering, bounded only by the
        // message cap; its field is bounded by the text cap.
        Assert.Equal("value " + new string('v', 100), standard.GetProperty("message").GetString());
        Assert.Equal("vvvvvvvv…", standard.GetProperty("Value").GetString());
        var fast = JsonDocument.Parse(lines[1]).RootElement;
        // The fast path renders the hole into the message as the field, so both are cut.
        Assert.Equal("note nnnnnnnn…", fast.GetProperty("message").GetString());
        Assert.Equal("nnnnnnnn…", fast.GetProperty("note").GetString());
    }

    [Fact]
    public async Task A_text_field_cut_lands_on_a_character_boundary()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions { MaxTextFieldLength = 5 }
        );
        var logger = provider.CreateLogger("Boundary");

        logger.LogInformation("value {Value}", new string('é', 10));
        await provider.DisposeAsync();

        var line = Assert.Single(sink.Lines());
        var root = JsonDocument.Parse(line).RootElement;
        Assert.Equal("éé…", root.GetProperty("Value").GetString());
        Assert.DoesNotContain('�', line);
    }

    [Fact]
    public async Task Enricher_and_static_text_share_the_field_cap()
    {
        var sink = NewSink();
        var options = new HostLoomLoggerOptions
        {
            MaxTextFieldLength = 4,
            ServiceName = "catalog-worker",
            AttachMachineName = false,
        };
        options.Enrichers.Add(new RegionEnricher(new string('r', 20)));
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            options
        );
        var logger = provider.CreateLogger("Enriched");

        logger.LogFast(LogLevel.Information, $"enriched");
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal("rrrr…", root.GetProperty("Region").GetString());
        Assert.Equal("cata…", root.GetProperty("ServiceName").GetString());
    }

    [Fact]
    public async Task Scope_texts_are_cut_at_MaxTextFieldLength()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions { MaxTextFieldLength = 8 }
        );
        var logger = provider.CreateLogger("Scoped");

        using (logger.BeginScope(new string('s', 100)))
        using (logger.BeginScope("step {Step} of {Run}", 3, new string('t', 100)))
        {
            logger.LogFast(LogLevel.Information, $"working");
        }

        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        var scope = root.GetProperty("Scope").EnumerateArray().Select(e => e.GetString()).ToArray();
        // The plain scope text and the rendered template text are both bounded; the templated
        // one was already built from the capped field value.
        Assert.Equal(["ssssssss…", "step 3 o…"], scope);
        Assert.Equal("tttttttt…", root.GetProperty("Run").GetString());
    }

    [Fact]
    public async Task Destructured_json_appended_to_a_safe_rendered_message_is_cut()
    {
        var sink = NewSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions { MaxMessageLength = 22 }
        );
        var logger = provider.CreateLogger("Safe");

        logger.LogInformation("catalog {@Catalog}", new { Region = "eu", Items = 1234567 });
        await provider.DisposeAsync();

        var root = JsonDocument.Parse(Assert.Single(sink.Lines())).RootElement;
        Assert.Equal("catalog {\"Region\":\"eu\"…", root.GetProperty("message").GetString());
        Assert.Equal(1234567, root.GetProperty("Catalog").GetProperty("Items").GetInt32());
    }

    [Fact]
    public async Task The_bootstrap_logger_applies_the_same_caps()
    {
        using var output = new MemoryStream();
        using (
            var bootstrap = new HostLoomBootstrapLogger(
                new HostLoomLoggerOptions
                {
                    MaxMessageLength = 8,
                    MaxTextFieldLength = 4,
                    AttachMachineName = false,
                },
                new JsonLogFormatter(),
                output,
                failFast: true
            )
        )
        {
            bootstrap.LogInformation("starting {Component}", "catalog-worker");
        }

        var lines = Encoding
            .UTF8.GetString(output.ToArray())
            .Split('\n', StringSplitOptions.RemoveEmptyEntries);
        var root = JsonDocument.Parse(Assert.Single(lines)).RootElement;
        Assert.Equal("starting…", root.GetProperty("message").GetString());
        Assert.Equal("cata…", root.GetProperty("Component").GetString());
    }

    [Fact]
    public void The_caps_default_to_sixteen_and_eight_kibibytes()
    {
        var options = new HostLoomLoggerOptions();

        Assert.Equal(16 * 1024, options.MaxMessageLength);
        Assert.Equal(8 * 1024, options.MaxTextFieldLength);
    }

    [Fact]
    public void Encoding_large_text_allocates_only_the_configured_budget()
    {
        var text = new string('é', 1_000_000);
        var utf8 = Encoding.UTF8.GetBytes(text);
        var entry = new LogEntry();
        entry.ApplyCaps(
            new HostLoomLoggerOptions { MaxMessageLength = 33, MaxTextFieldLength = 5 }
        );
        var before = GC.GetAllocatedBytesForCurrentThread();
        entry.AppendLiteral(text);
        entry.AddFieldText("Note", text);
        entry.AddFieldUtf8Text("Region", utf8, LogFieldSource.Static);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(allocated < 64 * 1024, $"Bounded text allocated {allocated} bytes.");
        Assert.Equal(new string('é', 16) + "…", Encoding.UTF8.GetString(entry.Message));
        for (var i = 0; i < entry.FieldCount; i++)
        {
            entry.GetField(i, out _, out var value, out _);
            Assert.Equal("éé…", Encoding.UTF8.GetString(value));
        }
    }

    [Theory]
    [InlineData("abc", 3, "abc")]
    [InlineData("abcé", 4, "abc…")]
    [InlineData("😀x", 4, "😀…")]
    [InlineData("😀", 3, "…")]
    [InlineData("a\uD800b", 4, "a�…")]
    public void Bounded_encoding_preserves_utf8_and_marks_only_truncated_text(
        string input,
        int cap,
        string expected
    )
    {
        var entry = new LogEntry();
        entry.ApplyCaps(
            new HostLoomLoggerOptions { MaxMessageLength = cap, MaxTextFieldLength = cap }
        );
        entry.AppendText(input, "Note");
        Assert.Equal(expected, Encoding.UTF8.GetString(entry.Message));
        entry.GetField(0, out _, out var value, out _);
        Assert.Equal(expected, Encoding.UTF8.GetString(value));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Clef_uses_the_capped_message_when_the_original_template_is_oversized(
        bool bootstrap
    )
    {
        var template = new string('é', 1_000_000) + " {@Catalog}";
        var options = new HostLoomLoggerOptions
        {
            MaxMessageLength = 33,
            AttachMachineName = false,
        };
        string line;
        if (bootstrap)
        {
            using var output = new MemoryStream();
            using (
                var logger = new HostLoomBootstrapLogger(
                    options,
                    new ClefLogFormatter(),
                    output,
                    failFast: true
                )
            )
            {
                Log(logger);
            }
            line = Encoding.UTF8.GetString(output.ToArray());
        }
        else
        {
            var sink = NewSink();
            await using var provider = new HostLoomLoggerProvider(
                new ClefLogFormatter(),
                sink,
                options
            );
            Log(provider.CreateLogger("Capped"));
            await provider.DisposeAsync();
            line = Assert.Single(sink.Lines());
        }

        var root = JsonDocument.Parse(line).RootElement;
        Assert.False(root.TryGetProperty("@mt", out _));
        Assert.Equal(new string('é', 16) + "…", root.GetProperty("@m").GetString());
        Assert.Equal("eu", root.GetProperty("Catalog").GetProperty("Region").GetString());
        Assert.DoesNotContain("private-value", line, StringComparison.Ordinal);

        // A dynamic template is deliberate: it must be bounded just like a literal template.
#pragma warning disable CA2254
        void Log(ILogger logger) => logger.LogInformation(template, new ProtectedCatalog());
#pragma warning restore CA2254
    }

    [Fact]
    public async Task A_blocked_writer_does_not_retain_input_sized_text_buffers_in_queued_records()
    {
        var sink = NewHeldSink();
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions
            {
                MaxMessageLength = 32,
                MaxTextFieldLength = 32,
                QueueCapacity = 4,
            }
        );
        var logger = provider.CreateLogger("Queued");
        var note = new string('n', 1_000_000);
        try
        {
            logger.LogFast(LogLevel.Information, $"first");
            await sink.Entered.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken
            );
            var before = GC.GetAllocatedBytesForCurrentThread();
            logger.LogFast(LogLevel.Information, $"queued {note}");
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.True(allocated < 64 * 1024, $"Queued capture allocated {allocated} bytes.");
        }
        finally
        {
            sink.Release();
        }
    }

    private sealed class ProtectedCatalog
    {
        public string Region { get; } = "eu";

        [NotLogged]
        public string Secret { get; } = "private-value";

        public override string ToString() => Secret;
    }

    private sealed class HeldSink : ILogSink
    {
        private readonly ManualResetEventSlim _release = new(false);
        public TaskCompletionSource Entered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken)
        {
            Entered.TrySetResult();
            _release.Wait(cancellationToken);
        }

        public void Release() => _release.Set();

        public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync()
        {
            _release.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    // CA2000: sink ownership transfers to the provider.
#pragma warning disable CA2000
    private static CollectingSink NewSink() => new();

    private static HeldSink NewHeldSink() => new();
#pragma warning restore CA2000

    private sealed class RegionEnricher(string region) : ILogEnricher
    {
        public void Enrich(ref LogEntryWriter writer) => writer.Add("Region", region);
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
