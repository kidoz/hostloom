using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

// CA1873: the boxing standard ILogger path is what these tests exercise on purpose.
#pragma warning disable CA1873

namespace HostLoom.Tests;

/// <summary>
/// Field bookkeeping under pathological records, formatter sharing between pipelines, and drop
/// attribution while a provider is being disposed.
/// </summary>
public sealed class LoggingPipelineHardeningTests
{
    [Fact]
    public void Capture_records_at_most_four_times_the_field_cap()
    {
        var entry = new LogEntry();
        entry.ApplyCaps(new HostLoomLoggerOptions { MaxFieldsPerRecord = 8 });

        for (var i = 0; i < 1000; i++)
        {
            entry.AddFieldText($"field{i}", "value");
        }

        Assert.Equal(32, entry.FieldCount);
        entry.NormalizeFields(128, 8, new ClefLogFormatter(), null);
        Assert.Equal(8, entry.FieldCount);
    }

    [Fact]
    public void Large_records_resolve_collisions_by_the_same_precedence()
    {
        // Enough fields for the hashed path, with names shared across every source.
        var fields = new List<(string Name, LogFieldSource Source)>();
        for (var i = 0; i < 30; i++)
        {
            fields.Add(($"n{i % 12}", LogFieldSource.Hole));
        }

        for (var i = 0; i < 20; i++)
        {
            fields.Add(($"n{(i % 16) + 6}", LogFieldSource.Scope));
        }

        for (var i = 0; i < 10; i++)
        {
            fields.Add(($"n{i + 14}", LogFieldSource.Enricher));
        }

        fields.Add(("n0", LogFieldSource.Static));
        fields.Add(("n30", LogFieldSource.Static));

        var entry = new LogEntry();
        entry.ApplyCaps(new HostLoomLoggerOptions { MaxFieldsPerRecord = 64 });
        for (var i = 0; i < fields.Count; i++)
        {
            entry.AddFieldText(
                fields[i].Name,
                i.ToString(System.Globalization.CultureInfo.InvariantCulture),
                fields[i].Source
            );
        }

        entry.NormalizeFields(128, 64, new ClefLogFormatter(), null);

        // Reference: per name, the lowest source rank wins, and the last occurrence within it.
        var expected = fields
            .Select((field, index) => (field.Name, field.Source, Index: index))
            .GroupBy(field => field.Name)
            .ToDictionary(
                group => group.Key,
                group =>
                    group
                        .OrderBy(field => field.Source)
                        .ThenByDescending(field => field.Index)
                        .First()
                        .Index
            );
        var actual = new Dictionary<string, int>();
        for (var i = 0; i < entry.FieldCount; i++)
        {
            entry.GetField(i, out var name, out var value, out _);
            actual.Add(
                Encoding.UTF8.GetString(name),
                int.Parse(
                    Encoding.UTF8.GetString(value),
                    System.Globalization.CultureInfo.InvariantCulture
                )
            );
        }

        Assert.Equal(expected.OrderBy(pair => pair.Key), actual.OrderBy(pair => pair.Key));
    }

    [Fact]
    public void A_pooled_entry_does_not_keep_an_oversized_field_table()
    {
        var entry = new LogEntry();
        for (var i = 0; i < 200; i++)
        {
            entry.AddFieldText($"field{i}", "value");
        }

        entry.Reset();
        entry.TrimIfOversized();

        Assert.True(entry.FieldCapacity <= 64, $"capacity is {entry.FieldCapacity}");
    }

    [Fact]
    public async Task One_formatter_shared_by_two_providers_writes_valid_lines()
    {
        var formatter = new ClefLogFormatter();
        // CA2000: sink ownership transfers to the providers.
#pragma warning disable CA2000
        var first = new CollectingSink();
        var second = new CollectingSink();
#pragma warning restore CA2000
        var options = new HostLoomLoggerOptions
        {
            AttachMachineName = false,
            QueueFullPolicy = QueueFullPolicy.Block,
        };
        await using var one = new HostLoomLoggerProvider(formatter, first, options);
        await using var two = new HostLoomLoggerProvider(formatter, second, options);

        var loggers = new[] { one.CreateLogger("One"), two.CreateLogger("Two") };
        await Task.WhenAll(
            loggers.Select(logger =>
                Task.Run(
                    () =>
                    {
                        for (var i = 0; i < 2000; i++)
                        {
                            logger.LogInformation(
                                "record {Index} of {Total} with {Payload}",
                                i,
                                2000,
                                "some text value"
                            );
                        }
                    },
                    TestContext.Current.CancellationToken
                )
            )
        );
        await one.DisposeAsync();
        await two.DisposeAsync();

        foreach (var sink in new[] { first, second })
        {
            var lines = sink.Lines();
            Assert.Equal(2000, lines.Length);
            foreach (var line in lines)
            {
                using var document = JsonDocument.Parse(line);
            }
        }
    }

    [Fact]
    public async Task Drops_while_disposing_are_counted_as_disposal_not_as_a_full_queue()
    {
        var reasons = new List<string>();
        var gate = new Lock();
        var meters = new HashSet<Meter>();
        var constructingThread = -1;
        using var listener = new MeterListener();
        // Other tests' providers publish the same instrument name; a provider's instruments are
        // published synchronously in its constructor, so the constructing thread identifies ours.
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (
                instrument.Name == "hostloom.logging.records.dropped"
                && Environment.CurrentManagedThreadId == Volatile.Read(ref constructingThread)
            )
            {
                lock (gate)
                {
                    meters.Add(instrument.Meter);
                }

                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>(
            (instrument, _, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == "reason" && tag.Value is string reason)
                    {
                        lock (gate)
                        {
                            if (meters.Contains(instrument.Meter))
                            {
                                reasons.Add(reason);
                            }
                        }
                    }
                }
            }
        );
        listener.Start();

        // A queue far larger than anything written, so no drop is ever a real full queue; the
        // loggers race the disposal that closes it.
        for (var round = 0; round < 200; round++)
        {
            // CA2000: sink ownership transfers to the provider.
#pragma warning disable CA2000
            var sink = new CollectingSink();
#pragma warning restore CA2000
            Volatile.Write(ref constructingThread, Environment.CurrentManagedThreadId);
            var provider = new HostLoomLoggerProvider(
                new ClefLogFormatter(),
                sink,
                new HostLoomLoggerOptions
                {
                    AttachMachineName = false,
                    QueueCapacity = 1_000_000,
                    QueueFullPolicy = QueueFullPolicy.DropNewest,
                }
            );
            Volatile.Write(ref constructingThread, -1);

            var logger = provider.CreateLogger("Race");
            using var stop = new CancellationTokenSource();
            var writers = Enumerable
                .Range(0, 4)
                .Select(_ =>
                    Task.Run(
                        () =>
                        {
                            for (var i = 0; i < 20_000 && !stop.IsCancellationRequested; i++)
                            {
                                logger.LogInformation("tick {Index}", i);
                            }
                        },
                        TestContext.Current.CancellationToken
                    )
                )
                .ToArray();
            await provider.DisposeAsync();
            await stop.CancelAsync();
            await Task.WhenAll(writers);
        }

        lock (gate)
        {
            Assert.Equal(200, meters.Count);
            Assert.DoesNotContain("queue_full", reasons);
        }
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
