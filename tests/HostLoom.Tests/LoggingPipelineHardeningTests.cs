using System.Diagnostics.Metrics;
using System.Text;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

// CA1873: the boxing standard ILogger path is what these tests exercise on purpose.
#pragma warning disable CA1873

namespace HostLoom.Tests;

/// <summary>
/// Drop attribution while a provider is being disposed.
/// </summary>
public sealed class LoggingPipelineHardeningTests
{
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
