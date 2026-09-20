using System.Diagnostics.Metrics;
using HostLoom.Leadership;
using HostLoom.Leadership.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// What a leader channel does with the items still buffered when leadership ends: kept by
/// default for the reader to judge against the leadership token, discarded and counted when
/// <see cref="LeaderChannelOptions.DrainOnLoss"/> is on.
/// </summary>
public sealed class LeaderChannelDrainTests
{
    [Fact]
    public async Task By_default_the_buffer_survives_a_loss_and_the_reader_decides()
    {
        using var leadership = new ManualLeadership("reconciler");
        using var channel = new LeaderChannel<string>("inventory-drain-default", leadership);
        leadership.Acquire();
        Assert.True(channel.Writer.TryWrite("eu"));
        Assert.True(channel.Writer.TryWrite("us"));

        leadership.Lose();

        Assert.False(channel.Options.DrainOnLoss);
        Assert.Equal(2, channel.Reader.Count);
        Assert.Equal(0, channel.LossDrops);
        Assert.Equal("eu", await channel.Reader.ReadAsync(TestContext.Current.CancellationToken));
        // The reader's cue that the item is from a term that ended.
        Assert.True(leadership.LeadershipToken.IsCancellationRequested);
    }

    [Fact]
    public async Task DrainOnLoss_discards_the_buffer_when_leadership_ends_and_reports_the_count()
    {
        var clock = new TestClock();
        var logger = new RecordingLogger<LeaderChannel<string>>();
        using var recorder = new LossRecorder("inventory-drain");
        using var leadership = new ManualLeadership("reconciler");
        using var channel = new LeaderChannel<string>(
            "inventory-drain",
            leadership,
            new LeaderChannelOptions { DrainOnLoss = true, Capacity = 8 },
            logger,
            clock
        );

        leadership.Acquire();
        Assert.True(channel.Writer.TryWrite("eu"));
        Assert.True(channel.Writer.TryWrite("us"));
        Assert.True(channel.Writer.TryWrite("apac"));
        Assert.Equal(3, channel.Reader.Count);

        leadership.Lose();

        Assert.Equal(0, channel.Reader.Count);
        Assert.Equal(3, channel.LossDrops);
        Assert.Equal(0, channel.FollowerDrops);
        Assert.Equal(0, channel.CapacityDrops);
        var report = Assert.Single(
            logger.Entries,
            entry => entry.Event.Id == LeadershipEvents.ChannelLossDrained.Id
        );
        Assert.Equal(LogLevel.Information, report.Level);
        Assert.Contains("3 items", report.Message, StringComparison.Ordinal);
        Assert.Contains("inventory-drain", report.Message, StringComparison.Ordinal);
        Assert.Contains("'reconciler'", report.Message, StringComparison.Ordinal);

        // A follower write is discarded as before, a new term buffers again, and a resignation
        // drains too.
        Assert.True(channel.Writer.TryWrite("eu"));
        Assert.Equal(1, channel.FollowerDrops);
        leadership.Acquire();
        Assert.True(channel.Writer.TryWrite("us"));
        Assert.Equal(1, channel.Reader.Count);
        leadership.Resign();
        Assert.Equal(0, channel.Reader.Count);
        Assert.Equal(4, channel.LossDrops);
        Assert.Equal(
            2,
            logger.Entries.Count(entry => entry.Event.Id == LeadershipEvents.ChannelLossDrained.Id)
        );
        Assert.Equal(4, recorder.Total);

        // Ending a term with an empty buffer reports nothing, and the reader still receives what
        // the next term writes.
        leadership.Acquire();
        leadership.Lose();
        Assert.Equal(4, channel.LossDrops);
        Assert.Equal(
            2,
            logger.Entries.Count(entry => entry.Event.Id == LeadershipEvents.ChannelLossDrained.Id)
        );
        leadership.Acquire();
        Assert.True(channel.Writer.TryWrite("apac"));
        Assert.Equal("apac", await channel.Reader.ReadAsync(TestContext.Current.CancellationToken));
    }

    /// <summary>Sums <c>hostloom.leader.channel.dropped</c> with reason <c>loss</c> for one channel name.</summary>
    private sealed class LossRecorder : IDisposable
    {
        private readonly MeterListener _listener = new();
        private long _total;

        public LossRecorder(string channel)
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (
                    instrument.Meter.Name == LeadershipDiagnostics.MeterName
                    && instrument.Name == "hostloom.leader.channel.dropped"
                )
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>(
                (_, value, tags, _) =>
                {
                    var loss = false;
                    var mine = false;
                    foreach (var tag in tags)
                    {
                        if (
                            tag.Key == LeadershipDiagnostics.DropReasonTag
                            && tag.Value is LeadershipDiagnostics.LossDrop
                        )
                        {
                            loss = true;
                        }
                        else if (
                            tag.Key == LeadershipDiagnostics.ChannelTag
                            && tag.Value is string name
                            && name == channel
                        )
                        {
                            mine = true;
                        }
                    }

                    if (loss && mine)
                    {
                        Interlocked.Add(ref _total, value);
                    }
                }
            );
            _listener.Start();
        }

        public long Total => Interlocked.Read(ref _total);

        public void Dispose() => _listener.Dispose();
    }
}
