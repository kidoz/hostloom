using System.Diagnostics.Metrics;
using System.Threading.Channels;
using HostLoom.Leadership;
using HostLoom.Leadership.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// A scripted <see cref="ILeadership"/> stands in for the elector: the channel only reads
/// <c>IsLeader</c> and follows <c>OnChange</c>, so the gate is proven without a lock backend.
/// </summary>
public sealed class LeaderChannelTests
{
    private static readonly TimeSpan Interval = TimeSpan.FromMinutes(1);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Losing_leadership_unblocks_a_full_channel_without_a_reader(bool write)
    {
        using var leadership = new ManualLeadership("catalog");
        using var channel = new LeaderChannel<int>(
            "catalog",
            leadership,
            new() { Capacity = 1, FullMode = BoundedChannelFullMode.Wait }
        );
        leadership.Acquire();
        Assert.True(channel.Writer.TryWrite(1));
        var waiting = write
            ? channel.Writer.WriteAsync(2, TestContext.Current.CancellationToken).AsTask()
            : channel.Writer.WaitToWriteAsync(TestContext.Current.CancellationToken).AsTask();
        Assert.False(waiting.IsCompleted);
        leadership.Lose();
        await waiting.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(write ? 1 : 0, channel.FollowerDrops);
        Assert.Equal(1, channel.Reader.Count);
        Assert.True(await channel.Writer.WaitToWriteAsync(TestContext.Current.CancellationToken));
        Assert.True(channel.Writer.TryComplete());
        Assert.False(await channel.Writer.WaitToWriteAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Cancelling_a_capacity_wait_does_not_break_the_next_term()
    {
        using var leadership = new ManualLeadership("catalog");
        using var channel = new LeaderChannel<int>(
            "catalog",
            leadership,
            new() { Capacity = 1, FullMode = BoundedChannelFullMode.Wait }
        );
        leadership.Acquire();
        channel.Writer.TryWrite(1);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        var waiting = channel.Writer.WaitToWriteAsync(cancellation.Token).AsTask();
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        leadership.Lose();
        leadership.Acquire();
        channel.Reader.TryRead(out _);
        Assert.True(await channel.Writer.WaitToWriteAsync(TestContext.Current.CancellationToken));
        Assert.True(channel.Writer.TryWrite(2));
    }

    [Fact]
    public async Task A_follower_write_is_accepted_and_discarded_and_a_leader_write_is_buffered()
    {
        using var leadership = new ManualLeadership("reconciler");
        using var channel = new LeaderChannel<string>("inventory-changes", leadership);

        Assert.True(channel.Writer.TryWrite("eu"));
        await channel.Writer.WriteAsync("us", TestContext.Current.CancellationToken);

        Assert.Equal(0, channel.Reader.Count);
        Assert.Equal(2, channel.FollowerDrops);
        Assert.Equal(0, channel.CapacityDrops);

        leadership.Acquire();
        Assert.True(channel.Writer.TryWrite("eu"));
        await channel.Writer.WriteAsync("us", TestContext.Current.CancellationToken);

        Assert.Equal(2, channel.Reader.Count);
        Assert.Equal("eu", await channel.Reader.ReadAsync(TestContext.Current.CancellationToken));
        Assert.Equal("us", await channel.Reader.ReadAsync(TestContext.Current.CancellationToken));
        Assert.Equal(2, channel.FollowerDrops);

        leadership.Lose();
        Assert.True(channel.Writer.TryWrite("eu"));
        Assert.Equal(0, channel.Reader.Count);
        Assert.Equal(3, channel.FollowerDrops);
    }

    [Fact]
    public async Task A_full_channel_drops_the_oldest_item_and_counts_it_separately()
    {
        using var leadership = new ManualLeadership("reconciler");
        leadership.Acquire();
        using var channel = new LeaderChannel<int>(
            "inventory-changes",
            leadership,
            new LeaderChannelOptions { Capacity = 2 }
        );

        Assert.True(channel.Writer.TryWrite(1));
        Assert.True(channel.Writer.TryWrite(2));
        Assert.True(channel.Writer.TryWrite(3));

        Assert.Equal(2, await channel.Reader.ReadAsync(TestContext.Current.CancellationToken));
        Assert.Equal(3, await channel.Reader.ReadAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1, channel.CapacityDrops);
        Assert.Equal(0, channel.FollowerDrops);
    }

    [Fact]
    public async Task Wait_mode_makes_the_leader_wait_for_room_and_a_follower_never_waits()
    {
        using var leadership = new ManualLeadership("reconciler");
        leadership.Acquire();
        using var channel = new LeaderChannel<int>(
            "inventory-changes",
            leadership,
            new LeaderChannelOptions { Capacity = 1, FullMode = BoundedChannelFullMode.Wait }
        );

        Assert.True(channel.Writer.TryWrite(1));
        Assert.False(channel.Writer.TryWrite(2));
        var waiting = channel.Writer.WriteAsync(2, TestContext.Current.CancellationToken);
        Assert.False(waiting.IsCompleted);

        Assert.Equal(1, await channel.Reader.ReadAsync(TestContext.Current.CancellationToken));
        await waiting;
        Assert.Equal(2, await channel.Reader.ReadAsync(TestContext.Current.CancellationToken));

        leadership.Lose();
        var discarded = channel.Writer.WriteAsync(3, TestContext.Current.CancellationToken);
        Assert.True(discarded.IsCompletedSuccessfully);
        Assert.Equal(0, channel.CapacityDrops);
        Assert.Equal(1, channel.FollowerDrops);
    }

    [Fact]
    public void Drops_are_summarised_at_once_then_once_per_interval_and_when_leadership_arrives()
    {
        var clock = new TestClock();
        var logger = new RecordingLogger<LeaderChannel<int>>();
        using var leadership = new ManualLeadership("reconciler");
        using var channel = new LeaderChannel<int>(
            "inventory-changes",
            leadership,
            new LeaderChannelOptions { DropReportInterval = Interval },
            logger,
            clock
        );

        // The first drop is reported immediately so an operator sees discarding start.
        channel.Writer.TryWrite(1);
        var first = Assert.Single(logger.Entries);
        Assert.Equal(LeadershipEvents.ChannelFollowerDropped.Id, first.Event.Id);
        Assert.Equal(LogLevel.Information, first.Level);
        Assert.Contains("discarded 1 items", first.Message, StringComparison.Ordinal);

        // Further drops inside the interval are counted, not logged.
        clock.Advance(Interval - TimeSpan.FromSeconds(1));
        channel.Writer.TryWrite(2);
        channel.Writer.TryWrite(3);
        Assert.Single(logger.Entries);

        // The next drop after the interval reports everything counted since the last line.
        clock.Advance(TimeSpan.FromSeconds(1));
        channel.Writer.TryWrite(4);
        Assert.Equal(2, logger.Entries.Count);
        Assert.Contains("discarded 3 items", logger.Entries[1].Message, StringComparison.Ordinal);

        // Becoming leader flushes the tail so the log shows what the follower missed.
        channel.Writer.TryWrite(5);
        leadership.Acquire();
        Assert.Equal(3, logger.Entries.Count);
        Assert.Contains("discarded 1 items", logger.Entries[2].Message, StringComparison.Ordinal);
        Assert.Equal(5, channel.FollowerDrops);

        // Nothing pending: acquiring again writes no empty summary.
        leadership.Lose();
        leadership.Acquire();
        Assert.Equal(3, logger.Entries.Count);
    }

    [Fact]
    public void A_full_channel_is_reported_as_a_warning_and_disposal_flushes_the_tail()
    {
        var clock = new TestClock();
        var logger = new RecordingLogger<LeaderChannel<int>>();
        using var leadership = new ManualLeadership("reconciler");
        leadership.Acquire();
        var channel = new LeaderChannel<int>(
            "inventory-changes",
            leadership,
            new LeaderChannelOptions { Capacity = 1, DropReportInterval = Interval },
            logger,
            clock
        );

        channel.Writer.TryWrite(1);
        channel.Writer.TryWrite(2);
        var first = Assert.Single(logger.Entries);
        Assert.Equal(LeadershipEvents.ChannelFull.Id, first.Event.Id);
        Assert.Equal(LogLevel.Warning, first.Level);

        channel.Writer.TryWrite(3);
        channel.Writer.TryWrite(4);
        Assert.Single(logger.Entries);

        channel.Dispose();
        Assert.Equal(2, logger.Entries.Count);
        Assert.Contains("dropped 2 items", logger.Entries[1].Message, StringComparison.Ordinal);
        Assert.Equal(3, channel.CapacityDrops);
    }

    [Fact]
    public void Drops_are_counted_on_the_leadership_meter_by_reason()
    {
        using var recorder = new DropRecorder("inventory-changes");
        using var leadership = new ManualLeadership("reconciler");
        using var channel = new LeaderChannel<int>(
            "inventory-changes",
            leadership,
            new LeaderChannelOptions { Capacity = 1 }
        );

        channel.Writer.TryWrite(1);
        channel.Writer.TryWrite(2);
        leadership.Acquire();
        channel.Writer.TryWrite(3);
        channel.Writer.TryWrite(4);

        Assert.Equal(2, recorder.Total(LeadershipDiagnostics.FollowerDrop));
        Assert.Equal(1, recorder.Total(LeadershipDiagnostics.FullDrop));
        Assert.All(recorder.Roles, role => Assert.Equal("reconciler", role));
    }

    [Fact]
    public async Task A_completed_channel_refuses_follower_writes_too()
    {
        using var leadership = new ManualLeadership("reconciler");
        using var channel = new LeaderChannel<int>("inventory-changes", leadership);

        Assert.True(channel.Writer.TryComplete());
        Assert.False(channel.Writer.TryComplete());

        Assert.False(channel.Writer.TryWrite(1));
        await Assert.ThrowsAsync<ChannelClosedException>(async () =>
            await channel.Writer.WriteAsync(1, TestContext.Current.CancellationToken)
        );
        Assert.Equal(0, channel.FollowerDrops);
        await channel.Reader.Completion.WaitAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public void Options_are_validated_by_name()
    {
        using var leadership = new ManualLeadership("reconciler");
        var options = new LeaderChannelOptions { Capacity = 0, DropReportInterval = TimeSpan.Zero };

        Assert.Equal(
            [
                "LeaderChannel:Capacity must be positive.",
                "LeaderChannel:DropReportInterval must be positive.",
            ],
            options.Validate()
        );
        var error = Assert.Throws<ArgumentException>(() =>
            new LeaderChannel<int>("inventory-changes", leadership, options)
        );
        Assert.Contains("LeaderChannel:Capacity", error.Message, StringComparison.Ordinal);
        Assert.Throws<ArgumentException>(() => new LeaderChannel<int>(" ", leadership));
    }

    private sealed class DropRecorder : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly Lock _gate = new();
        private readonly List<(string Reason, string Role, long Value)> _drops = [];

        public DropRecorder(string channel)
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
                    string? reason = null;
                    string? role = null;
                    string? name = null;
                    foreach (var tag in tags)
                    {
                        switch (tag.Key)
                        {
                            case LeadershipDiagnostics.DropReasonTag:
                                reason = tag.Value as string;
                                break;
                            case LeadershipDiagnostics.RoleTag:
                                role = tag.Value as string;
                                break;
                            case LeadershipDiagnostics.ChannelTag:
                                name = tag.Value as string;
                                break;
                        }
                    }

                    if (name == channel && reason is not null && role is not null)
                    {
                        lock (_gate)
                        {
                            _drops.Add((reason, role, value));
                        }
                    }
                }
            );
            _listener.Start();
        }

        public IReadOnlyList<string> Roles
        {
            get
            {
                lock (_gate)
                {
                    return [.. _drops.Select(drop => drop.Role)];
                }
            }
        }

        public long Total(string reason)
        {
            lock (_gate)
            {
                return _drops.Where(drop => drop.Reason == reason).Sum(drop => drop.Value);
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
