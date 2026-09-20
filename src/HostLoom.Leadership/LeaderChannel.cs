using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HostLoom.Leadership;

/// <summary>
/// A bounded channel whose writes count only while this instance leads a role. Producers on every
/// instance run the same code: a leader's write is buffered for the reader, a follower's write is
/// accepted and discarded. That keeps a warm standby's state current without letting it act.
/// Drops are counted on the <c>HostLoom.Leadership</c> meter by reason and summarised in the
/// log at most once per <see cref="LeaderChannelOptions.DropReportInterval"/>; the summary is
/// also written when this instance becomes leader and when the channel is disposed, so nothing
/// a follower discarded goes unreported. Composes without a container:
/// <c>new LeaderChannel&lt;T&gt;("priority-changes", leadership)</c>.
/// </summary>
/// <remarks>
/// Admission is gated, delivery is not: an item the leader buffered stays readable after
/// leadership ends, because the channel cannot know whether the reader has already acted on it.
/// The reader owns that decision. A reader that must act only while leading checks
/// <see cref="ILeadership.LeadershipToken"/> around each side effect, or runs its loop on a token
/// linked to it, and treats an item read under a cancelled token as stale. Set
/// <see cref="LeaderChannelOptions.DrainOnLoss"/> to have the channel discard the buffer when
/// leadership ends; that narrows the window but does not close it, since a write and a loss can
/// land in the same instant, so the token check on the reader remains the guarantee.
/// </remarks>
/// <typeparam name="T">The item type.</typeparam>
public sealed class LeaderChannel<T> : Channel<T>, IDisposable
{
    private const long Never = long.MinValue;

    private readonly ILeadership _leadership;
    private readonly ILogger _logger;
    private readonly TimeProvider _clock;
    private readonly IDisposable _subscription;
    private readonly ChannelReader<T> _inner;
    private readonly KeyValuePair<string, object?>[] _followerTags;
    private readonly KeyValuePair<string, object?>[] _fullTags;
    private readonly KeyValuePair<string, object?>[] _lossTags;
    private long _followerDrops;
    private long _capacityDrops;
    private long _lossDrops;
    private long _pendingFollowerDrops;
    private long _pendingCapacityDrops;
    private long _pendingLossDrops;
    private long _lastReport = Never;
    private volatile bool _completed;

    /// <summary>Composes a channel gated on <paramref name="leadership"/>.</summary>
    /// <param name="name">Names the channel in metrics and logs.</param>
    /// <param name="leadership">The role whose leader may write.</param>
    /// <param name="options">Capacity, full mode, and the drop report interval; defaults when <see langword="null"/>.</param>
    /// <param name="logger">Receives the drop summaries; silent when <see langword="null"/>.</param>
    /// <param name="clock">Paces the drop summaries; the system clock when <see langword="null"/>.</param>
    /// <exception cref="ArgumentException"><paramref name="options"/> fail <see cref="LeaderChannelOptions.Validate"/>.</exception>
    public LeaderChannel(
        string name,
        ILeadership leadership,
        LeaderChannelOptions? options = null,
        ILogger? logger = null,
        TimeProvider? clock = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(leadership);
        options ??= new LeaderChannelOptions();
        var problems = options.Validate();
        if (problems.Count > 0)
        {
            throw new ArgumentException(
                "LeaderChannelOptions are not usable: " + string.Join(" ", problems),
                nameof(options)
            );
        }

        Name = name;
        Options = options;
        _leadership = leadership;
        _logger = logger ?? NullLogger.Instance;
        _clock = clock ?? TimeProvider.System;
        _followerTags = Tags(LeadershipDiagnostics.FollowerDrop);
        _fullTags = Tags(LeadershipDiagnostics.FullDrop);
        _lossTags = Tags(LeadershipDiagnostics.LossDrop);

        var inner = Channel.CreateBounded<T>(
            new BoundedChannelOptions(options.Capacity)
            {
                FullMode = options.FullMode,
                SingleReader = options.SingleReader,
                SingleWriter = options.SingleWriter,
            },
            _ => Dropped(follower: false)
        );
        _inner = inner.Reader;
        Reader = inner.Reader;
        Writer = new GatedWriter(this, inner.Writer);
        _subscription = leadership.OnChange(change =>
        {
            if (change.IsLeader)
            {
                Flush();
            }
            else if (Options.DrainOnLoss)
            {
                Drain();
            }
        });
    }

    /// <summary>The channel's name, as tagged on metrics and named in logs.</summary>
    public string Name { get; }

    /// <summary>The role whose leader may write.</summary>
    public string Role => _leadership.Role;

    /// <summary>The options in force.</summary>
    public LeaderChannelOptions Options { get; }

    /// <summary>Items discarded because this instance was not leading when they were written.</summary>
    public long FollowerDrops => Interlocked.Read(ref _followerDrops);

    /// <summary>Items the full channel discarded under <see cref="LeaderChannelOptions.FullMode"/>.</summary>
    public long CapacityDrops => Interlocked.Read(ref _capacityDrops);

    /// <summary>Items still buffered when leadership ended and discarded under <see cref="LeaderChannelOptions.DrainOnLoss"/>.</summary>
    public long LossDrops => Interlocked.Read(ref _lossDrops);

    /// <summary>Stops following leadership changes and writes the pending drop summary. Leaves the channel itself as it is.</summary>
    public void Dispose()
    {
        _subscription.Dispose();
        Flush();
    }

    private KeyValuePair<string, object?>[] Tags(string reason) =>
        [
            new(LeadershipDiagnostics.RoleTag, _leadership.Role),
            new(LeadershipDiagnostics.ChannelTag, Name),
            new(LeadershipDiagnostics.DropReasonTag, reason),
        ];

    private void Dropped(bool follower)
    {
        if (follower)
        {
            Interlocked.Increment(ref _followerDrops);
            Interlocked.Increment(ref _pendingFollowerDrops);
            LeadershipDiagnostics.ChannelDropped.Add(1, _followerTags);
        }
        else
        {
            Interlocked.Increment(ref _capacityDrops);
            Interlocked.Increment(ref _pendingCapacityDrops);
            LeadershipDiagnostics.ChannelDropped.Add(1, _fullTags);
        }

        var last = Interlocked.Read(ref _lastReport);
        if (last != Never && _clock.GetElapsedTime(last) < Options.DropReportInterval)
        {
            return;
        }

        var now = _clock.GetTimestamp();
        if (Interlocked.CompareExchange(ref _lastReport, now, last) == last)
        {
            Report();
        }
    }

    /// <summary>Discards what the leader left in the buffer; a loss is rare, so the summary is written at once.</summary>
    private void Drain()
    {
        var drained = 0L;
        while (_inner.TryRead(out _))
        {
            drained++;
        }

        if (drained == 0)
        {
            return;
        }

        Interlocked.Add(ref _lossDrops, drained);
        Interlocked.Add(ref _pendingLossDrops, drained);
        LeadershipDiagnostics.ChannelDropped.Add(drained, _lossTags);
        Flush();
    }

    private void Flush()
    {
        Interlocked.Exchange(ref _lastReport, _clock.GetTimestamp());
        Report();
    }

    private void Report()
    {
        var follower = Interlocked.Exchange(ref _pendingFollowerDrops, 0);
        if (follower > 0 && _logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation(
                LeadershipEvents.ChannelFollowerDropped,
                "Channel '{Channel}' discarded {Count} items written while this instance did not lead role '{Role}'.",
                Name,
                follower,
                Role
            );
        }

        var full = Interlocked.Exchange(ref _pendingCapacityDrops, 0);
        if (full > 0)
        {
            _logger.LogWarning(
                LeadershipEvents.ChannelFull,
                "Channel '{Channel}' for role '{Role}' was full and dropped {Count} items; the reader is slower than the leader's writes.",
                Name,
                Role,
                full
            );
        }

        var loss = Interlocked.Exchange(ref _pendingLossDrops, 0);
        if (loss > 0 && _logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation(
                LeadershipEvents.ChannelLossDrained,
                "Channel '{Channel}' discarded {Count} items still buffered when this instance stopped leading role '{Role}' (LeaderChannel:DrainOnLoss).",
                Name,
                loss,
                Role
            );
        }
    }

    /// <summary>The leader's writes reach the buffer; a follower's are accepted and discarded.</summary>
    private sealed class GatedWriter(LeaderChannel<T> owner, ChannelWriter<T> inner)
        : ChannelWriter<T>
    {
        public override bool TryWrite(T item)
        {
            if (owner._leadership.IsLeader)
            {
                return inner.TryWrite(item);
            }

            if (owner._completed)
            {
                return false;
            }

            owner.Dropped(follower: true);
            return true;
        }

        // The base writer retries through this writer's admission gate after a capacity wait.
        public override ValueTask WriteAsync(
            T item,
            CancellationToken cancellationToken = default
        ) => base.WriteAsync(item, cancellationToken);

        public override async ValueTask<bool> WaitToWriteAsync(
            CancellationToken cancellationToken = default
        )
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (owner._completed)
                    return await inner.WaitToWriteAsync(cancellationToken).ConfigureAwait(false);
                if (!owner._leadership.IsLeader)
                    return true;

                var lost = owner._leadership.LeadershipToken;
                using var waiting = CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken,
                    lost
                );
                try
                {
                    return await inner.WaitToWriteAsync(waiting.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                    when (!cancellationToken.IsCancellationRequested && lost.IsCancellationRequested
                    )
                {
                    // Recheck both completion and the current role, including rapid reacquisition.
                }
            }
        }

        public override bool TryComplete(Exception? error = null)
        {
            if (!inner.TryComplete(error))
            {
                return false;
            }

            owner._completed = true;
            return true;
        }
    }
}
