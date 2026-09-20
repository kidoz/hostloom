using System.Threading.Channels;

namespace HostLoom.Leadership;

/// <summary>
/// How a <see cref="LeaderChannel{T}"/> buffers and reports. <see cref="Validate"/> reports every
/// violation with the option key it names, so a container-free composition fails the same way a
/// hosted one does.
/// </summary>
public sealed class LeaderChannelOptions
{
    /// <summary>How many items the channel holds before <see cref="FullMode"/> applies.</summary>
    public int Capacity { get; set; } = 10_000;

    /// <summary>
    /// What a leader's write does when the channel is full. The default drops the oldest item so
    /// a slow reader sees the newest state; <see cref="BoundedChannelFullMode.Wait"/> makes
    /// <c>WriteAsync</c> wait for room instead.
    /// </summary>
    public BoundedChannelFullMode FullMode { get; set; } = BoundedChannelFullMode.DropOldest;

    /// <summary>
    /// How often dropped items are summarised in the log. The first drop after a quiet period is
    /// logged at once; further drops within the interval are counted and reported together.
    /// </summary>
    public TimeSpan DropReportInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Whether the items still buffered when leadership ends are discarded. Off by default: the
    /// buffer is kept, and the reader decides what to do with leader-era items by checking
    /// <see cref="ILeadership.LeadershipToken"/> around each side effect. On, a loss or
    /// resignation drains the buffer at once; the discarded items are counted on the meter with
    /// reason <c>loss</c>, reported on <see cref="LeaderChannel{T}.LossDrops"/>, and summarised in
    /// the log. Items a leader wrote in the same instant the loss was observed can still reach
    /// the reader, so the token check on the reader remains the guarantee.
    /// </summary>
    public bool DrainOnLoss { get; set; }

    /// <summary>Whether exactly one reader consumes the channel; lets the channel skip some synchronisation.</summary>
    public bool SingleReader { get; set; }

    /// <summary>Whether exactly one writer produces into the channel; lets the channel skip some synchronisation.</summary>
    public bool SingleWriter { get; set; }

    /// <summary>Every violation, each naming the option key at fault. Empty when the options are usable.</summary>
    public IReadOnlyList<string> Validate()
    {
        List<string> problems = [];
        if (Capacity <= 0)
        {
            problems.Add("LeaderChannel:Capacity must be positive.");
        }

        if (DropReportInterval <= TimeSpan.Zero)
        {
            problems.Add("LeaderChannel:DropReportInterval must be positive.");
        }

        return problems;
    }
}
