namespace HostLoom;

/// <summary>How the outbox relay drains the store. <see cref="Validate"/> names the option key at fault.</summary>
public sealed class OutboxOptions
{
    /// <summary>How long the relay waits between drains when nothing wakes it earlier.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Messages claimed per store round trip.</summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// How long a claim keeps a message from other relays. A relay that dies inside the lease
    /// leaves the message to be claimed again when it expires.
    /// </summary>
    public TimeSpan ClaimLease { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>Every violation, each naming the option key at fault. Empty when the options are usable.</summary>
    public IReadOnlyList<string> Validate()
    {
        List<string> problems = [];
        if (PollInterval <= TimeSpan.Zero)
        {
            problems.Add("Outbox:PollInterval must be positive.");
        }

        if (BatchSize <= 0)
        {
            problems.Add("Outbox:BatchSize must be positive.");
        }

        if (ClaimLease <= TimeSpan.Zero)
        {
            problems.Add("Outbox:ClaimLease must be positive.");
        }

        return problems;
    }
}
