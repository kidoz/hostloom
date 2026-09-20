using HostLoom.Pipelines;

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

    /// <summary>
    /// Publish attempts a message gets before it is dead-lettered and no longer claimed. The
    /// first attempt counts, so the default allows nine retries.
    /// </summary>
    public int MaxAttempts { get; set; } = 10;

    /// <summary>Delay before the first retry of a failed message; later retries grow by <see cref="RetryBackoffFactor"/>.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Upper bound on the delay between two attempts of one message.</summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Multiplier applied to the retry delay per failed attempt. One disables the growth.</summary>
    public double RetryBackoffFactor { get; set; } = 2;

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

        if (MaxAttempts <= 0)
        {
            problems.Add("Outbox:MaxAttempts must be positive.");
        }

        if (RetryDelay < TimeSpan.Zero)
        {
            problems.Add("Outbox:RetryDelay must not be negative.");
        }

        if (MaxRetryDelay < RetryDelay)
        {
            problems.Add("Outbox:MaxRetryDelay must not be shorter than Outbox:RetryDelay.");
        }

        if (double.IsNaN(RetryBackoffFactor) || RetryBackoffFactor < 1)
        {
            problems.Add("Outbox:RetryBackoffFactor must be at least 1.");
        }

        return problems;
    }

    /// <summary>
    /// The delay before <paramref name="attempt"/> (counted from 1 for the first retry), grown by
    /// <see cref="RetryBackoffFactor"/> from <see cref="RetryDelay"/> and clamped to
    /// <see cref="MaxRetryDelay"/>. Same arithmetic as <see cref="RetryPolicy.Exponential"/>.
    /// </summary>
    internal TimeSpan GetRetryDelay(int attempt) =>
        RetryPolicy
            .Exponential(MaxAttempts, RetryDelay, MaxRetryDelay, RetryBackoffFactor)
            .GetDelay(attempt);
}
