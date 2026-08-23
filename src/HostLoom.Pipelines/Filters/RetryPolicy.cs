namespace HostLoom.Pipelines;

/// <summary>
/// How many times a failed pipeline invocation is retried, and how long to wait between attempts.
/// Instances are immutable and safe to share across pipelines and threads.
/// </summary>
public sealed class RetryPolicy
{
    private readonly TimeSpan _minInterval;
    private readonly TimeSpan _maxInterval;
    private readonly double _factor;
    private readonly double _jitterRatio;

    private RetryPolicy(
        string description,
        int retryLimit,
        TimeSpan minInterval,
        TimeSpan maxInterval,
        double factor,
        double jitterRatio)
    {
        Description = description;
        RetryLimit = retryLimit;
        _minInterval = minInterval;
        _maxInterval = maxInterval;
        _factor = factor;
        _jitterRatio = jitterRatio;
    }

    /// <summary>Retries allowed after the first attempt. A limit of 2 permits up to three invocations.</summary>
    public int RetryLimit { get; }

    /// <summary>Short policy name, surfaced by pipeline probes.</summary>
    public string Description { get; }

    /// <summary>Retries without waiting.</summary>
    public static RetryPolicy Immediate(int retryLimit)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(retryLimit);
        return new RetryPolicy("immediate", retryLimit, TimeSpan.Zero, TimeSpan.Zero, 1, 0);
    }

    /// <summary>Waits the same interval before every retry.</summary>
    public static RetryPolicy Interval(int retryLimit, TimeSpan interval)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(retryLimit);
        ArgumentOutOfRangeException.ThrowIfLessThan(interval, TimeSpan.Zero);
        return new RetryPolicy("interval", retryLimit, interval, interval, 1, 0);
    }

    /// <summary>Multiplies the wait by <paramref name="factor"/> per retry, never exceeding <paramref name="maxInterval"/>.</summary>
    public static RetryPolicy Exponential(
        int retryLimit,
        TimeSpan minInterval,
        TimeSpan maxInterval,
        double factor = 2)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(retryLimit);
        ArgumentOutOfRangeException.ThrowIfLessThan(minInterval, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxInterval, minInterval);
        ArgumentOutOfRangeException.ThrowIfLessThan(factor, 1);
        return new RetryPolicy("exponential", retryLimit, minInterval, maxInterval, factor, 0);
    }

    /// <summary>
    /// Spreads each delay by plus or minus <paramref name="ratio"/> so callers that failed together do
    /// not retry in lockstep. A ratio of 0.2 varies a one second delay between 0.8s and 1.2s.
    /// </summary>
    public RetryPolicy WithJitter(double ratio)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(ratio);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(ratio, 1);
        return new RetryPolicy(Description, RetryLimit, _minInterval, _maxInterval, _factor, ratio);
    }

    /// <summary>Delay preceding <paramref name="attempt"/>, counted from 1 for the first retry.</summary>
    public TimeSpan GetDelay(int attempt)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);
        if (_minInterval <= TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }

        // Computed in double ticks and clamped, because a large factor raised to a large attempt
        // overflows TimeSpan multiplication long before the clamp would apply.
        var ticks = Math.Min(_minInterval.Ticks * Math.Pow(_factor, attempt - 1), _maxInterval.Ticks);
        var delay = TimeSpan.FromTicks((long)ticks);
        if (_jitterRatio == 0)
        {
            return delay;
        }

        // CA5394: jitter exists to decorrelate retry timing between callers, not to resist an
        // attacker. A cryptographic generator would cost more and buy nothing here.
#pragma warning disable CA5394
        var offset = ((Random.Shared.NextDouble() * 2) - 1) * _jitterRatio;
#pragma warning restore CA5394
        var jittered = delay * (1 + offset);
        return jittered < TimeSpan.Zero ? TimeSpan.Zero : jittered;
    }
}
