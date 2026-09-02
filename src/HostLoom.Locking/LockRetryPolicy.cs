using System.Globalization;

namespace HostLoom.Locking;

/// <summary>
/// How many times a contended acquisition is retried and how long to wait between attempts.
/// Instances are immutable and safe to share. The shape mirrors the pipelines' retry policy
/// without depending on it, so <c>HostLoom.Locking</c> stays a leaf.
/// </summary>
public sealed class LockRetryPolicy
{
    private readonly Shape _shape;
    private readonly TimeSpan _interval;
    private readonly TimeSpan _maxInterval;
    private readonly double _factor;

    private LockRetryPolicy(
        Shape shape,
        int retryLimit,
        TimeSpan interval,
        TimeSpan maxInterval,
        double factor,
        TimeSpan jitter
    )
    {
        _shape = shape;
        RetryLimit = retryLimit;
        _interval = interval;
        _maxInterval = maxInterval;
        _factor = factor;
        Jitter = jitter;

        var total = TimeSpan.Zero;
        for (var attempt = 1; attempt <= retryLimit; attempt++)
        {
            total += GetBaseDelay(attempt);
        }

        MaxTotalDelay = total + (jitter * retryLimit);
        Description = Describe();
    }

    private enum Shape
    {
        Immediate,
        Interval,
        Linear,
        Exponential,
    }

    /// <summary>Retries allowed after the first attempt. A limit of 2 permits up to three attempts.</summary>
    public int RetryLimit { get; }

    /// <summary>Additive jitter added to every delay, in <c>[0, Jitter]</c>.</summary>
    public TimeSpan Jitter { get; }

    /// <summary>
    /// The longest an acquisition can wait under this policy: the sum of every base delay plus
    /// the jitter bound per retry. Logged at startup as the derived maximum wait.
    /// </summary>
    public TimeSpan MaxTotalDelay { get; }

    /// <summary>Short policy description, surfaced by the probe and the startup log.</summary>
    public string Description { get; }

    /// <summary>Retries without waiting.</summary>
    public static LockRetryPolicy Immediate(int retryLimit)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(retryLimit);
        return new LockRetryPolicy(
            Shape.Immediate,
            retryLimit,
            TimeSpan.Zero,
            TimeSpan.Zero,
            1,
            TimeSpan.Zero
        );
    }

    /// <summary>Waits the same <paramref name="interval"/> before every retry.</summary>
    public static LockRetryPolicy Interval(int retryLimit, TimeSpan interval)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(retryLimit);
        ArgumentOutOfRangeException.ThrowIfLessThan(interval, TimeSpan.Zero);
        return new LockRetryPolicy(
            Shape.Interval,
            retryLimit,
            interval,
            interval,
            1,
            TimeSpan.Zero
        );
    }

    /// <summary>
    /// Waits <c>attempt × step</c> before retry <c>attempt</c>: 50 ms, then 100 ms, then 150 ms
    /// for a 50 ms step. This is the platform's historical shape.
    /// </summary>
    public static LockRetryPolicy Linear(int retryLimit, TimeSpan step)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(retryLimit);
        ArgumentOutOfRangeException.ThrowIfLessThan(step, TimeSpan.Zero);
        return new LockRetryPolicy(Shape.Linear, retryLimit, step, step, 1, TimeSpan.Zero);
    }

    /// <summary>Multiplies the wait by <paramref name="factor"/> per retry, never exceeding <paramref name="maxInterval"/>.</summary>
    public static LockRetryPolicy Exponential(
        int retryLimit,
        TimeSpan minInterval,
        TimeSpan maxInterval,
        double factor = 2
    )
    {
        ArgumentOutOfRangeException.ThrowIfNegative(retryLimit);
        ArgumentOutOfRangeException.ThrowIfLessThan(minInterval, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxInterval, minInterval);
        ArgumentOutOfRangeException.ThrowIfLessThan(factor, 1);
        return new LockRetryPolicy(
            Shape.Exponential,
            retryLimit,
            minInterval,
            maxInterval,
            factor,
            TimeSpan.Zero
        );
    }

    /// <summary>
    /// Adds a uniformly random delay in <c>[0, jitter]</c> to every wait so callers that collided
    /// do not retry in lockstep.
    /// </summary>
    public LockRetryPolicy WithJitter(TimeSpan jitter)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(jitter, TimeSpan.Zero);
        return new LockRetryPolicy(_shape, RetryLimit, _interval, _maxInterval, _factor, jitter);
    }

    /// <summary>Delay preceding retry <paramref name="attempt"/>, counted from 1, without jitter.</summary>
    public TimeSpan GetBaseDelay(int attempt)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);
        switch (_shape)
        {
            case Shape.Immediate:
                return TimeSpan.Zero;
            case Shape.Interval:
                return _interval;
            case Shape.Linear:
                return _interval * attempt;
            default:
                if (_interval <= TimeSpan.Zero)
                {
                    return TimeSpan.Zero;
                }

                // Computed in double ticks and clamped, because a large factor raised to a large
                // attempt overflows TimeSpan multiplication long before the clamp would apply.
                var ticks = Math.Min(
                    _interval.Ticks * Math.Pow(_factor, attempt - 1),
                    _maxInterval.Ticks
                );
                return TimeSpan.FromTicks((long)ticks);
        }
    }

    /// <summary>Delay preceding retry <paramref name="attempt"/>, counted from 1, with jitter applied.</summary>
    public TimeSpan GetDelay(int attempt)
    {
        var delay = GetBaseDelay(attempt);
        if (Jitter == TimeSpan.Zero)
        {
            return delay;
        }

        // CA5394: jitter exists to decorrelate retry timing between callers, not to resist an
        // attacker. A cryptographic generator would cost more and buy nothing here.
#pragma warning disable CA5394
        var offset = Random.Shared.NextDouble();
#pragma warning restore CA5394
        return delay + (Jitter * offset);
    }

    private string Describe()
    {
        var invariant = CultureInfo.InvariantCulture;
        var shape = _shape switch
        {
            Shape.Immediate => "immediate",
            Shape.Interval => string.Create(
                invariant,
                $"interval {_interval.TotalMilliseconds:0} ms"
            ),
            Shape.Linear => string.Create(
                invariant,
                $"linear step {_interval.TotalMilliseconds:0} ms"
            ),
            _ => string.Create(
                invariant,
                $"exponential {_interval.TotalMilliseconds:0} ms to {_maxInterval.TotalMilliseconds:0} ms x {_factor}"
            ),
        };
        var jitter =
            Jitter == TimeSpan.Zero
                ? ""
                : string.Create(invariant, $", jitter {Jitter.TotalMilliseconds:0} ms");
        return string.Create(invariant, $"{shape}, {RetryLimit} retries{jitter}");
    }
}
