using HostLoom.Pipelines;
using Microsoft.Extensions.Logging;

namespace HostLoom;

/// <summary>
/// Failure ratio, over a window, above which one receive destination stops taking work.
/// </summary>
public sealed class KillSwitchPolicy
{
    public KillSwitchPolicy(
        int activationThreshold,
        double tripThreshold,
        TimeSpan trackingPeriod,
        TimeSpan restartTimeout,
        Func<Exception, bool>? shouldCount = null
    )
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(activationThreshold, 1);
        if (tripThreshold is <= 0 or > 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(tripThreshold),
                "The trip threshold must be greater than 0 and at most 1."
            );
        }

        if (trackingPeriod <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(trackingPeriod),
                "The tracking period must be positive."
            );
        }

        if (restartTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(restartTimeout),
                "The restart timeout must be positive."
            );
        }

        ActivationThreshold = activationThreshold;
        TripThreshold = tripThreshold;
        TrackingPeriod = trackingPeriod;
        RestartTimeout = restartTimeout;
        ShouldCount = shouldCount;
    }

    public int ActivationThreshold { get; }

    public double TripThreshold { get; }

    public TimeSpan TrackingPeriod { get; }

    public TimeSpan RestartTimeout { get; }

    public Func<Exception, bool>? ShouldCount { get; }
}

/// <summary>
/// Counts failures per destination and pauses that destination when the ratio trips. The delivery
/// that trips still finishes as a fault. The next one is refused.
/// </summary>
internal sealed class KillSwitchFilter : IFilter<ReceiveContext>
{
    private readonly KillSwitchPolicy _policy;
    private readonly ReceiveAdmission _admission;
    private readonly TimeProvider _clock;
    private readonly ILogger _logger;
    private readonly Lock _gate = new();
    private readonly Dictionary<ReceiveDestination, Window> _windows = [];

    public KillSwitchFilter(
        KillSwitchPolicy policy,
        ReceiveAdmission admission,
        TimeProvider clock,
        ILogger logger
    )
    {
        _policy = policy;
        _admission = admission;
        _clock = clock;
        _logger = logger;
    }

    public async ValueTask SendAsync(ReceiveContext context, IPipe<ReceiveContext> next)
    {
        var destination = ReceiveDestination.For(context);
        var now = _clock.GetUtcNow();
        if (_admission.TryGetSuspension(destination, now, out var resumeAt))
        {
            throw new ReceiveSuspendedException(
                context.Destination,
                destination.Subscription,
                resumeAt
            );
        }

        try
        {
            await next.SendAsync(context).ConfigureAwait(false);
            Record(destination, failed: false);
        }
        catch (Exception exception) when (Counts(exception))
        {
            if (Record(destination, failed: true))
            {
                var resume = _clock.GetUtcNow() + _policy.RestartTimeout;
                _admission.Suspend(destination, resume);
                HostLoomDiagnostics.ReceiveSuspensions.Add(
                    1,
                    new System.Diagnostics.TagList
                    {
                        { "messaging.destination.name", destination.Address },
                    }
                );
                _logger.LogWarning(
                    "Receive destination '{Destination}' paused until {ResumeAt} after its failure ratio tripped.",
                    destination.Address,
                    resume
                );
            }

            throw;
        }
    }

    public void Probe(IProbeContext context)
    {
        var scope = context.CreateScope("kill-switch");
        scope.Set("activationThreshold", _policy.ActivationThreshold);
        scope.Set("tripThreshold", _policy.TripThreshold);
        scope.Set("trackingPeriod", _policy.TrackingPeriod.ToString());
        scope.Set("suspended", _admission.CountSuspended(_clock.GetUtcNow()));
    }

    private bool Counts(Exception exception)
    {
        if (exception is OperationCanceledException or ReceiveSuspendedException)
        {
            return false;
        }

        return _policy.ShouldCount?.Invoke(exception) ?? true;
    }

    /// <summary>Records one attempt and reports whether the window just tripped.</summary>
    private bool Record(ReceiveDestination destination, bool failed)
    {
        var now = _clock.GetUtcNow();
        lock (_gate)
        {
            if (!_windows.TryGetValue(destination, out var window))
            {
                window = new Window();
                _windows[destination] = window;
            }

            window.Add(now, failed, _policy.TrackingPeriod);
            if (window.Count < _policy.ActivationThreshold)
            {
                return false;
            }

            if (window.Failures / (double)window.Count < _policy.TripThreshold)
            {
                return false;
            }

            window.Clear();
            return true;
        }
    }

    private sealed class Window
    {
        private readonly Queue<Attempt> _attempts = [];

        public int Count => _attempts.Count;

        public int Failures { get; private set; }

        public void Add(DateTimeOffset now, bool failed, TimeSpan period)
        {
            var cutoff = now - period;
            while (_attempts.Count > 0 && _attempts.Peek().At < cutoff)
            {
                if (_attempts.Dequeue().Failed)
                {
                    Failures--;
                }
            }

            _attempts.Enqueue(new Attempt(now, failed));
            if (failed)
            {
                Failures++;
            }
        }

        public void Clear()
        {
            _attempts.Clear();
            Failures = 0;
        }

        private readonly record struct Attempt(DateTimeOffset At, bool Failed);
    }
}
