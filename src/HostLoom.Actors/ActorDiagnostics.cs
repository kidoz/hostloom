using System.Diagnostics.Metrics;

namespace HostLoom.Actors;

public readonly record struct ActorSnapshot(
    long Actors,
    long Queued,
    long Executing,
    long Full,
    long Stopped,
    long Discarded,
    long Faults,
    long Timeouts
);

internal sealed class ActorDiagnostics : IDisposable
{
    private readonly Meter _meter = new("HostLoom.Actors");
    private readonly Histogram<double> _delay;
    private readonly Histogram<double> _duration;
    private long _actors,
        _queued,
        _executing,
        _full,
        _stopped,
        _discarded,
        _faults,
        _timeouts;

    internal ActorDiagnostics()
    {
        _meter.CreateObservableGauge(
            "hostloom.actors.population",
            () => Volatile.Read(ref _actors)
        );
        _meter.CreateObservableGauge("hostloom.actors.queued", () => Volatile.Read(ref _queued));
        _meter.CreateObservableGauge(
            "hostloom.actors.executing",
            () => Volatile.Read(ref _executing)
        );
        _meter.CreateObservableCounter("hostloom.actors.full", () => Volatile.Read(ref _full));
        _meter.CreateObservableCounter(
            "hostloom.actors.stopped_sends",
            () => Volatile.Read(ref _stopped)
        );
        _meter.CreateObservableCounter(
            "hostloom.actors.discarded",
            () => Volatile.Read(ref _discarded)
        );
        _meter.CreateObservableCounter("hostloom.actors.faults", () => Volatile.Read(ref _faults));
        _meter.CreateObservableCounter(
            "hostloom.actors.ask_timeouts",
            () => Volatile.Read(ref _timeouts)
        );
        _delay = _meter.CreateHistogram<double>("hostloom.actors.queue_delay", "s");
        _duration = _meter.CreateHistogram<double>("hostloom.actors.turn_duration", "s");
    }

    internal ActorSnapshot Snapshot =>
        new(
            Volatile.Read(ref _actors),
            Volatile.Read(ref _queued),
            Volatile.Read(ref _executing),
            Volatile.Read(ref _full),
            Volatile.Read(ref _stopped),
            Volatile.Read(ref _discarded),
            Volatile.Read(ref _faults),
            Volatile.Read(ref _timeouts)
        );

    internal void Reserve() => Interlocked.Increment(ref _actors);

    internal void Release() => Interlocked.Decrement(ref _actors);

    internal void Enqueue() => Interlocked.Increment(ref _queued);

    internal void BeginTurn()
    {
        Interlocked.Decrement(ref _queued);
        Interlocked.Increment(ref _executing);
    }

    internal void EndTurn()
    {
        Interlocked.Decrement(ref _executing);
    }

    internal void RecordTurn(TimeSpan delay, TimeSpan duration)
    {
        _delay.Record(delay.TotalSeconds);
        _duration.Record(duration.TotalSeconds);
    }

    internal void Full() => Interlocked.Increment(ref _full);

    internal void Stopped() => Interlocked.Increment(ref _stopped);

    internal void Fault() => Interlocked.Increment(ref _faults);

    internal void Timeout() => Interlocked.Increment(ref _timeouts);

    internal void Discard()
    {
        Interlocked.Decrement(ref _queued);
        Interlocked.Increment(ref _discarded);
    }

    public void Dispose() => _meter.Dispose();
}
