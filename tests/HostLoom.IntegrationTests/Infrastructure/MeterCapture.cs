using System.Diagnostics.Metrics;

namespace HostLoom.IntegrationTests.Infrastructure;

/// <summary>
/// Records one meter's measurements that carry a scoping tag, such as a unique client name,
/// topic, or namespace, so a real-backend test reads its own traffic while parallel tests share
/// the static meter. Counters and histograms accumulate; observable gauges are read on demand.
/// </summary>
internal sealed class MeterCapture : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly string _scopeTag;
    private readonly string _scopeValue;
    private readonly Lock _gate = new();
    private readonly List<Measured> _measurements = [];
    private readonly Dictionary<string, double> _observed = new(StringComparer.Ordinal);
    private TaskCompletionSource _changed = NewSignal();

    public MeterCapture(string meterName, string scopeTag, string scopeValue)
    {
        _scopeTag = scopeTag;
        _scopeValue = scopeValue;
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == meterName)
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        _listener.SetMeasurementEventCallback<long>(
            (instrument, value, tags, _) => Record(instrument, value, tags)
        );
        _listener.SetMeasurementEventCallback<int>(
            (instrument, value, tags, _) => Record(instrument, value, tags)
        );
        _listener.SetMeasurementEventCallback<double>(
            (instrument, value, tags, _) => Record(instrument, value, tags)
        );
        _listener.Start();
    }

    /// <summary>Sum of an instrument's recorded values whose tags include every pair given.</summary>
    public double Sum(string instrument, params (string Key, string Value)[] tags)
    {
        lock (_gate)
        {
            return _measurements
                .Where(m => m.Name == instrument && tags.All(tag => Has(m.Tags, tag)))
                .Sum(m => m.Value);
        }
    }

    /// <summary>Number of recordings, rather than their sum, for a histogram.</summary>
    public int Count(string instrument, params (string Key, string Value)[] tags)
    {
        lock (_gate)
        {
            return _measurements.Count(m =>
                m.Name == instrument && tags.All(tag => Has(m.Tags, tag))
            );
        }
    }

    /// <summary>Polls observable instruments once and returns this scope's current value.</summary>
    public double? Observe(string instrument)
    {
        lock (_gate)
        {
            _observed.Remove(instrument);
        }

        _listener.RecordObservableInstruments();
        lock (_gate)
        {
            return _observed.TryGetValue(instrument, out var value) ? value : null;
        }
    }

    /// <summary>
    /// Completes once <paramref name="instrument"/> has summed to at least
    /// <paramref name="atLeast"/> for the given tags; woken by recordings, never by polling.
    /// </summary>
    public async Task WaitForAsync(
        string instrument,
        double atLeast,
        CancellationToken cancellationToken,
        params (string Key, string Value)[] tags
    )
    {
        while (true)
        {
            Task changed;
            lock (_gate)
            {
                changed = _changed.Task;
            }

            if (Sum(instrument, tags) >= atLeast)
            {
                return;
            }

            await changed.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public void Dispose() => _listener.Dispose();

    private static TaskCompletionSource NewSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static bool Has(
        KeyValuePair<string, object?>[] tags,
        (string Key, string Value) pair
    ) =>
        tags.Any(tag =>
            tag.Key == pair.Key
            && string.Equals(tag.Value?.ToString(), pair.Value, StringComparison.Ordinal)
        );

    private void Record<T>(
        Instrument instrument,
        T value,
        ReadOnlySpan<KeyValuePair<string, object?>> tags
    )
        where T : struct, IConvertible
    {
        var copy = tags.ToArray();
        if (!Has(copy, (_scopeTag, _scopeValue)))
        {
            return;
        }

        var number = value.ToDouble(System.Globalization.CultureInfo.InvariantCulture);
        TaskCompletionSource signal;
        lock (_gate)
        {
            if (instrument.IsObservable)
            {
                _observed[instrument.Name] = _observed.GetValueOrDefault(instrument.Name) + number;
                return;
            }

            _measurements.Add(new Measured(instrument.Name, number, copy));
            signal = _changed;
            _changed = NewSignal();
        }

        signal.TrySetResult();
    }

    private sealed record Measured(string Name, double Value, KeyValuePair<string, object?>[] Tags);
}
