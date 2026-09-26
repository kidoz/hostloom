using System.Diagnostics.Metrics;
using System.Reflection;
using HostLoom.Logging;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Without bucket advice an exporter falls back to its own defaults, which for OpenTelemetry end
/// at 10 000 — ten thousand seconds for these instruments — and put every sub-millisecond cache
/// hit into one bucket.
/// </summary>
public sealed class HistogramAdviceTests
{
    [Fact]
    public void Every_histogram_advises_ascending_bucket_boundaries()
    {
        var histograms = StaticInstruments()
            .Concat(LoggingInstruments())
            .Where(IsHistogram)
            .ToList();

        Assert.Equal(15, histograms.Count);
        Assert.All(
            histograms,
            histogram =>
            {
                var boundaries = Boundaries(histogram);
                Assert.True(boundaries is { Count: > 0 }, $"{histogram.Name} has no bucket advice");
                Assert.True(
                    boundaries.Zip(boundaries.Skip(1)).All(pair => pair.First < pair.Second),
                    $"{histogram.Name} boundaries are not strictly ascending"
                );
            }
        );
    }

    private static IEnumerable<Instrument> StaticInstruments() =>
        typeof(HistogramAdviceTests)
            .Assembly.GetReferencedAssemblies()
            .Where(name => name.Name!.StartsWith("HostLoom", StringComparison.Ordinal))
            .Select(Assembly.Load)
            .SelectMany(assembly => assembly.GetTypes())
            .SelectMany(type =>
                type.GetFields(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
            )
            .Where(field =>
                typeof(Instrument).IsAssignableFrom(field.FieldType)
                && !field.FieldType.ContainsGenericParameters
            )
            .Select(field => (Instrument)field.GetValue(null)!);

    private static List<Instrument> LoggingInstruments()
    {
        using var metrics = new LoggingMetrics(() => 0, () => true, () => "running");
        return typeof(LoggingMetrics)
            .GetFields(BindingFlags.Instance | BindingFlags.NonPublic)
            .Where(field => typeof(Instrument).IsAssignableFrom(field.FieldType))
            .Select(field => (Instrument)field.GetValue(metrics)!)
            .ToList();
    }

    private static bool IsHistogram(Instrument instrument) =>
        instrument is Histogram<double> or Histogram<long>;

    private static IReadOnlyList<double>? Boundaries(Instrument instrument) =>
        instrument switch
        {
            Histogram<double> histogram => histogram.Advice?.HistogramBucketBoundaries,
            Histogram<long> histogram => histogram
                .Advice?.HistogramBucketBoundaries?.Select(value => (double)value)
                .ToList(),
            _ => null,
        };
}
