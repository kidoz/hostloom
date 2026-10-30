using System.Diagnostics.Metrics;
using System.Net;
using HostLoom.RateLimiting;
using HostLoom.RateLimiting.HttpClient;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(RateLimitingDiagnosticsTests))]
[CollectionDefinition(nameof(RateLimitingDiagnosticsTests), DisableParallelization = true)]
public sealed class RateLimitingDiagnosticsTests
{
    [Fact]
    public async Task KernelAndHttpMeters_EmitAcquisitionMeasurements()
    {
        var instruments = new List<string>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (
                instrument.Meter.Name
                is RateLimitingDiagnostics.MeterName
                    or HttpRateLimitingDiagnostics.MeterName
            )
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>(
            (instrument, _, _, _) =>
            {
                lock (instruments)
                {
                    instruments.Add(instrument.Name);
                }
            }
        );
        listener.SetMeasurementEventCallback<double>(
            (instrument, _, _, _) =>
            {
                lock (instruments)
                {
                    instruments.Add(instrument.Name);
                }
            }
        );
        listener.Start();
        using var limiter = new DistributedRateLimiter(
            new RateLimitingOptions
            {
                Namespace = "catalog",
                TokenBucket = new TokenBucketRateLimitPolicy { TokenLimit = 2 },
            },
            "catalog",
            new InMemoryRateLimitStore()
        );

        using var lease = await limiter.AcquireAsync(
            cancellationToken: TestContext.Current.CancellationToken
        );
        using var downstream = new OkHandler();
        using var handler = new RateLimitedHttpMessageHandler(limiter)
        {
            InnerHandler = downstream,
        };
        using var client = new HttpClient(handler);
        using var response = await client.GetAsync(
            new Uri("https://catalog.example"),
            TestContext.Current.CancellationToken
        );

        Assert.Contains("hostloom.ratelimit.acquisitions", instruments);
        Assert.Contains("hostloom.ratelimit.acquire.duration", instruments);
        Assert.Contains("hostloom.ratelimit.http.requests", instruments);
        Assert.Contains("hostloom.ratelimit.http.wait.duration", instruments);
    }

    private sealed class OkHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken
        ) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
    }
}
