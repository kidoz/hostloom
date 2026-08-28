using System.Diagnostics.Metrics;
using HostLoom.Pipelines;
using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.Tests;

public sealed class DiagnosticsTests
{
    [Fact]
    public async Task Liveness_stays_healthy_while_readiness_fails_on_an_unreachable_broker()
    {
        using var host = await StartAsync("live-probe");
        ((InMemoryRequestBroker)host.Services.GetRequiredService<IRequestBroker>()).IsReachable =
            false;
        var health = host.Services.GetRequiredService<HealthCheckService>();

        var live = await health.CheckHealthAsync(
            r => r.Tags.Contains("live"),
            TestContext.Current.CancellationToken
        );
        var ready = await health.CheckHealthAsync(
            r => r.Tags.Contains("ready"),
            TestContext.Current.CancellationToken
        );

        // The whole point of the split: a broker outage must not tell Kubernetes to restart the
        // pod, or one broker outage becomes a cluster-wide restart storm.
        Assert.Equal(HealthStatus.Healthy, live.Status);
        Assert.Equal(HealthStatus.Unhealthy, ready.Status);
    }

    [Fact]
    public async Task Readiness_is_healthy_once_endpoints_are_listening()
    {
        using var host = await StartAsync("ready-probe");
        var health = host.Services.GetRequiredService<HealthCheckService>();

        var ready = await health.CheckHealthAsync(
            r => r.Tags.Contains("ready"),
            TestContext.Current.CancellationToken
        );

        Assert.Equal(HealthStatus.Healthy, ready.Status);
    }

    [Fact]
    public async Task Readiness_is_unhealthy_before_the_host_starts()
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHealthChecks()
            .AddHandler<Ping, Pong, PingHandler>("not-started");

        using var host = builder.Build();
        var health = host.Services.GetRequiredService<HealthCheckService>();

        // Never started, so the endpoint is registered but nothing is listening on it.
        var ready = await health.CheckHealthAsync(
            r => r.Tags.Contains("ready"),
            TestContext.Current.CancellationToken
        );

        Assert.Equal(HealthStatus.Unhealthy, ready.Status);
        Assert.Contains(
            "not listening",
            ready.Entries.Values.Single().Description,
            StringComparison.Ordinal
        );
    }

    [Fact]
    public async Task A_subscription_only_application_is_not_ready_before_it_starts()
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHealthChecks()
            .AddSubscriber<Pinged, PingedSubscriber>("orders", subscription: "audit");

        using var host = builder.Build();
        var health = host.Services.GetRequiredService<HealthCheckService>();

        var ready = await health.CheckHealthAsync(
            r => r.Tags.Contains("ready"),
            TestContext.Current.CancellationToken
        );

        // Counting request endpoints alone reported this as a healthy client-only service while
        // no subscription was listening — ready, and consuming nothing.
        Assert.Equal(HealthStatus.Unhealthy, ready.Status);
        Assert.Contains(
            "not listening",
            ready.Entries.Values.Single().Description,
            StringComparison.Ordinal
        );
    }

    [Fact]
    public async Task A_subscription_only_application_is_ready_once_it_starts()
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHealthChecks()
            .AddSubscriber<Pinged, PingedSubscriber>("orders", subscription: "audit");

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var health = host.Services.GetRequiredService<HealthCheckService>();

        var ready = await health.CheckHealthAsync(
            r => r.Tags.Contains("ready"),
            TestContext.Current.CancellationToken
        );

        Assert.Equal(HealthStatus.Healthy, ready.Status);
    }

    [Fact]
    public async Task A_client_only_application_is_ready_without_any_endpoint()
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHealthChecks()
            .AddRequestClient<Ping, Pong>();

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var health = host.Services.GetRequiredService<HealthCheckService>();

        var ready = await health.CheckHealthAsync(
            r => r.Tags.Contains("ready"),
            TestContext.Current.CancellationToken
        );

        Assert.Equal(HealthStatus.Healthy, ready.Status);
    }

    [Fact]
    public async Task A_handled_request_records_duration_and_balances_the_active_count()
    {
        const string endpoint = "metrics-ok";
        using var recorder = new MetricRecorder(endpoint);
        using var host = await StartAsync(endpoint);

        await ClientOf(host)
            .GetResponseAsync(
                endpoint,
                new Ping(),
                cancellationToken: TestContext.Current.CancellationToken
            );

        Assert.Single(recorder.Values("hostloom.request.duration"));
        Assert.All(recorder.Values("hostloom.request.duration"), value => Assert.True(value >= 0));
        // One increment and one decrement, so the gauge returns to zero.
        Assert.Equal([1, -1], recorder.Values("hostloom.request.active"));
        Assert.Empty(recorder.Values("hostloom.request.faults"));
    }

    [Fact]
    public async Task A_faulted_request_is_counted_as_a_fault()
    {
        const string endpoint = "metrics-fault";
        using var recorder = new MetricRecorder(endpoint);
        using var host = await StartAsync(endpoint, failUntil: int.MaxValue);

        await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await ClientOf(host)
                .GetResponseAsync(
                    endpoint,
                    new Ping(),
                    cancellationToken: TestContext.Current.CancellationToken
                )
        );

        Assert.Equal([1], recorder.Values("hostloom.request.faults"));
        Assert.Single(recorder.Values("hostloom.request.duration"));
    }

    [Fact]
    public async Task Receive_pipeline_retries_are_counted()
    {
        const string endpoint = "metrics-retry";
        using var recorder = new MetricRecorder(endpoint);
        using var host = await StartAsync(
            endpoint,
            failUntil: 3,
            configure: pipe => pipe.UseRetry(RetryPolicy.Immediate(3))
        );

        await ClientOf(host)
            .GetResponseAsync(
                endpoint,
                new Ping(),
                cancellationToken: TestContext.Current.CancellationToken
            );

        // Three handler invocations is two retries.
        Assert.Equal([2], recorder.Values("hostloom.request.retries"));
        Assert.Empty(recorder.Values("hostloom.request.faults"));
    }

    [Fact]
    public async Task Probe_describes_the_filters_composed_around_handler_execution()
    {
        using var host = await StartAsync(
            "probe",
            configure: pipe =>
            {
                pipe.UseRetry(RetryPolicy.Immediate(2));
                pipe.UseCircuitBreaker(3, TimeSpan.FromSeconds(30));
            }
        );

        var probe = host.Services.GetRequiredService<HostLoomProbe>();
        var result = probe.ReceivePipeline(TestContext.Current.CancellationToken);

        Assert.Equal(
            ["retry", "circuitBreaker", "executeReceive", "empty"],
            result.Children.Select(child => child.Name)
        );
        Assert.Equal(2, result.Children[0].Properties["retryLimit"]);
        Assert.Equal("Closed", result.Children[1].Properties["state"]);
    }

    private static async Task<IHost> StartAsync(
        string endpoint,
        int failUntil = 1,
        Action<PipeBuilder<ReceiveContext>>? configure = null
    )
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(new Attempts { FailUntil = failUntil });

        var hostLoom = builder.Services.AddHostLoom().UseInMemory().AddHealthChecks();
        if (configure is not null)
        {
            hostLoom.ConfigureReceivePipeline(configure);
        }

        hostLoom.AddHandler<Ping, Pong, PingHandler>(endpoint);

        var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        return host;
    }

    private static IRequestClient<Ping, Pong> ClientOf(IHost host) =>
        host.Services.GetRequiredService<IRequestClient<Ping, Pong>>();

    public sealed record Ping : IRequest<Pong>;

    public sealed record Pinged : IEvent;

    public sealed class PingedSubscriber : IEventHandler<Pinged>
    {
        public ValueTask HandleAsync(Pinged @event, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;
    }

    public sealed record Pong(string Value);

    public sealed class Attempts
    {
        private int _count;

        public int FailUntil { get; init; } = 1;

        public int Next() => Interlocked.Increment(ref _count);
    }

    public sealed class PingHandler(Attempts attempts) : IRequestHandler<Ping, Pong>
    {
        public ValueTask<Pong> HandleAsync(Ping request, CancellationToken cancellationToken) =>
            attempts.Next() < attempts.FailUntil
                ? throw new InvalidOperationException("transient")
                : ValueTask.FromResult(new Pong("pong"));
    }

    /// <summary>
    /// Collects HostLoom measurements for one endpoint. The meter is process-wide and static, so
    /// filtering by destination keeps concurrently running tests from contaminating each other.
    /// </summary>
    private sealed class MetricRecorder : IDisposable
    {
        private readonly string _endpoint;
        private readonly MeterListener _listener = new();
        private readonly List<(string Name, double Value)> _measurements = [];
        private readonly Lock _gate = new();

        public MetricRecorder(string endpoint)
        {
            _endpoint = endpoint;
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == HostLoomDiagnostics.MeterName)
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<double>(
                (instrument, value, tags, _) => Record(instrument, value, tags)
            );
            _listener.SetMeasurementEventCallback<long>(
                (instrument, value, tags, _) => Record(instrument, value, tags)
            );
            _listener.Start();
        }

        public List<double> Values(string instrumentName)
        {
            lock (_gate)
            {
                return _measurements
                    .Where(m => m.Name == instrumentName)
                    .Select(m => m.Value)
                    .ToList();
            }
        }

        public void Dispose() => _listener.Dispose();

        private void Record(
            Instrument instrument,
            double value,
            ReadOnlySpan<KeyValuePair<string, object?>> tags
        )
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "messaging.destination.name" && (tag.Value as string) == _endpoint)
                {
                    lock (_gate)
                    {
                        _measurements.Add((instrument.Name, value));
                    }

                    return;
                }
            }
        }
    }
}
