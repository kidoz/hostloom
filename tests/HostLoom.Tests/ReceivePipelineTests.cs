using HostLoom.Pipelines;
using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.Tests;

public sealed class ReceivePipelineTests
{
    [Fact]
    public async Task Retry_filter_reruns_a_failing_handler_until_it_succeeds()
    {
        var attempts = new Attempts { FailUntil = 3 };
        using var host = await StartAsync(attempts, pipe => pipe.UseRetry(RetryPolicy.Immediate(3)));

        var response = await ClientOf(host).GetResponseAsync(
            "flaky",
            new Flaky(),
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal("done", response.Value);
        Assert.Equal(3, attempts.Count);
    }

    [Fact]
    public async Task Without_a_receive_pipeline_the_first_failure_becomes_a_fault()
    {
        var attempts = new Attempts { FailUntil = 3 };
        using var host = await StartAsync(attempts, configure: null);

        await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await ClientOf(host).GetResponseAsync(
                "flaky",
                new Flaky(),
                cancellationToken: TestContext.Current.CancellationToken));

        Assert.Equal(1, attempts.Count);
    }

    [Fact]
    public async Task Every_retry_attempt_runs_in_a_fresh_dependency_injection_scope()
    {
        var attempts = new Attempts { FailUntil = 3 };
        using var host = await StartAsync(attempts, pipe => pipe.UseRetry(RetryPolicy.Immediate(3)));

        await ClientOf(host).GetResponseAsync(
            "flaky",
            new Flaky(),
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(3, attempts.ScopeIds.Count);
        Assert.Equal(3, attempts.ScopeIds.Distinct().Count());
    }

    [Fact]
    public async Task Retry_exhaustion_still_returns_the_handler_fault_to_the_caller()
    {
        var attempts = new Attempts();
        using var host = await StartAsync(attempts, pipe => pipe.UseRetry(RetryPolicy.Immediate(2)));

        var exception = await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await ClientOf(host).GetResponseAsync(
                "flaky",
                new Flaky(),
                cancellationToken: TestContext.Current.CancellationToken));

        Assert.Equal(typeof(InvalidOperationException).FullName, exception.ErrorType);
        Assert.Equal(3, attempts.Count);
    }

    [Fact]
    public async Task Open_circuit_breaker_stops_later_deliveries_reaching_the_handler()
    {
        var attempts = new Attempts();
        using var host = await StartAsync(
            attempts,
            pipe => pipe.UseCircuitBreaker(failureThreshold: 2, resetInterval: TimeSpan.FromMinutes(5)));
        var client = ClientOf(host);

        for (var i = 0; i < 2; i++)
        {
            await Assert.ThrowsAsync<RemoteRequestException>(async () =>
                await client.GetResponseAsync("flaky", new Flaky(), cancellationToken: TestContext.Current.CancellationToken));
        }

        Assert.Equal(2, attempts.Count);

        // The breaker is open: this delivery is rejected before the handler runs, and the
        // rejection travels back to the caller as an ordinary remote fault.
        var exception = await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await client.GetResponseAsync("flaky", new Flaky(), cancellationToken: TestContext.Current.CancellationToken));

        Assert.Equal(typeof(CircuitBreakerOpenException).FullName, exception.ErrorType);
        Assert.Equal(2, attempts.Count);
    }

    [Fact]
    public async Task Receive_filters_observe_the_endpoint_and_message_before_the_handler()
    {
        var observed = new List<string>();
        var attempts = new Attempts { FailUntil = 1 };
        using var host = await StartAsync(attempts, pipe => pipe.Use(
            async (context, next) =>
            {
                observed.Add($"{context.Destination.Value}|{context.MessageType}|{context.Message.GetType().Name}");
                await next.SendAsync(context);
            },
            "observer"));

        await ClientOf(host).GetResponseAsync(
            "flaky",
            new Flaky(),
            cancellationToken: TestContext.Current.CancellationToken);

        var entry = Assert.Single(observed);
        Assert.StartsWith("flaky|", entry, StringComparison.Ordinal);
        Assert.Contains("Flaky", entry, StringComparison.Ordinal);
    }

    private static async Task<IHost> StartAsync(Attempts attempts, Action<PipeBuilder<ReceiveContext>>? configure)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(attempts);
        builder.Services.AddScoped<ScopeMarker>();

        var hostLoom = builder.Services.AddHostLoom().UseInMemory();
        if (configure is not null)
        {
            hostLoom.ConfigureReceivePipeline(configure);
        }

        hostLoom.AddHandler<Flaky, Done, FlakyHandler>("flaky");

        var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        return host;
    }

    private static IRequestClient<Flaky, Done> ClientOf(IHost host) =>
        host.Services.GetRequiredService<IRequestClient<Flaky, Done>>();

    public sealed record Flaky : IRequest<Done>;

    public sealed record Done(string Value);

    /// <summary>Singleton tally shared across delivery attempts and scopes.</summary>
    public sealed class Attempts
    {
        private readonly List<Guid> _scopeIds = [];

        /// <summary>Attempt number at which the handler starts succeeding. Never, by default.</summary>
        public int FailUntil { get; init; } = int.MaxValue;

        public int Count => _scopeIds.Count;

        public IReadOnlyList<Guid> ScopeIds => _scopeIds;

        public void Record(Guid scopeId) => _scopeIds.Add(scopeId);
    }

    /// <summary>Scoped, so its identity reveals whether a retry opened a new scope.</summary>
    public sealed class ScopeMarker
    {
        public Guid Id { get; } = Guid.NewGuid();
    }

    public sealed class FlakyHandler(Attempts attempts, ScopeMarker marker) : IRequestHandler<Flaky, Done>
    {
        public ValueTask<Done> HandleAsync(Flaky request, CancellationToken cancellationToken)
        {
            attempts.Record(marker.Id);
            return attempts.Count < attempts.FailUntil
                ? throw new InvalidOperationException("transient")
                : ValueTask.FromResult(new Done("done"));
        }
    }
}
