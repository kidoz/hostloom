using HostLoom.Analyzers.Infrastructure;
using HostLoom.Analyzers.Usage;
using Microsoft.CodeAnalysis;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed class SingletonMapperInjectionAnalyzerTests
{
    private const string Contracts = """
        using HostLoom.Mapping;
        using Microsoft.Extensions.DependencyInjection;
        using Microsoft.Extensions.Hosting;
        using System.Threading;
        using System.Threading.Tasks;

        public sealed record Source(string Name);

        public sealed record Destination(string Name);

        public sealed class CapturesDispatcher
        {
            public CapturesDispatcher(IMapper mapper) { }
        }

        public sealed class CapturesClosedMap
        {
            public CapturesClosedMap(IMapper<Source, Destination> mapper) { }
        }

        public sealed class Subscriber : IHostedService
        {
            public Subscriber(IMapper mapper) { }

            public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

            public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        }
        """;

    [Fact]
    public async Task A_singleton_taking_the_dispatcher_is_reported()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            "services.AddSingleton<CapturesDispatcher>();"
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(
            HostLoomDiagnosticDescriptors.SingletonMapperInjectionDiagnosticId,
            diagnostic.Id
        );
    }

    [Fact]
    public async Task A_hosted_service_taking_the_dispatcher_is_reported()
    {
        // The shape the pilot actually hit: an IHostedService subscriber is a singleton the host
        // resolves once, so the captive dependency is the same one by another registration.
        Diagnostic[] diagnostics = await AnalyzeAsync("services.AddHostedService<Subscriber>();");

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(
            HostLoomDiagnosticDescriptors.SingletonMapperInjectionDiagnosticId,
            diagnostic.Id
        );
    }

    [Fact]
    public async Task A_singleton_taking_a_closed_map_is_not_reported()
    {
        // The closed map is transient and is exactly what the rule tells callers to inject, so
        // reporting it would contradict the fix.
        Diagnostic[] diagnostics = await AnalyzeAsync(
            "services.AddSingleton<CapturesClosedMap>();"
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task A_scoped_registration_of_the_dispatcher_consumer_is_not_reported()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync("services.AddScoped<CapturesDispatcher>();");

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task A_keyed_singleton_taking_the_dispatcher_is_reported()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """services.AddKeyedSingleton<CapturesDispatcher>("key");"""
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(
            HostLoomDiagnosticDescriptors.SingletonMapperInjectionDiagnosticId,
            diagnostic.Id
        );
    }

    private static Task<Diagnostic[]> AnalyzeAsync(string registration) =>
        AnalyzerTestHarness.AnalyzeAsync(
            $$"""
            {{Contracts}}

            public static class Registration
            {
                public static void Configure(IServiceCollection services)
                {
                    {{registration}}
                }
            }
            """,
            new SingletonMapperInjectionAnalyzer()
        );
}
