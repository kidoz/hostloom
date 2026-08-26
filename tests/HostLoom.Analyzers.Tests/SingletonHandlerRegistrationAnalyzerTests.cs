using HostLoom.Analyzers.Infrastructure;
using HostLoom.Analyzers.Usage;
using Microsoft.CodeAnalysis;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed class SingletonHandlerRegistrationAnalyzerTests
{
    [Fact]
    public async Task Reports_singleton_handler_registrations()
    {
        Diagnostic[] diagnostics = await AnalyzerTestHarness.AnalyzeAsync(
            Contracts
                + """

                internal static class Registration
                {
                    public static void Configure(IServiceCollection services)
                    {
                        services.AddSingleton<GreetingHandler>();
                        services.TryAddSingleton<AuditHandler>();
                        services.AddKeyedSingleton<GreetingBehavior>("greetings");
                    }
                }
                """,
            new SingletonHandlerRegistrationAnalyzer()
        );

        Assert.Equal(3, diagnostics.Length);
        Assert.All(
            diagnostics,
            diagnostic =>
                Assert.Equal(
                    HostLoomDiagnosticDescriptors.SingletonHandlerRegistrationDiagnosticId,
                    diagnostic.Id
                )
        );
    }

    [Fact]
    public async Task Accepts_scoped_handlers_and_unrelated_singletons()
    {
        Diagnostic[] diagnostics = await AnalyzerTestHarness.AnalyzeAsync(
            Contracts
                + """

                internal static class Registration
                {
                    public static void Configure(IServiceCollection services)
                    {
                        services.AddScoped<GreetingHandler>();
                        services.AddScoped<AuditHandler>();
                        services.AddScoped<GreetingBehavior>();
                        services.AddSingleton<ThreadSafeCache>();
                    }
                }

                internal sealed class ThreadSafeCache;
                """,
            new SingletonHandlerRegistrationAnalyzer()
        );

        Assert.Empty(diagnostics);
    }

    private const string Contracts = """
        using System.Threading;
        using System.Threading.Tasks;
        using HostLoom;
        using Microsoft.Extensions.DependencyInjection;
        using Microsoft.Extensions.DependencyInjection.Extensions;

        internal sealed record GreetingResponse(string Text);
        internal sealed record GreetingRequest : IRequest<GreetingResponse>;
        internal sealed record AuditEvent : IEvent;

        internal sealed class GreetingHandler : IRequestHandler<GreetingRequest, GreetingResponse>
        {
            public ValueTask<GreetingResponse> HandleAsync(
                GreetingRequest request,
                CancellationToken cancellationToken) =>
                ValueTask.FromResult(new GreetingResponse("hello"));
        }

        internal sealed class AuditHandler : IEventHandler<AuditEvent>
        {
            public ValueTask HandleAsync(
                AuditEvent @event,
                CancellationToken cancellationToken) => ValueTask.CompletedTask;
        }

        internal sealed class GreetingBehavior : IRequestBehavior<GreetingRequest, GreetingResponse>
        {
            public ValueTask<GreetingResponse> HandleAsync(
                GreetingRequest request,
                RequestHandlerDelegate<GreetingResponse> next,
                CancellationToken cancellationToken) => next(cancellationToken);
        }
        """;
}
