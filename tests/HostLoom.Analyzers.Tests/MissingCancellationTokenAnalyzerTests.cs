using System.Globalization;
using HostLoom.Analyzers.Infrastructure;
using HostLoom.Analyzers.Usage;
using Microsoft.CodeAnalysis;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed class MissingCancellationTokenAnalyzerTests
{
    [Fact]
    public async Task Reports_an_omitted_method_cancellation_token()
    {
        Diagnostic diagnostic = Assert.Single(
            await AnalyzerTestHarness.AnalyzeAsync(
                ConsumerContracts
                    + """

                    internal static class Consumer
                    {
                        public static async Task SendAsync(
                            IRequestClient<GreetingRequest, GreetingResponse> client,
                            CancellationToken stoppingToken)
                        {
                            await client.GetResponseAsync("greetings", new GreetingRequest());
                        }
                    }
                    """,
                new MissingCancellationTokenAnalyzer()
            )
        );

        Assert.Equal(
            HostLoomDiagnosticDescriptors.MissingCancellationTokenDiagnosticId,
            diagnostic.Id
        );
        Assert.Contains(
            "stoppingToken",
            diagnostic.GetMessage(CultureInfo.InvariantCulture),
            StringComparison.Ordinal
        );
    }

    [Fact]
    public async Task Recognizes_a_pipeline_context_cancellation_token()
    {
        Diagnostic diagnostic = Assert.Single(
            await AnalyzerTestHarness.AnalyzeAsync(
                ConsumerContracts
                    + """

                    internal static class Consumer
                    {
                        public static async Task PublishAsync(
                            IPublishEndpoint publisher,
                            PipeContext context)
                        {
                            await publisher.PublishAsync("greetings", new GreetingEvent());
                        }
                    }
                    """,
                new MissingCancellationTokenAnalyzer()
            )
        );

        Assert.Equal(
            HostLoomDiagnosticDescriptors.MissingCancellationTokenDiagnosticId,
            diagnostic.Id
        );
        Assert.Contains(
            "context.CancellationToken",
            diagnostic.GetMessage(CultureInfo.InvariantCulture),
            StringComparison.Ordinal
        );
    }

    [Fact]
    public async Task Accepts_an_explicit_cancellation_token()
    {
        Diagnostic[] diagnostics = await AnalyzerTestHarness.AnalyzeAsync(
            ConsumerContracts
                + """

                internal static class Consumer
                {
                    public static async Task SendAsync(
                        IRequestClient<GreetingRequest, GreetingResponse> client,
                        CancellationToken stoppingToken)
                    {
                        await client.GetResponseAsync(
                            "greetings",
                            new GreetingRequest(),
                            cancellationToken: stoppingToken);
                    }
                }
                """,
            new MissingCancellationTokenAnalyzer()
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task Ignores_calls_when_no_token_is_available()
    {
        Diagnostic[] diagnostics = await AnalyzerTestHarness.AnalyzeAsync(
            ConsumerContracts
                + """

                internal static class Consumer
                {
                    public static async Task SendAsync(
                        IRequestClient<GreetingRequest, GreetingResponse> client)
                    {
                        await client.GetResponseAsync("greetings", new GreetingRequest());
                    }
                }
                """,
            new MissingCancellationTokenAnalyzer()
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task Ignores_an_outer_token_that_a_static_lambda_cannot_capture()
    {
        Diagnostic[] diagnostics = await AnalyzerTestHarness.AnalyzeAsync(
            ConsumerContracts
                + """

                internal static class Consumer
                {
                    public static Func<
                        IRequestClient<GreetingRequest, GreetingResponse>,
                        Task> Create(CancellationToken stoppingToken) =>
                        static async client =>
                            await client.GetResponseAsync("greetings", new GreetingRequest());
                }
                """,
            new MissingCancellationTokenAnalyzer()
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task Ignores_consumer_assemblies_whose_names_start_with_HostLoom()
    {
        Diagnostic[] diagnostics = await AnalyzerTestHarness.AnalyzeAsync(
            """
            using System.Threading;
            using System.Threading.Tasks;

            internal static class OrderOperations
            {
                public static Task SaveAsync(
                    string order,
                    CancellationToken cancellationToken = default) => Task.CompletedTask;
            }

            internal static class Consumer
            {
                public static async Task SaveAsync(
                    string order,
                    CancellationToken stoppingToken)
                {
                    await OrderOperations.SaveAsync(order);
                }
            }
            """,
            new MissingCancellationTokenAnalyzer()
        );

        Assert.Empty(diagnostics);
    }

    private const string ConsumerContracts = """
        using System;
        using System.Threading;
        using System.Threading.Tasks;
        using HostLoom;
        using HostLoom.Pipelines;

        internal sealed record GreetingResponse(string Text);
        internal sealed record GreetingRequest : IRequest<GreetingResponse>;
        internal sealed record GreetingEvent : IEvent;
        """;
}
