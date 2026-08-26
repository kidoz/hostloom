using HostLoom.Analyzers.Infrastructure;
using HostLoom.Analyzers.Usage;
using Microsoft.CodeAnalysis;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed class SyncOverAsyncAnalyzerTests
{
    [Fact]
    public async Task Reports_result_wait_and_get_result()
    {
        Diagnostic[] diagnostics = await AnalyzerTestHarness.AnalyzeAsync(
            """
            using HostLoom;

            internal sealed record GreetingResponse(string Text);
            internal sealed record GreetingRequest : IRequest<GreetingResponse>;
            internal sealed record GreetingEvent : IEvent;

            internal static class Consumer
            {
                public static GreetingResponse Result(
                    IRequestClient<GreetingRequest, GreetingResponse> client) =>
                    client.GetResponseAsync("greetings", new GreetingRequest()).Result;

                public static void Wait(IPublishEndpoint publisher) =>
                    publisher.PublishAsync("greetings", new GreetingEvent()).AsTask().Wait();

                public static GreetingResponse GetResult(
                    IRequestClient<GreetingRequest, GreetingResponse> client) =>
                    client.GetResponseAsync("greetings", new GreetingRequest())
                        .GetAwaiter()
                        .GetResult();
            }
            """,
            new SyncOverAsyncAnalyzer()
        );

        Assert.Equal(3, diagnostics.Length);
        Assert.All(
            diagnostics,
            diagnostic =>
                Assert.Equal(HostLoomDiagnosticDescriptors.SyncOverAsyncDiagnosticId, diagnostic.Id)
        );
    }

    [Fact]
    public async Task Accepts_awaited_operations()
    {
        Diagnostic[] diagnostics = await AnalyzerTestHarness.AnalyzeAsync(
            """
            using System.Threading;
            using System.Threading.Tasks;
            using HostLoom;

            internal sealed record GreetingResponse(string Text);
            internal sealed record GreetingRequest : IRequest<GreetingResponse>;

            internal static class Consumer
            {
                public static async Task<GreetingResponse> SendAsync(
                    IRequestClient<GreetingRequest, GreetingResponse> client,
                    CancellationToken cancellationToken) =>
                    await client.GetResponseAsync(
                        "greetings",
                        new GreetingRequest(),
                        cancellationToken: cancellationToken);
            }
            """,
            new SyncOverAsyncAnalyzer()
        );

        Assert.Empty(diagnostics);
    }
}
