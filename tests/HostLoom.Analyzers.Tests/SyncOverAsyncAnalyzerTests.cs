using HostLoom.Analyzers.Infrastructure;
using HostLoom.Analyzers.Usage;
using Microsoft.CodeAnalysis;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed class SyncOverAsyncAnalyzerTests
{
    [Fact]
    public async Task Reports_configured_awaitables_and_local_task_origins()
    {
        var diagnostics = await AnalyzerTestHarness.AnalyzeAsync(
            """
            using HostLoom;
            internal sealed record GreetingRequest : IRequest<string>;
            internal static class Consumer
            {
                public static void Run(IRequestClient<GreetingRequest, string> client)
                {
                    var first = client.GetResponseAsync("catalog", new GreetingRequest());
                    first.AsTask().Wait();
                    var second = client.GetResponseAsync("catalog", new GreetingRequest()).ConfigureAwait(false);
                    var awaiter = second.GetAwaiter();
                    _ = awaiter.GetResult();
                }
            }
            """,
            new SyncOverAsyncAnalyzer()
        );
        Assert.Equal(2, diagnostics.Length);
    }

    [Fact]
    public async Task A_reassigned_local_does_not_have_a_provable_HostLoom_origin()
    {
        var diagnostics = await AnalyzerTestHarness.AnalyzeAsync(
            """
            using System.Threading.Tasks;
            using HostLoom;
            internal sealed record GreetingRequest : IRequest<string>;
            internal static class Consumer
            {
                public static void Run(IRequestClient<GreetingRequest, string> client)
                {
                    var pending = client.GetResponseAsync("catalog", new GreetingRequest());
                    pending = ValueTask.FromResult("books");
                    _ = pending.Result;
                }
            }
            """,
            new SyncOverAsyncAnalyzer()
        );
        Assert.Empty(diagnostics);
    }

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

    [Fact]
    public async Task Ignores_consumer_assemblies_whose_names_start_with_HostLoom()
    {
        Diagnostic[] diagnostics = await AnalyzerTestHarness.AnalyzeAsync(
            """
            using System.Threading.Tasks;

            internal static class OrderOperations
            {
                public static Task<int> LoadAsync() => Task.FromResult(42);
            }

            internal static class Consumer
            {
                public static int Load() => OrderOperations.LoadAsync().Result;
            }
            """,
            new SyncOverAsyncAnalyzer()
        );

        Assert.Empty(diagnostics);
    }
}
