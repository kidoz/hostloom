using System.Globalization;
using HostLoom.Analyzers.Infrastructure;
using HostLoom.Analyzers.Usage;
using Microsoft.CodeAnalysis;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed class FactoryIgnoresCancellationTokenAnalyzerTests
{
    [Fact]
    public async Task Reports_a_named_token_the_lambda_never_uses()
    {
        Diagnostic diagnostic = Assert.Single(
            await Analyze(
                """
                internal static class Consumer
                {
                    public static ValueTask<int> LoadAsync(ICache cache, CancellationToken cancellationToken) =>
                        cache.GetOrCreateAsync("k", token => ValueTask.FromResult(1), TimeSpan.FromMinutes(1), cancellationToken);
                }
                """
            )
        );

        Assert.Equal(
            HostLoomDiagnosticDescriptors.FactoryIgnoresCancellationTokenDiagnosticId,
            diagnostic.Id
        );
        string message = diagnostic.GetMessage(CultureInfo.InvariantCulture);
        Assert.Contains("'token'", message, StringComparison.Ordinal);
        Assert.Contains("GetOrCreateAsync", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Accepts_a_discarded_token()
    {
        Diagnostic[] diagnostics = await Analyze(
            """
            internal static class Consumer
            {
                public static ValueTask<int> LoadAsync(ICache cache, CancellationToken cancellationToken) =>
                    cache.GetOrCreateAsync("k", _ => ValueTask.FromResult(1), TimeSpan.FromMinutes(1), cancellationToken);

                public static ValueTask<int> LoadWithStateAsync(ICache cache, CancellationToken cancellationToken) =>
                    cache.GetOrCreateAsync("k", 7, static (state, _) => ValueTask.FromResult(state), new CacheEntryOptions(TimeSpan.FromMinutes(1)), cancellationToken);
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task Accepts_a_token_the_lambda_forwards()
    {
        Diagnostic[] diagnostics = await Analyze(
            """
            internal static class Consumer
            {
                public static ValueTask<int> LoadAsync(ICache cache, CancellationToken cancellationToken) =>
                    cache.GetOrCreateAsync("k", token => LoadAsync(token), TimeSpan.FromMinutes(1), cancellationToken);

                private static ValueTask<int> LoadAsync(CancellationToken token) => ValueTask.FromResult(1);
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task Reports_the_state_overload_and_accepts_it_when_forwarded()
    {
        Diagnostic[] diagnostics = await Analyze(
            """
            internal static class Consumer
            {
                public static ValueTask<int> IgnoresAsync(ICache cache, CancellationToken cancellationToken) =>
                    cache.GetOrCreateAsync("k", 7, (state, token) => Load(state), new CacheEntryOptions(TimeSpan.FromMinutes(1)), cancellationToken);

                public static ValueTask<int> ForwardsAsync(ICache cache, CancellationToken cancellationToken) =>
                    cache.GetOrCreateAsync("k", 7, (state, token) => Load(state, token), new CacheEntryOptions(TimeSpan.FromMinutes(1)), cancellationToken);

                private static ValueTask<int> Load(int state) => ValueTask.FromResult(state);

                private static ValueTask<int> Load(int state, CancellationToken token) => ValueTask.FromResult(state);
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Contains(
            "'token'",
            diagnostic.GetMessage(CultureInfo.InvariantCulture),
            StringComparison.Ordinal
        );
    }

    [Fact]
    public async Task Accepts_a_token_used_only_inside_a_nested_lambda()
    {
        Diagnostic[] diagnostics = await Analyze(
            """
            internal static class Consumer
            {
                public static ValueTask<int> LoadAsync(ICache cache, CancellationToken cancellationToken) =>
                    cache.GetOrCreateAsync(
                        "k",
                        static async token =>
                        {
                            Func<Task<int>> inner = () => Task.Run(() => 1, token);
                            return await inner();
                        },
                        TimeSpan.FromMinutes(1),
                        cancellationToken);
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task Reports_a_method_group_whose_target_ignores_the_token()
    {
        Diagnostic[] diagnostics = await Analyze(
            """
            internal static class Consumer
            {
                public static ValueTask<int> IgnoresAsync(ICache cache, CancellationToken cancellationToken) =>
                    cache.GetOrCreateAsync("k", Ignoring, TimeSpan.FromMinutes(1), cancellationToken);

                public static ValueTask<int> ForwardsAsync(ICache cache, CancellationToken cancellationToken) =>
                    cache.GetOrCreateAsync("k", Forwarding, TimeSpan.FromMinutes(1), cancellationToken);

                private static ValueTask<int> Ignoring(CancellationToken token) => ValueTask.FromResult(1);

                private static async ValueTask<int> Forwarding(CancellationToken token)
                {
                    await Task.Delay(1, token);
                    return 1;
                }
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Contains(
            "'token'",
            diagnostic.GetMessage(CultureInfo.InvariantCulture),
            StringComparison.Ordinal
        );
    }

    private static Task<Diagnostic[]> Analyze(string consumer) =>
        AnalyzerTestHarness.AnalyzeAsync(
            CachingLockingContracts.Source + consumer,
            new FactoryIgnoresCancellationTokenAnalyzer()
        );
}
