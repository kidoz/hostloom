using System.Globalization;
using HostLoom.Analyzers.Infrastructure;
using HostLoom.Analyzers.Usage;
using Microsoft.CodeAnalysis;
using Xunit;

namespace HostLoom.Analyzers.Tests;

/// <summary>
/// HLM0001 and HLM0002 over the cache and lock contracts, which the analyzers recognise by
/// metadata name rather than by the framework-assembly marker.
/// </summary>
public sealed class CachingLockingCoverageTests
{
    [Fact]
    public async Task Missing_token_reports_omitted_tokens_on_cache_and_lock_members()
    {
        Diagnostic[] diagnostics = await Analyze(
            """
            internal static class Consumer
            {
                public static async Task RunAsync(ICache cache, IDistributedLock locks, ILockHandle handle, CancellationToken stoppingToken)
                {
                    await cache.GetAsync<int>("k");
                    await cache.SetAsync("k", 1, new CacheEntryOptions(TimeSpan.FromMinutes(1)));
                    await locks.ExecuteWithLockAsync("k", _ => ValueTask.FromResult(1));
                    await locks.TryAcquireAsync("k");
                    await handle.ExtendAsync(TimeSpan.FromSeconds(30));
                }
            }
            """,
            new MissingCancellationTokenAnalyzer()
        );

        Assert.Equal(5, diagnostics.Length);
        Assert.All(
            diagnostics,
            diagnostic =>
            {
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
        );
    }

    [Fact]
    public async Task Missing_token_accepts_cache_and_lock_calls_that_pass_it()
    {
        Diagnostic[] diagnostics = await Analyze(
            """
            internal static class Consumer
            {
                public static async Task RunAsync(ICache cache, IDistributedLock locks, CancellationToken stoppingToken)
                {
                    await cache.GetAsync<int>("k", stoppingToken);
                    await cache.GetOrCreateAsync("k", token => ValueTask.FromResult(1), TimeSpan.FromMinutes(1), stoppingToken);
                    await locks.ExecuteWithLockAsync("k", _ => ValueTask.FromResult(1), cancellationToken: stoppingToken);
                }
            }
            """,
            new MissingCancellationTokenAnalyzer()
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task Missing_token_ignores_cache_calls_when_no_token_is_in_scope()
    {
        Diagnostic[] diagnostics = await Analyze(
            """
            internal static class Consumer
            {
                public static async Task RunAsync(ICache cache, IDistributedLock locks)
                {
                    await cache.GetAsync<int>("k");
                    await locks.TryAcquireAsync("k");
                }
            }
            """,
            new MissingCancellationTokenAnalyzer()
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task Sync_over_async_reports_blocking_on_cache_and_lock_members()
    {
        Diagnostic[] diagnostics = await Analyze(
            """
            internal static class Consumer
            {
                public static int Result(ICache cache) => cache.GetAsync<int>("k").Result;

                public static void Wait(IDistributedLock locks) =>
                    locks.ExecuteWithLockAsync("k", _ => ValueTask.CompletedTask).AsTask().Wait();

                public static bool GetResult(ILockHandle handle) =>
                    handle.ExtendAsync(TimeSpan.FromSeconds(1)).GetAwaiter().GetResult();
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
    public async Task Sync_over_async_accepts_awaited_cache_and_lock_members()
    {
        Diagnostic[] diagnostics = await Analyze(
            """
            internal static class Consumer
            {
                public static async Task<int> RunAsync(ICache cache, IDistributedLock locks, CancellationToken cancellationToken)
                {
                    int cached = await cache.GetAsync<int>("k", cancellationToken);
                    return await locks.ExecuteWithLockAsync("k", _ => ValueTask.FromResult(cached), cancellationToken: cancellationToken);
                }
            }
            """,
            new SyncOverAsyncAnalyzer()
        );

        Assert.Empty(diagnostics);
    }

    private static Task<Diagnostic[]> Analyze(
        string consumer,
        Microsoft.CodeAnalysis.Diagnostics.DiagnosticAnalyzer analyzer
    ) => AnalyzerTestHarness.AnalyzeAsync(CachingLockingContracts.Source + consumer, analyzer);
}
