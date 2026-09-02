using System.Globalization;
using HostLoom.Analyzers.Infrastructure;
using HostLoom.Analyzers.Usage;
using Microsoft.CodeAnalysis;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed class SensitiveCacheKeyAnalyzerTests
{
    [Fact]
    public async Task Reports_an_interpolated_refresh_token()
    {
        Diagnostic diagnostic = Assert.Single(
            await Analyze(
                """
                internal static class Consumer
                {
                    public static async Task<int> LoadAsync(ICache cache, string refreshToken, CancellationToken cancellationToken) =>
                        await cache.GetOrCreateAsync(
                            $"session:{refreshToken}",
                            token => ValueTask.FromResult(1),
                            TimeSpan.FromMinutes(5),
                            cancellationToken);
                }
                """
            )
        );

        Assert.Equal(HostLoomDiagnosticDescriptors.SensitiveCacheKeyDiagnosticId, diagnostic.Id);
        string message = diagnostic.GetMessage(CultureInfo.InvariantCulture);
        Assert.Contains("refreshToken", message, StringComparison.Ordinal);
        Assert.Contains("CacheKey.FromSensitive", message, StringComparison.Ordinal);
        Assert.Contains("GetOrCreateAsync", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Accepts_a_hole_wrapped_in_FromSensitive()
    {
        Diagnostic[] diagnostics = await Analyze(
            """
            internal static class Consumer
            {
                public static async Task<int> LoadAsync(ICache cache, string refreshToken, CancellationToken cancellationToken) =>
                    await cache.GetOrCreateAsync(
                        $"session:{CacheKey.FromSensitive(refreshToken)}",
                        token => ValueTask.FromResult(1),
                        TimeSpan.FromMinutes(5),
                        cancellationToken);

                public static ValueTask RemoveAsync(ICache cache, string apiKey, CancellationToken cancellationToken) =>
                    cache.RemoveAsync(CacheKey.FromSensitive(apiKey), cancellationToken);
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task Reports_a_property_named_like_a_credential()
    {
        Diagnostic diagnostic = Assert.Single(
            await Analyze(
                """
                internal sealed record User(string Name, string ApiKey);

                internal static class Consumer
                {
                    public static ValueTask<int> LoadAsync(ICache cache, User user, CancellationToken cancellationToken) =>
                        cache.GetAsync<int>($"user:{user.ApiKey}", cancellationToken);
                }
                """
            )
        );

        Assert.Contains(
            "ApiKey",
            diagnostic.GetMessage(CultureInfo.InvariantCulture),
            StringComparison.Ordinal
        );
    }

    [Fact]
    public async Task Ignores_names_that_are_not_credentials()
    {
        Diagnostic[] diagnostics = await Analyze(
            """
            internal static class Consumer
            {
                private static readonly string Tenant = "eu";

                public static ValueTask<int> LoadAsync(ICache cache, string region, int page, CancellationToken cancellationToken) =>
                    cache.GetAsync<int>($"catalog:{region}:{page}:{Tenant}", cancellationToken);

                public static ValueTask<int> LiteralAsync(ICache cache, CancellationToken cancellationToken) =>
                    cache.GetAsync<int>("token-bucket:global", cancellationToken);
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task Reports_a_concatenated_password_and_a_formatted_secret()
    {
        Diagnostic[] diagnostics = await Analyze(
            """
            internal static class Consumer
            {
                public static ValueTask ConcatAsync(ICache cache, string password, CancellationToken cancellationToken) =>
                    cache.RemoveAsync("k:" + password, cancellationToken);

                public static ValueTask FormatAsync(ICache cache, string clientSecret, CancellationToken cancellationToken) =>
                    cache.RemoveAsync(string.Format("k:{0}", clientSecret), cancellationToken);

                public static ValueTask JoinAsync(ICache cache, string tenant, string secret, CancellationToken cancellationToken) =>
                    cache.RemoveAsync(string.Join(":", tenant, secret), cancellationToken);
            }
            """
        );

        Assert.Equal(3, diagnostics.Length);
        Assert.All(
            diagnostics,
            diagnostic =>
                Assert.Equal(
                    HostLoomDiagnosticDescriptors.SensitiveCacheKeyDiagnosticId,
                    diagnostic.Id
                )
        );
    }

    [Fact]
    public async Task Reports_a_lock_key_and_names_the_lock_helper()
    {
        Diagnostic diagnostic = Assert.Single(
            await Analyze(
                """
                internal static class Consumer
                {
                    public static ValueTask<int> RefreshAsync(IDistributedLock locks, string refreshToken, CancellationToken cancellationToken) =>
                        locks.ExecuteWithLockAsync(
                            $"sso:{refreshToken}",
                            _ => ValueTask.FromResult(1),
                            cancellationToken: cancellationToken);
                }
                """
            )
        );

        string message = diagnostic.GetMessage(CultureInfo.InvariantCulture);
        Assert.Contains("LockKey.FromSensitive", message, StringComparison.Ordinal);
        Assert.Contains("ExecuteWithLockAsync", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Reports_each_sensitive_element_of_a_bulk_removal()
    {
        Diagnostic[] diagnostics = await Analyze(
            """
            internal static class Consumer
            {
                public static ValueTask RemoveAsync(ICache cache, string token, string region, CancellationToken cancellationToken) =>
                    cache.RemoveAsync([$"a:{token}", $"b:{region}", "c:" + token], cancellationToken);
            }
            """
        );

        Assert.Equal(2, diagnostics.Length);
    }

    private static Task<Diagnostic[]> Analyze(string consumer) =>
        AnalyzerTestHarness.AnalyzeAsync(
            CachingLockingContracts.Source + consumer,
            new SensitiveCacheKeyAnalyzer()
        );
}
