namespace HostLoom.Analyzers.Tests;

/// <summary>
/// Stubs of the caching and locking consumer contracts, declared in the analysed source under
/// their real namespaces. The analyzers recognise these types by metadata name, so the tests
/// need neither package; what they prove is exactly the name-based recognition.
/// </summary>
internal static class CachingLockingContracts
{
    public const string Source = """
        using System;
        using System.Collections.Generic;
        using System.Threading;
        using System.Threading.Tasks;
        using HostLoom.Caching;
        using HostLoom.Locking;

        namespace HostLoom.Caching
        {
            public sealed class CacheEntryOptions
            {
                public CacheEntryOptions(TimeSpan expiration) { }
            }

            public readonly record struct CacheLookup<T>(bool Found, T? Value);

            public readonly record struct CacheWarmupProgress(int Written, int Total);

            public static class CacheKey
            {
                public static string FromSensitive(string value) => value;

                public static string Versioned(string key, string version) => key;
            }

            public interface ICache
            {
                ValueTask<T?> GetOrCreateAsync<T>(string key, Func<CancellationToken, ValueTask<T>> factory, CacheEntryOptions options, CancellationToken cancellationToken = default);
                ValueTask<T?> GetOrCreateAsync<T>(string key, Func<CancellationToken, ValueTask<T>> factory, TimeSpan expiration, CancellationToken cancellationToken = default);
                ValueTask<T?> GetOrCreateAsync<TState, T>(string key, TState state, Func<TState, CancellationToken, ValueTask<T>> factory, CacheEntryOptions options, CancellationToken cancellationToken = default);
                ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default);
                ValueTask<CacheLookup<T>> TryGetAsync<T>(string key, CancellationToken cancellationToken = default);
                ValueTask SetAsync<T>(string key, T value, CacheEntryOptions options, CancellationToken cancellationToken = default);
                ValueTask<bool> SetIfAbsentAsync<T>(string key, T value, CacheEntryOptions options, CancellationToken cancellationToken = default);
                ValueTask<bool> SetIfAbsentAsync<T>(string key, T value, TimeSpan expiration, CancellationToken cancellationToken = default);
                ValueTask RemoveAsync(string key, CancellationToken cancellationToken = default);
                ValueTask RemoveAsync(IEnumerable<string> keys, CancellationToken cancellationToken = default);
                ValueTask RemoveByTagAsync(string tag, CancellationToken cancellationToken = default);
                ValueTask<IReadOnlyDictionary<string, T>> GetManyAsync<T>(IEnumerable<string> keys, CancellationToken cancellationToken = default);
                ValueTask WarmupAsync<T>(IReadOnlyDictionary<string, T> entries, TimeSpan expiration, IProgress<CacheWarmupProgress>? progress = null, CancellationToken cancellationToken = default);
            }
        }

        namespace HostLoom.Locking
        {
            public sealed class LockOptions
            {
                public TimeSpan? Lease { get; set; }
            }

            public static class LockKey
            {
                public static string FromSensitive(string value) => value;
            }

            public interface ILockHandle : IAsyncDisposable
            {
                string Key { get; }
                bool IsHeld { get; }
                ValueTask<bool> ExtendAsync(TimeSpan lease, CancellationToken cancellationToken = default);
            }

            public interface IDistributedLock
            {
                ValueTask<T> ExecuteWithLockAsync<T>(string key, Func<CancellationToken, ValueTask<T>> action, LockOptions? options = null, CancellationToken cancellationToken = default);
                ValueTask ExecuteWithLockAsync(string key, Func<CancellationToken, ValueTask> action, LockOptions? options = null, CancellationToken cancellationToken = default);
                ValueTask<ILockHandle?> TryAcquireAsync(string key, LockOptions? options = null, CancellationToken cancellationToken = default);
            }
        }

        """;
}
