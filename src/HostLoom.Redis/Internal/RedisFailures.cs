using HostLoom.Caching;
using HostLoom.Locking;
using StackExchange.Redis;

namespace HostLoom.Redis.Internal;

/// <summary>Maps StackExchange.Redis exceptions to the backend-neutral failure shapes the kernels understand.</summary>
internal static class RedisFailures
{
    public static CacheStoreException ToCacheStoreException(
        Exception exception,
        string operation
    ) =>
        new(
            ClassifyCache(exception),
            $"Redis {operation} failed: {exception.GetType().Name}.",
            exception
        );

    public static LockProviderException ToLockProviderException(
        Exception exception,
        string operation
    ) =>
        new(
            ClassifyLock(exception),
            $"Redis {operation} failed: {exception.GetType().Name}.",
            exception
        );

    public static bool IsCallerCancellation(Exception exception, CancellationToken token) =>
        exception is OperationCanceledException && token.IsCancellationRequested;

    internal static long Milliseconds(TimeSpan duration)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(duration, TimeSpan.Zero);
        // Round upward: a positive sub-millisecond lease must never become an invalid PX 0.
        return checked((long)Math.Ceiling(duration.TotalMilliseconds));
    }

    private static CacheFailureKind ClassifyCache(Exception exception) =>
        exception switch
        {
            RedisConnectionException or ObjectDisposedException => CacheFailureKind.Unavailable,
            RedisTimeoutException or TimeoutException => CacheFailureKind.Timeout,
            _ => CacheFailureKind.Other,
        };

    private static LockFailureKind ClassifyLock(Exception exception) =>
        exception switch
        {
            RedisConnectionException or ObjectDisposedException => LockFailureKind.Unavailable,
            RedisTimeoutException or TimeoutException => LockFailureKind.Timeout,
            _ => LockFailureKind.Other,
        };
}
