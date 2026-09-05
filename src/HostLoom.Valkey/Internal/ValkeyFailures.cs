using HostLoom.Caching;
using HostLoom.Locking;
using ValkeyDotNet;

namespace HostLoom.Valkey.Internal;

internal static class ValkeyFailures
{
    internal static bool IsCallerCancellation(Exception exception, CancellationToken token) =>
        exception is OperationCanceledException && token.IsCancellationRequested;

    internal static CacheFailureKind Classify(Exception exception) =>
        exception switch
        {
            TimeoutException => CacheFailureKind.Timeout,
            ValkeyConnectionException or ValkeyCapacityException or ObjectDisposedException =>
                CacheFailureKind.Unavailable,
            _ => CacheFailureKind.Other,
        };

    internal static CacheStoreException Cache(Exception exception, string operation) =>
        new(
            Classify(exception),
            $"Valkey {operation} failed ({exception.GetType().Name}).",
            exception
        );

    internal static LockProviderException Lock(Exception exception, string operation) =>
        new(
            Classify(exception) switch
            {
                CacheFailureKind.Timeout => LockFailureKind.Timeout,
                CacheFailureKind.Unavailable => LockFailureKind.Unavailable,
                _ => LockFailureKind.Other,
            },
            $"Valkey {operation} failed ({exception.GetType().Name}).",
            exception
        );

    internal static long Milliseconds(TimeSpan duration)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(duration, TimeSpan.Zero);
        // Round upward: a positive sub-millisecond lease must never become an invalid PX 0.
        return checked((long)Math.Ceiling(duration.TotalMilliseconds));
    }
}
