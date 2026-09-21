using System.Globalization;

namespace HostLoom.Redis.Internal;

/// <summary>
/// The one bounded <c>PING</c> behind every Redis health contract. The cache store and the lock
/// provider answer different public contracts but ask the same question, so the timeout, the
/// linked cancellation, the latency wording, and the sanitized failure text live here once and
/// cannot drift apart.
/// </summary>
internal static class RedisHealthProbe
{
    /// <summary>
    /// Pings Redis, bounded by <see cref="RedisOptions.HealthTimeout"/>. Caller cancellation stays
    /// cancellation and propagates; anything else, the timeout included, becomes an unreachable
    /// result rather than an exception, because a readiness endpoint asks a question rather than
    /// performing an operation.
    /// </summary>
    /// <remarks>
    /// The description is meant for a readiness endpoint, so it names no endpoint, host, machine,
    /// or process; <see cref="RedisConnection.Describe"/> keeps those for logs.
    /// </remarks>
    public static async ValueTask<RedisHealthResult> CheckAsync(
        RedisConnection connection,
        CancellationToken cancellationToken
    )
    {
        var timeout = connection.Options.HealthTimeout;
        try
        {
            using var bounded = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            bounded.CancelAfter(timeout);
            var db = await connection.GetDatabaseAsync(bounded.Token).ConfigureAwait(false);
            var latency = await db.PingAsync().WaitAsync(bounded.Token).ConfigureAwait(false);
            return new RedisHealthResult(
                true,
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"Redis reachable; PING answered in {latency.TotalMilliseconds:F1} ms on database {connection.Options.DatabaseIndex}."
                )
            );
        }
        catch (Exception exception)
            when (!RedisFailures.IsCallerCancellation(exception, cancellationToken))
        {
            return new RedisHealthResult(
                false,
                $"Redis unreachable; PING did not answer within {timeout} ({exception.GetType().Name})."
            );
        }
    }
}

/// <summary>What the probe learned, before a package maps it to its own health contract.</summary>
internal readonly record struct RedisHealthResult(bool Reachable, string Description);
