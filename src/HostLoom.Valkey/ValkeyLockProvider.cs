using HostLoom.Locking;
using HostLoom.Valkey.Internal;
using ValkeyDotNet;

namespace HostLoom.Valkey;

/// <summary>
/// Standalone coordination leases using SET NX PX and atomic owner-checked Lua release/extension.
/// Failover can lose a lease; this provider supplies neither fencing tokens nor consensus locks.
/// </summary>
public sealed class ValkeyLockProvider : ILockProvider, ILockProviderHealthProbe
{
    private static readonly ValkeyScript Release = new(
        """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        end
        return 0
        """
    );
    private static readonly ValkeyScript Extend = new(
        """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('PEXPIRE', KEYS[1], ARGV[2])
        end
        return 0
        """
    );
    private readonly ValkeyConnection _connection;

    /// <summary>Borrows the shared command connection; the caller retains ownership.</summary>
    public ValkeyLockProvider(ValkeyConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        _connection = connection;
    }

    /// <inheritdoc />
    public async ValueTask<bool> TryAcquireAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        var milliseconds = ValkeyFailures.Milliseconds(lease);
        try
        {
            var reply = await _connection
                .ExecuteAsync(
                    new ValkeyCommand("SET", key, owner, "NX", "PX", milliseconds),
                    cancellationToken
                )
                .ConfigureAwait(false);
            return !reply.IsNull && string.Equals(reply.AsString(), "OK", StringComparison.Ordinal);
        }
        catch (Exception exception)
            when (!ValkeyFailures.IsCallerCancellation(exception, cancellationToken))
        {
            throw ValkeyFailures.Lock(exception, "acquire");
        }
    }

    /// <inheritdoc />
    public ValueTask<bool> ReleaseAsync(
        string key,
        string owner,
        CancellationToken cancellationToken = default
    ) => ExecuteAsync(Release, key, [owner], cancellationToken);

    /// <inheritdoc />
    public ValueTask<bool> ExtendAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    ) => ExecuteAsync(Extend, key, [owner, ValkeyFailures.Milliseconds(lease)], cancellationToken);

    private async ValueTask<bool> ExecuteAsync(
        ValkeyScript script,
        string key,
        ValkeyArgument[] arguments,
        CancellationToken token
    )
    {
        try
        {
            var reply = await _connection
                .ScriptAsync(script, [key], arguments, token)
                .ConfigureAwait(false);
            return reply.AsInt64() == 1;
        }
        catch (Exception exception) when (!ValkeyFailures.IsCallerCancellation(exception, token))
        {
            throw ValkeyFailures.Lock(exception, "owner-checked script");
        }
    }

    /// <inheritdoc />
    public async ValueTask<LockProviderHealth> CheckHealthAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _connection.PingAsync(cancellationToken).ConfigureAwait(false);
            return LockProviderHealth.Healthy("Valkey answered PING.");
        }
        catch (Exception exception)
            when (!ValkeyFailures.IsCallerCancellation(exception, cancellationToken))
        {
            return LockProviderHealth.Unhealthy(
                $"Valkey PING failed ({exception.GetType().Name})."
            );
        }
    }
}
