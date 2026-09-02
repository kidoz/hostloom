using HostLoom.Pipelines;

namespace HostLoom.Locking.Pipelines;

/// <summary>Adds the distributed-lock filter to a pipe composed without a container.</summary>
public static class LockingPipeBuilderExtensions
{
    /// <summary>
    /// Runs the rest of the pipe under the lock for the key <paramref name="keySelector"/>
    /// derives, so runs for one key are serialised across every instance sharing the provider.
    /// </summary>
    public static PipeBuilder<TContext> UseDistributedLock<TContext>(
        this PipeBuilder<TContext> builder,
        IDistributedLock lockService,
        Func<TContext, string> keySelector,
        LockOptions? options = null
    )
        where TContext : class, IPipeContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(lockService);
        ArgumentNullException.ThrowIfNull(keySelector);
        return builder.Use(
            new DistributedLockFilter<TContext>(
                lockService,
                new DistributedLockFilterOptions<TContext>
                {
                    KeySelector = keySelector,
                    Lock = options,
                }
            )
        );
    }
}
