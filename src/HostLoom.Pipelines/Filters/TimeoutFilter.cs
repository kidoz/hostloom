namespace HostLoom.Pipelines;

internal sealed class TimeoutFilter<TContext> : IFilter<TContext>
    where TContext : PipeContext
{
    private readonly TimeSpan _timeout;
    private readonly TimeProvider _timeProvider;

    public TimeoutFilter(TimeSpan timeout, TimeProvider timeProvider)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _timeout = timeout;
        _timeProvider = timeProvider;
    }

    public async ValueTask SendAsync(TContext context, IPipe<TContext> next)
    {
        var callerToken = context.CancellationToken;
        using var timeoutSource = new CancellationTokenSource(_timeout, _timeProvider);
        using var linkedSource = CancellationTokenSource.CreateLinkedTokenSource(
            callerToken,
            timeoutSource.Token
        );
        using var swap = context.SwapCancellationToken(linkedSource.Token);
        try
        {
            await next.SendAsync(context).ConfigureAwait(false);
        }
        // Caller cancellation stays cancellation even when it races the timer.
        catch (OperationCanceledException exception)
            when (timeoutSource.IsCancellationRequested && !callerToken.IsCancellationRequested)
        {
            throw new PipelineTimeoutException(_timeout, exception);
        }

        // Downstream returning normally does not mean it finished in time. Filters between stages
        // are checked by the pipe itself, but a terminal filter has nothing after it to observe the
        // token, so without this check an over-budget run reports success.
        if (timeoutSource.IsCancellationRequested && !callerToken.IsCancellationRequested)
        {
            throw new PipelineTimeoutException(_timeout, null);
        }
    }

    public void Probe(IProbeContext context)
    {
        var scope = context.CreateScope("timeout");
        scope.Set("timeout", _timeout);
    }
}
