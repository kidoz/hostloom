namespace HostLoom.Pipelines;

internal sealed class RetryFilter<TContext> : IFilter<TContext> where TContext : class, IPipeContext
{
    private readonly RetryPolicy _policy;
    private readonly Func<Exception, bool> _shouldRetry;
    private readonly TimeProvider _timeProvider;

    public RetryFilter(RetryPolicy policy, Func<Exception, bool>? shouldRetry, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _policy = policy;
        _shouldRetry = shouldRetry ?? (static _ => true);
        _timeProvider = timeProvider;
    }

    public async ValueTask SendAsync(TContext context, IPipe<TContext> next)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await next.SendAsync(context).ConfigureAwait(false);
                return;
            }
            catch (Exception exception) when (CanRetry(exception, attempt, context))
            {
                var number = attempt + 1;
                context.AddOrUpdatePayload(() => new RetryAttempt(number), _ => new RetryAttempt(number));

                var delay = _policy.GetDelay(number);
                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, _timeProvider, context.CancellationToken).ConfigureAwait(false);
                }
            }
        }
    }

    public void Probe(IProbeContext context)
    {
        var scope = context.CreateScope("retry");
        scope.Set("retryLimit", _policy.RetryLimit);
        scope.Set("policy", _policy.Description);
    }

    // Cancellation is the caller's decision, never a downstream fault, so it is never retried.
    private bool CanRetry(Exception exception, int attempt, TContext context) =>
        attempt < _policy.RetryLimit
        && exception is not OperationCanceledException
        && !context.CancellationToken.IsCancellationRequested
        && _shouldRetry(exception);
}
