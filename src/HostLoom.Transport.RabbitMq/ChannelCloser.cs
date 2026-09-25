using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace HostLoom.Transport.RabbitMq;

/// <summary>
/// Closes channels off the caller's path and tracks the closes still running.
/// </summary>
/// <remarks>
/// Closing a channel waits for the broker's close-ok, up to the client library's continuation
/// timeout of 20 seconds, and the broker that made a channel worth closing is the one least likely
/// to answer promptly. So no caller waits for a close: each runs as a task that logs its own
/// failure, and disposal waits for the closes still running only for a bounded time.
/// </remarks>
internal sealed class ChannelCloser
{
    private readonly string _kind;
    private readonly ILogger _logger;
    private readonly Lock _gate = new();
    private TaskCompletionSource _closeFinished = NewSignal();
    private int _closing;

    /// <param name="kind">What the channels are for, as the cleanup failure log names them.</param>
    /// <param name="logger">Receives the failures of closes nobody waits for.</param>
    public ChannelCloser(string kind, ILogger logger)
    {
        _kind = kind;
        _logger = logger;
    }

    /// <summary>Channels whose close was started and has not finished.</summary>
    public int ClosingCount => Volatile.Read(ref _closing);

    /// <summary>
    /// Starts closing <paramref name="channel"/> on the thread pool and returns at once. The task
    /// never faults: it completes with the close's failure, or, when <paramref name="logFailure"/>
    /// is set, with none once the failure is logged.
    /// </summary>
    public Task<Exception?> Start(IChannel channel, bool logFailure = true)
    {
        lock (_gate)
        {
            _closing++;
        }

        return Task.Run(() => CloseAsync(channel, logFailure));
    }

    /// <summary>Waits until fewer than <paramref name="limit"/> closes are running.</summary>
    public async ValueTask WaitUntilFewerThanAsync(int limit, CancellationToken cancellationToken)
    {
        while (true)
        {
            Task finished;
            lock (_gate)
            {
                if (_closing < limit)
                {
                    return;
                }

                finished = _closeFinished.Task;
            }

            await finished.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Whether every close finished before <paramref name="deadline"/> was cancelled.</summary>
    public async ValueTask<bool> WaitForAllAsync(CancellationToken deadline)
    {
        try
        {
            await WaitUntilFewerThanAsync(1, deadline).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested)
        {
            return false;
        }
    }

    private async Task<Exception?> CloseAsync(IChannel channel, bool logFailure)
    {
        try
        {
            await channel.DisposeAsync().ConfigureAwait(false);
            return null;
        }
        catch (Exception exception)
        {
            if (!logFailure)
            {
                return exception;
            }

            _logger.LogWarning(
                new EventId(1408, "RabbitMqChannelCloseFailed"),
                exception,
                "RabbitMQ {Kind} channel cleanup failed.",
                _kind
            );
            return null;
        }
        finally
        {
            TaskCompletionSource finished;
            lock (_gate)
            {
                _closing--;
                finished = _closeFinished;
                _closeFinished = NewSignal();
            }

            finished.TrySetResult();
        }
    }

    private static TaskCompletionSource NewSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}
