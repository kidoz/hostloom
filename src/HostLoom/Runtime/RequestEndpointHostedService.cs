using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HostLoom;

internal sealed class RequestEndpointHostedService(
    HostLoomConfiguration configuration,
    IRequestBroker broker,
    MessageDispatcher dispatcher,
    EventDispatcher eventDispatcher,
    EndpointRuntimeState state,
    ILogger<RequestEndpointHostedService>? logger = null
) : IHostedService, IAsyncDisposable
{
    private readonly List<IAsyncDisposable> _subscriptions = [];
    private readonly ILogger _logger = logger ?? NullLogger<RequestEndpointHostedService>.Instance;
    private readonly Lock _gate = new();
    private Task? _stopping;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (configuration.Endpoints.Count == 0 && configuration.Subscriptions.Count == 0)
        {
            return;
        }

        try
        {
            foreach (var endpoint in configuration.Endpoints)
            {
                // The endpoint is bound into the handler so the dispatcher only considers
                // registrations belonging to the endpoint that received the frame.
                var address = endpoint;
                var subscription = await broker
                    .ListenAsync(
                        address,
                        (frame, token) => dispatcher.DispatchAsync(address, frame, token),
                        cancellationToken
                    )
                    .ConfigureAwait(false);
                _subscriptions.Add(subscription);
            }

            if (configuration.Subscriptions.Count > 0)
            {
                if (broker is not IEventBroker events)
                {
                    throw new NotSupportedException(
                        $"The configured transport '{broker.GetType().Name}' supports request/response only, "
                            + $"but {configuration.Subscriptions.Count} event subscription(s) are registered."
                    );
                }

                foreach (var topicSubscription in configuration.Subscriptions)
                {
                    // Bound into the handler so the dispatcher only considers subscribers belonging
                    // to the subscription that received the frame.
                    var target = topicSubscription;
                    var subscription = await events
                        .SubscribeAsync(
                            target.Topic,
                            target.Name,
                            (frame, token) =>
                                eventDispatcher.DispatchAsync(
                                    target.Topic,
                                    target.Name,
                                    frame,
                                    token
                                ),
                            cancellationToken
                        )
                        .ConfigureAwait(false);
                    _subscriptions.Add(subscription);
                }
            }

            state.MarkListening(_subscriptions.Count);
        }
        catch
        {
            try
            {
                await UnwindAsync().ConfigureAwait(false);
            }
            catch (AggregateException)
            {
                // The startup failure is the actionable one; rollback noise must not mask it.
            }

            throw;
        }
    }

    /// <summary>
    /// Stops every listener and subscription, waiting no longer than the host allows. A handler
    /// that ignores its cancellation can hold a transport's stop; when the host's shutdown
    /// timeout ends first, the stop carries on in the background and its failures are logged.
    /// Stopping again waits for the same stop and releases nothing twice; only the first call
    /// reports a failure.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        var stopping = BeginStop(out var started);
        try
        {
            await stopping.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            if (started)
            {
                _logger.LogWarning(
                    "The host stopped waiting for the HostLoom endpoints to stop; they finish stopping in the background."
                );
                _ = ReportLateFailureAsync(stopping);
            }
        }
        catch (Exception) when (!started)
        {
            // The stop that began the unwind reported this failure.
        }
    }

    /// <summary>
    /// Stops the endpoints if the host never did. After a stop, it neither waits for that stop
    /// again nor rethrows what it already reported.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        var stopping = BeginStop(out var started);
        if (started)
        {
            await stopping.ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Starts the one unwind shared by stop and disposal, so a disposal that follows a stop the
    /// host gave up on does not release the same subscriptions a second time.
    /// </summary>
    private Task BeginStop(out bool started)
    {
        TaskCompletionSource stopped;
        lock (_gate)
        {
            started = _stopping is null;
            if (_stopping is not null)
            {
                return _stopping;
            }

            stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _stopping = stopped.Task;
        }

        // Run outside the gate: a subscription's disposal is the transport's code.
        _ = RunStopAsync(stopped);
        return stopped.Task;
    }

    private async Task RunStopAsync(TaskCompletionSource stopped)
    {
        try
        {
            await UnwindAsync().ConfigureAwait(false);
            stopped.TrySetResult();
        }
        catch (Exception exception)
        {
            stopped.TrySetException(exception);
        }
    }

    private async Task ReportLateFailureAsync(Task stopping)
    {
        try
        {
            await stopping.ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "The HostLoom endpoints failed to stop after the host stopped waiting for them."
            );
        }
    }

    /// <summary>
    /// Releases every acquired subscription in reverse acquisition order. Disposal failures are
    /// collected rather than thrown eagerly so one bad subscription cannot strand the rest.
    /// </summary>
    private async ValueTask UnwindAsync()
    {
        state.MarkStopped();
        List<Exception>? failures = null;

        for (var i = _subscriptions.Count - 1; i >= 0; i--)
        {
            try
            {
                await _subscriptions[i].DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                (failures ??= []).Add(exception);
            }
        }

        _subscriptions.Clear();

        if (failures is not null)
        {
            throw new AggregateException(
                "One or more endpoint subscriptions failed to dispose.",
                failures
            );
        }
    }
}
