using Microsoft.Extensions.Hosting;

namespace HostLoom;

internal sealed class RequestEndpointHostedService(
    HostLoomConfiguration configuration,
    IRequestBroker broker,
    MessageDispatcher dispatcher,
    EndpointRuntimeState state) : IHostedService, IAsyncDisposable
{
    private readonly List<IAsyncDisposable> _subscriptions = [];

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (configuration.Endpoints.Count == 0)
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
                        cancellationToken)
                    .ConfigureAwait(false);
                _subscriptions.Add(subscription);
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

    public async Task StopAsync(CancellationToken cancellationToken) => await DisposeAsync().ConfigureAwait(false);

    public async ValueTask DisposeAsync() => await UnwindAsync().ConfigureAwait(false);

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
            throw new AggregateException("One or more endpoint subscriptions failed to dispose.", failures);
        }
    }
}
