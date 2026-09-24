using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace HostLoom.Transport.RabbitMq;

public sealed partial class RabbitMqRequestBroker
{
    private const string RequestRole = "request";
    private const string EventRole = "event";
    private const string ReplyRole = "reply";

    /// <summary>
    /// The wait after a failed attempt to restore a cancelled consumer. Each further failure
    /// doubles it, up to <see cref="RestoreBackoffCap"/>.
    /// </summary>
    internal static readonly TimeSpan RestoreBackoff = TimeSpan.FromSeconds(1);

    /// <summary>The longest wait between two attempts to restore a cancelled consumer.</summary>
    internal static readonly TimeSpan RestoreBackoffCap = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Opens a channel for one listener or subscription, declares what it consumes, and consumes
    /// it with a consumer from <see cref="ConsumerSubscription.CreateConsumer"/>. Returns the
    /// channel, or closes it and throws. <paramref name="restoring"/> is set when the broker
    /// cancelled the consumer this one replaces.
    /// </summary>
    private delegate Task<IChannel> ConsumeAsync(
        ConsumerSubscription owner,
        bool restoring,
        CancellationToken cancellationToken
    );

    private void ReportConsumerCancelled(string role, string queue)
    {
        RabbitMqDiagnostics.Consumers.Add(
            1,
            _clientTag,
            new(RabbitMqDiagnostics.EventTag, "cancelled"),
            new(RabbitMqDiagnostics.RoleTag, role)
        );
        if (role == ReplyRole)
        {
            _logger.LogWarning(
                new EventId(1411, "RabbitMqConsumerCancelled"),
                "The broker cancelled the RabbitMQ reply consumer of queue {Queue}, as it does when the queue is deleted. Requests waiting for a reply there time out; the next request declares a new reply queue.",
                queue
            );
            return;
        }

        _logger.LogWarning(
            new EventId(1411, "RabbitMqConsumerCancelled"),
            "The broker cancelled the RabbitMQ {Role} consumer of queue {Queue}, as it does when the queue is deleted; declaring the queue again and resubscribing.",
            role,
            queue
        );
    }

    private void ReportConsumerRestored(string role, string queue, int attempt)
    {
        RabbitMqDiagnostics.Consumers.Add(
            1,
            _clientTag,
            new(RabbitMqDiagnostics.EventTag, "restored"),
            new(RabbitMqDiagnostics.RoleTag, role)
        );
        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation(
                new EventId(1412, "RabbitMqConsumerRestored"),
                "RabbitMQ {Role} consumption resumed on queue {Queue} after the broker cancelled the previous consumer, on attempt {Attempt}.",
                role,
                queue,
                attempt
            );
        }
    }

    /// <summary>
    /// One listener or subscription: the consumer channel it runs on, the deliveries its handler
    /// is working on, and the restoration of consumption after the broker cancels the consumer.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Disposal cancels the handlers in flight, starts closing the channel, and waits for those
    /// handlers, whose deliveries are requeued, but not for the close. A close waits for the
    /// broker's close-ok, up to the client library's 20-second continuation timeout, so awaiting
    /// it made every listener stopped against an unresponsive broker take that long, one after
    /// another. The close runs in the background instead, and the broker's disposal waits a
    /// bounded time for it. A delivery that arrives once disposal has begun is requeued without
    /// reaching the handler.
    /// </para>
    /// <para>
    /// The broker cancels a consumer when its queue is deleted, or becomes unavailable, and the
    /// client library reports that to nobody: the channel stays open and nothing is delivered
    /// again. So a cancellation is logged and counted, and consumption is restored on a new
    /// channel, declaring the queue again, with a doubling wait between failed attempts, until
    /// it succeeds or the subscription or the broker is disposed. The cancelled channel is closed
    /// rather than reused, because the client library still holds the cancelled consumer for its
    /// own recovery and would re-create it on the next reconnect.
    /// </para>
    /// </remarks>
    private sealed class ConsumerSubscription : IAsyncDisposable
    {
        private readonly RabbitMqRequestBroker _broker;
        private readonly string _queue;
        private readonly string _role;
        private readonly ConsumeAsync _consume;
        private readonly CancellationTokenSource _stopping = new();
        private readonly CancellationTokenSource _restoration;
        private readonly TaskCompletionSource _drained = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private readonly Lock _gate = new();
        private IChannel? _channel;
        private IChannel? _cancelled;
        private Task _restoring = Task.CompletedTask;
        private bool _restoreRequested;
        private bool _restoreRunning;

        // One for the subscription itself, plus one for each delivery its handler is working on.
        private int _active = 1;
        private int _disposed;

        private ConsumerSubscription(
            RabbitMqRequestBroker broker,
            string queue,
            string role,
            ConsumeAsync consume
        )
        {
            _broker = broker;
            _queue = queue;
            _role = role;
            _consume = consume;
            Stopping = _stopping.Token;
            _restoration = CancellationTokenSource.CreateLinkedTokenSource(
                Stopping,
                broker._shutdownToken
            );
        }

        /// <summary>Cancelled when the subscription is disposed; every handler token is linked to it.</summary>
        public CancellationToken Stopping { get; }

        public static async Task<ConsumerSubscription> StartAsync(
            RabbitMqRequestBroker broker,
            string queue,
            string role,
            ConsumeAsync consume,
            CancellationToken cancellationToken
        )
        {
            var subscription = new ConsumerSubscription(broker, queue, role, consume);
            try
            {
                var channel = await consume(subscription, restoring: false, cancellationToken)
                    .ConfigureAwait(false);
                subscription.Attach(channel);
                return subscription;
            }
            catch
            {
                await subscription._stopping.CancelAsync().ConfigureAwait(false);
                subscription.Exit();
                subscription._restoration.Dispose();
                subscription._stopping.Dispose();
                throw;
            }
        }

        /// <summary>A consumer for <paramref name="channel"/> that reports its cancellation here.</summary>
        public NotifyingConsumer CreateConsumer(IChannel channel) =>
            new(channel, () => OnCancelled(channel));

        /// <summary>
        /// Admits a delivery to the handler, unless the subscription is being disposed. Every
        /// admission is paired with <see cref="Exit"/>, which disposal waits for.
        /// </summary>
        public bool TryEnter()
        {
            var active = Volatile.Read(ref _active);
            while (active > 0 && !Stopping.IsCancellationRequested)
            {
                var seen = Interlocked.CompareExchange(ref _active, active + 1, active);
                if (seen == active)
                {
                    return true;
                }

                active = seen;
            }

            return false;
        }

        public void Exit()
        {
            if (Interlocked.Decrement(ref _active) == 0)
            {
                _drained.TrySetResult();
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            try
            {
                await _stopping.CancelAsync().ConfigureAwait(false);
            }
            finally
            {
                IChannel? channel;
                Task restoring;
                lock (_gate)
                {
                    channel = _channel;
                    _channel = null;
                    restoring = _restoring;
                }

                if (channel is not null)
                {
                    _ = _broker._consumerChannels.Start(channel);
                }

                Exit();
                await restoring.ConfigureAwait(false);
                await _drained.Task.ConfigureAwait(false);
                _restoration.Dispose();
                _stopping.Dispose();
            }
        }

        private void Attach(IChannel channel)
        {
            lock (_gate)
            {
                _channel = channel;
                // Cancelled before it was attached: restore it now, as if it came after.
                if (ReferenceEquals(_cancelled, channel))
                {
                    RequestRestoration();
                }
            }
        }

        private void OnCancelled(IChannel channel)
        {
            lock (_gate)
            {
                if (Stopping.IsCancellationRequested)
                {
                    return;
                }

                _cancelled = channel;
                if (ReferenceEquals(_channel, channel))
                {
                    RequestRestoration();
                }
            }

            _broker.ReportConsumerCancelled(_role, _queue);
        }

        /// <summary>Starts the restoration loop unless it is running already; called under the gate.</summary>
        private void RequestRestoration()
        {
            _restoreRequested = true;
            if (!_restoreRunning)
            {
                _restoreRunning = true;
                _restoring = Task.Run(RestoreAsync);
            }
        }

        private async Task RestoreAsync()
        {
            while (true)
            {
                lock (_gate)
                {
                    if (!_restoreRequested || _restoration.IsCancellationRequested)
                    {
                        _restoreRunning = false;
                        return;
                    }

                    _restoreRequested = false;
                }

                await RestoreOnceAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Consumes again on a new channel, retrying with a doubling wait until that succeeds or
        /// the subscription or the broker is disposed, then closes the channel it replaces.
        /// </summary>
        private async Task RestoreOnceAsync()
        {
            var token = _restoration.Token;
            var delay = RestoreBackoff;
            for (var attempt = 1; ; attempt++)
            {
                IChannel channel;
                try
                {
                    channel = await _consume(this, restoring: true, token).ConfigureAwait(false);
                }
                catch (Exception) when (token.IsCancellationRequested)
                {
                    return;
                }
                catch (ObjectDisposedException)
                {
                    // The broker was disposed; its shutdown ends every restoration.
                    return;
                }
                catch (Exception exception)
                {
                    _broker._logger.LogWarning(
                        new EventId(1413, "RabbitMqConsumerRestoreFailed"),
                        exception,
                        "Restoring RabbitMQ {Role} consumption on queue {Queue} failed on attempt {Attempt}; retrying in {Delay}.",
                        _role,
                        _queue,
                        attempt,
                        delay
                    );
                    try
                    {
                        await Task.Delay(delay, _broker._clock, token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }

                    delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, RestoreBackoffCap.Ticks));
                    continue;
                }

                IChannel? replaced;
                var disposing = false;
                lock (_gate)
                {
                    if (Stopping.IsCancellationRequested)
                    {
                        // Disposal took the previous channel already; this one goes too.
                        replaced = channel;
                        disposing = true;
                    }
                    else
                    {
                        replaced = _channel;
                        _channel = channel;
                        if (ReferenceEquals(_cancelled, channel))
                        {
                            _restoreRequested = true;
                        }
                    }
                }

                if (replaced is not null)
                {
                    _ = _broker._consumerChannels.Start(replaced);
                }

                if (!disposing)
                {
                    _broker.ReportConsumerRestored(_role, _queue, attempt);
                }

                return;
            }
        }
    }

    /// <summary>
    /// A consumer that reports the broker cancelling it, which the client library otherwise only
    /// records on the consumer itself.
    /// </summary>
    private sealed class NotifyingConsumer(IChannel channel, Action cancelled)
        : AsyncEventingBasicConsumer(channel)
    {
        private volatile bool _cancelledByBroker;

        /// <summary>Whether the broker cancelled this consumer, as it does when its queue is deleted.</summary>
        public bool CancelledByBroker => _cancelledByBroker;

        /// <summary>
        /// Called for a <c>basic.cancel</c> from the broker only: neither a channel closing nor a
        /// cancellation this client asked for arrives here.
        /// </summary>
        public override async Task HandleBasicCancelAsync(
            string consumerTag,
            CancellationToken cancellationToken = default
        )
        {
            _cancelledByBroker = true;
            await base.HandleBasicCancelAsync(consumerTag, cancellationToken).ConfigureAwait(false);
            cancelled();
        }
    }
}
