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
    /// How long stopping a listener or subscription waits for the handlers it cancelled before it
    /// closes their channel anyway. The same five seconds the in-memory transport gives its
    /// handlers and disposal gives closing channels: long enough for a handler finishing its
    /// cleanup, short enough that a handler that never returns cannot keep its deliveries from
    /// the broker. The host's shutdown timeout bounds how long the host waits for all of it.
    /// </summary>
    internal static readonly TimeSpan HandlerDrainBound = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Opens a channel for one listener or subscription, declares what it consumes, and consumes
    /// it with a consumer from <see cref="ConsumerSubscription.CreateConsumer"/>. Returns the
    /// channel and its consumer's tag, or closes the channel and throws.
    /// <paramref name="restoring"/> is set when the broker cancelled the consumer this one
    /// replaces.
    /// </summary>
    private delegate Task<Consumption> ConsumeAsync(
        ConsumerSubscription owner,
        bool restoring,
        CancellationToken cancellationToken
    );

    /// <summary>A channel consuming a queue, and the tag the broker knows its consumer by.</summary>
    private readonly record struct Consumption(IChannel Channel, string ConsumerTag);

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
            "The broker cancelled the RabbitMQ {Role} consumer of queue {Queue}, as it does when the queue is deleted; declaring the queue again and resubscribing. What the deleted queue held is lost, and so is whatever is sent before the queue is declared again: the broker confirms an event and the exchange drops it, and a request returns as unroutable and times out.",
            role,
            queue
        );
    }

    private void ReportHandlersAbandoned(string role, string queue, int running) =>
        _logger.LogWarning(
            new EventId(1406, "RabbitMqHandlersAbandoned"),
            "Stopping the RabbitMQ {Role} consumer of queue {Queue} waited {Bound} for {Running} handlers to return after their cancellation; closing its channel without them. The broker redelivers their deliveries, so those handlers may run again.",
            role,
            queue,
            HandlerDrainBound,
            running
        );

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
    /// Disposal cancels the handlers in flight, asks the broker to stop delivering to the
    /// consumer, and waits for those handlers, up to <see cref="HandlerDrainBound"/>, before it
    /// starts closing the channel. While the channel is open a handler that returns is settled as
    /// usual: one that honoured its cancellation has its delivery requeued, and one that finished
    /// all the same is answered and acknowledged rather than run again. Closing the channel first
    /// made that acknowledgement fail, and a handler that had succeeded was reported as a
    /// rejected delivery the broker then redelivered. A handler still running after the bound is
    /// logged and left running; the channel closes without it, and the broker redelivers its
    /// delivery.
    /// </para>
    /// <para>
    /// Disposal does not wait for the close. A close waits for the broker's close-ok, up to the
    /// client library's 20-second continuation timeout, so awaiting it made every listener
    /// stopped against an unresponsive broker take that long, one after another. The close runs
    /// in the background instead, and the broker's disposal waits a bounded time for it. A
    /// delivery that arrives once disposal has begun is requeued without reaching the handler.
    /// </para>
    /// <para>
    /// The broker cancels a consumer when its queue is deleted, or becomes unavailable, and the
    /// client library reports that to nobody: the channel stays open and nothing is delivered
    /// again. So a cancellation is logged and counted, and consumption is restored on a new
    /// channel, declaring the queue again, with a doubling wait between failed attempts, until
    /// it succeeds or the subscription or the broker is disposed. The cancelled channel is closed
    /// rather than reused, because the client library still holds the cancelled consumer for its
    /// own recovery and would re-create it on the next reconnect. Restoring cannot recover what
    /// was sent while the queue did not exist: an event publication is not mandatory, so the
    /// broker confirmed it and the fanout exchange, with no queue bound, dropped it.
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
        private string? _consumerTag;
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

        /// <summary>The queue this listener or subscription consumes.</summary>
        public string Queue => _queue;

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
                var consumption = await consume(subscription, restoring: false, cancellationToken)
                    .ConfigureAwait(false);
                subscription.Attach(consumption);
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
                string? consumerTag;
                Task restoring;
                lock (_gate)
                {
                    channel = _channel;
                    consumerTag = _consumerTag;
                    _channel = null;
                    restoring = _restoring;
                }

                Exit();
                var drained = await DrainAsync(channel, consumerTag, restoring)
                    .ConfigureAwait(false);
                _restoration.Dispose();
                // Kept while a handler left running may still link a token to it.
                if (drained)
                {
                    _stopping.Dispose();
                }

                // Only now: closing the channel ends every settlement a handler has yet to make.
                if (channel is not null)
                {
                    _ = _broker._consumerChannels.Start(channel);
                }
            }
        }

        /// <summary>
        /// Asks the broker to stop delivering to the consumer, without waiting for its answer, so
        /// the deliveries that arrive while the stop waits for its handlers are not requeued and
        /// handed straight back to this same consumer, over and over. A cancel that cannot be sent
        /// changes nothing: closing the channel afterwards stops the deliveries all the same.
        /// </summary>
        private static async Task StopDeliveriesAsync(
            IChannel channel,
            string consumerTag,
            CancellationToken deadline
        )
        {
            try
            {
                await channel
                    .BasicCancelAsync(consumerTag, noWait: true, deadline)
                    .WaitAsync(deadline)
                    .ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Closed, closing, or out of time: the close that follows stops the deliveries.
            }
        }

        /// <summary>
        /// Stops the deliveries to the consumer, joins the restoration, and waits for the
        /// handlers the stop cancelled, all within <see cref="HandlerDrainBound"/>, reporting the
        /// handlers still running then. Returns whether every handler returned.
        /// </summary>
        private async Task<bool> DrainAsync(IChannel? channel, string? consumerTag, Task restoring)
        {
            using var deadline = new CancellationTokenSource(HandlerDrainBound, _broker._clock);
            if (channel is not null && consumerTag is not null)
            {
                await StopDeliveriesAsync(channel, consumerTag, deadline.Token)
                    .ConfigureAwait(false);
            }

            await restoring.ConfigureAwait(false);
            try
            {
                await _drained.Task.WaitAsync(deadline.Token).ConfigureAwait(false);
                return true;
            }
            catch (OperationCanceledException) when (deadline.IsCancellationRequested)
            {
                _broker.ReportHandlersAbandoned(_role, _queue, Volatile.Read(ref _active));
                return false;
            }
        }

        private void Attach(Consumption consumption)
        {
            lock (_gate)
            {
                _channel = consumption.Channel;
                _consumerTag = consumption.ConsumerTag;
                // Cancelled before it was attached: restore it now, as if it came after.
                if (ReferenceEquals(_cancelled, consumption.Channel))
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
                Consumption consumption;
                try
                {
                    consumption = await _consume(this, restoring: true, token)
                        .ConfigureAwait(false);
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

                var channel = consumption.Channel;
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
                        _consumerTag = consumption.ConsumerTag;
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
