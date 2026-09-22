using HostLoom.Transport.RabbitMq;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace HostLoom.IntegrationTests.Infrastructure;

/// <summary>
/// Records the request addresses, subscriptions, and topics a test mints, and removes the durable
/// queues and exchanges they became on the shared broker once the test is done. The transport
/// declares its topology durable on purpose and never deletes it, so without this every run
/// leaves one queue per address and subscription and one exchange per topic behind.
/// </summary>
public sealed class RabbitMqTopologyScope(
    RabbitMqQueueNaming naming = RabbitMqQueueNaming.Version2,
    Uri? uri = null
) : IAsyncDisposable
{
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(30);

    private readonly Lock _gate = new();
    private readonly List<string> _queues = [];
    private readonly List<string> _exchanges = [];

    /// <summary>Records the request queue a listener on <paramref name="address"/> declares.</summary>
    public string Request(string address)
    {
        Record(_queues, RabbitMqQueueNames.Request(address, naming));
        return address;
    }

    /// <summary>Records the subscription queue and the topic exchange it is bound to.</summary>
    public string Subscription(string topic, string subscription)
    {
        Record(_queues, RabbitMqQueueNames.Subscription(topic, subscription, naming));
        return Topic(topic);
    }

    /// <summary>Records the fanout exchange a topic is declared as.</summary>
    public string Topic(string topic)
    {
        Record(_exchanges, new RequestAddress(topic).Value);
        return topic;
    }

    /// <summary>
    /// Deletes every recorded queue, then every recorded exchange, under one bounded deadline so a
    /// broker that stops answering fails the cleanup rather than hanging the run. A name that was
    /// never declared, because the test failed before reaching it, is not an error.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        List<string> queues;
        List<string> exchanges;
        lock (_gate)
        {
            queues = [.. _queues];
            exchanges = [.. _exchanges];
            _queues.Clear();
            _exchanges.Clear();
        }

        if (queues.Count == 0 && exchanges.Count == 0)
        {
            return;
        }

        using var deadline = new CancellationTokenSource(Deadline);
        var token = deadline.Token;
        var factory = new ConnectionFactory();
        if (uri is not null)
        {
            factory.Uri = uri;
        }

        await using var connection = await factory
            .CreateConnectionAsync(token)
            .ConfigureAwait(false);
        foreach (var queue in queues)
        {
            await DeleteAsync(
                    connection,
                    channel => channel.QueueDeleteAsync(queue, cancellationToken: token),
                    token
                )
                .ConfigureAwait(false);
        }

        foreach (var exchange in exchanges)
        {
            await DeleteAsync(
                    connection,
                    channel => channel.ExchangeDeleteAsync(exchange, cancellationToken: token),
                    token
                )
                .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// One channel per deletion: a not-found reply closes the channel it arrives on, so sharing
    /// one would turn the first missing name into a failure for every name after it.
    /// </summary>
    private static async Task DeleteAsync(
        IConnection connection,
        Func<IChannel, Task> delete,
        CancellationToken token
    )
    {
        await using var channel = await connection
            .CreateChannelAsync(cancellationToken: token)
            .ConfigureAwait(false);
        try
        {
            await delete(channel).ConfigureAwait(false);
        }
        catch (OperationInterruptedException exception)
            when (exception.ShutdownReason?.ReplyCode == 404)
        {
            // Never declared, or already gone: the outcome the cleanup wanted.
        }
    }

    private void Record(List<string> names, string name)
    {
        lock (_gate)
        {
            if (!names.Contains(name, StringComparer.Ordinal))
            {
                names.Add(name);
            }
        }
    }
}
