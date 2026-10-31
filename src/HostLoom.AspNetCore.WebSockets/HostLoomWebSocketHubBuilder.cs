namespace HostLoom.AspNetCore.WebSockets;

/// <summary>Registers request operations and keyed channels within one logical Hub.</summary>
public sealed class HostLoomWebSocketHubBuilder
{
    private readonly HostLoomWebSocketBuilder _gateway;
    private readonly string _prefix;

    internal HostLoomWebSocketHubBuilder(HostLoomWebSocketBuilder gateway, string name)
    {
        ArgumentNullException.ThrowIfNull(gateway);
        ValidatePart(name, nameof(name));
        _gateway = gateway;
        _prefix = name + ".";
    }

    /// <summary>Registers a typed request operation relative to the Hub name.</summary>
    public HostLoomWebSocketHubBuilder AddRequest<TRequest, TResponse>(
        string operation,
        RequestAddress destination,
        string? authorizationPolicy = null
    )
        where TRequest : class, IRequest<TResponse>
    {
        _gateway.AddRequest<TRequest, TResponse>(
            Compose(operation, nameof(operation)),
            destination,
            authorizationPolicy
        );
        return this;
    }

    /// <summary>Registers a keyed event channel relative to the Hub name.</summary>
    public HostLoomWebSocketHubBuilder AddChannel<TEvent>(
        string channel,
        RequestAddress source,
        Func<TEvent, string?> keySelector,
        string subscription = "hostloom-websocket",
        string? authorizationPolicy = null,
        bool allowTopicWideSubscription = false
    )
        where TEvent : class, IEvent
    {
        _gateway.AddTopic(
            Compose(channel, nameof(channel)),
            source,
            keySelector,
            subscription,
            authorizationPolicy,
            allowTopicWideSubscription
        );
        return this;
    }

    /// <summary>Registers the snapshot provider for a channel in this Hub.</summary>
    public HostLoomWebSocketHubBuilder AddChannelSnapshot<TEvent, TProvider>(string channel)
        where TEvent : class, IEvent
        where TProvider : class, IWebSocketTopicSnapshotProvider<TEvent>
    {
        _gateway.AddTopicSnapshot<TEvent, TProvider>(Compose(channel, nameof(channel)));
        return this;
    }

    private string Compose(string value, string parameterName)
    {
        ValidatePart(value, parameterName);
        var composed = _prefix + value;
        GatewayConfiguration.ValidateName(composed, parameterName);
        return composed;
    }

    private static void ValidatePart(string value, string parameterName)
    {
        GatewayConfiguration.ValidateName(value, parameterName);
        if (value[0] == '.' || value[^1] == '.')
        {
            throw new ArgumentException(
                "Hub and relative route names cannot start or end with a dot.",
                parameterName
            );
        }
    }
}
