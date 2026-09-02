using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace HostLoom.AspNetCore.WebSockets;

public sealed class HostLoomWebSocketBuilder
{
    private readonly HostLoomBuilder _hostLoom;
    private readonly GatewayConfiguration _configuration;

    internal HostLoomWebSocketBuilder(HostLoomBuilder hostLoom, GatewayConfiguration configuration)
    {
        _hostLoom = hostLoom;
        _configuration = configuration;
    }

    public IServiceCollection Services => _hostLoom.Services;

    public HostLoomWebSocketBuilder AddRequest<TRequest, TResponse>(
        string operation,
        RequestAddress destination,
        string? authorizationPolicy = null
    )
        where TRequest : class, IRequest<TResponse>
    {
        _configuration.AddRequest(
            new RequestRoute(
                operation,
                destination,
                typeof(WebSocketRequestInvoker<TRequest, TResponse>),
                authorizationPolicy
            )
        );
        _hostLoom.AddRequestClient<TRequest, TResponse>();
        Services.AddScoped<WebSocketRequestInvoker<TRequest, TResponse>>();
        return this;
    }

    public HostLoomWebSocketBuilder AddTopic<TEvent>(
        string topic,
        RequestAddress source,
        string subscription = "hostloom-websocket",
        string? authorizationPolicy = null
    )
        where TEvent : class, IEvent =>
        AddTopic<TEvent>(topic, source, static _ => null, subscription, authorizationPolicy);

    public HostLoomWebSocketBuilder AddTopic<TEvent>(
        string topic,
        RequestAddress source,
        Func<TEvent, string?> keySelector,
        string subscription = "hostloom-websocket",
        string? authorizationPolicy = null
    )
        where TEvent : class, IEvent
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        _configuration.AddTopic(
            new TopicRoute(
                topic,
                typeof(TEvent),
                value => keySelector((TEvent)value),
                authorizationPolicy
            )
        );
        _hostLoom.AddSubscriber<TEvent, WebSocketEventForwarder<TEvent>>(source, subscription);
        return this;
    }

    public HostLoomWebSocketBuilder AddTopicSnapshot<TEvent, TProvider>(string topic)
        where TEvent : class, IEvent
        where TProvider : class, IWebSocketTopicSnapshotProvider<TEvent>
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        _configuration.AddTopicSnapshot(
            topic,
            typeof(TEvent),
            typeof(WebSocketTopicSnapshotInvoker<TEvent>)
        );
        Services.TryAddScoped<IWebSocketTopicSnapshotProvider<TEvent>, TProvider>();
        Services.TryAddScoped<WebSocketTopicSnapshotInvoker<TEvent>>();
        return this;
    }
}
