using System.Diagnostics.CodeAnalysis;
using HostLoom.Pipelines;

namespace HostLoom;

internal sealed class HostLoomConfiguration
{
    private readonly Dictionary<RequestAddress, Dictionary<string, HandlerRegistration>> _endpoints = [];
    private readonly Dictionary<TopicSubscription, Dictionary<string, SubscriberRegistration>> _subscriptions = [];
    private readonly HashSet<string> _messageTypes = new(StringComparer.Ordinal);

    public IReadOnlyCollection<RequestAddress> Endpoints => _endpoints.Keys;

    /// <summary>
    /// Receive-pipeline filters, in registration order. Composed once when the dispatcher is
    /// constructed, so stateful filters such as a circuit breaker span every delivery.
    /// </summary>
    public Action<PipeBuilder<ReceiveContext>>? ReceivePipeline { get; private set; }

    public void ConfigureReceivePipeline(Action<PipeBuilder<ReceiveContext>> configure) =>
        ReceivePipeline += configure;

    public void AddHandler(HandlerRegistration registration, RequestAddress endpoint)
    {
        if (!_messageTypes.Add(registration.MessageType))
        {
            throw new InvalidOperationException($"A handler for '{registration.MessageType}' is already registered.");
        }

        if (!_endpoints.TryGetValue(endpoint, out var handlers))
        {
            handlers = new Dictionary<string, HandlerRegistration>(StringComparer.Ordinal);
            _endpoints[endpoint] = handlers;
        }

        handlers.Add(registration.MessageType, registration);
    }

    /// <summary>Every (topic, subscription) pair that must be attached at startup.</summary>
    public IReadOnlyCollection<TopicSubscription> Subscriptions => _subscriptions.Keys;

    /// <summary>
    /// Unlike request handlers, several subscriptions may consume the same event type, and several
    /// handlers may consume it within one subscription. Only the (topic, subscription, type) triple
    /// is unique, and the handlers themselves are resolved as a set from the container.
    /// </summary>
    public void AddSubscriber(SubscriberRegistration registration, TopicSubscription subscription, Type handlerType)
    {
        if (!_subscriptions.TryGetValue(subscription, out var subscribers))
        {
            subscribers = new Dictionary<string, SubscriberRegistration>(StringComparer.Ordinal);
            _subscriptions[subscription] = subscribers;
        }

        if (!subscribers.TryGetValue(registration.MessageType, out var existing))
        {
            existing = registration;
            subscribers[registration.MessageType] = existing;
        }

        if (!existing.HandlerTypes.Contains(handlerType))
        {
            existing.HandlerTypes.Add(handlerType);
        }
    }

    public bool TryGetSubscriber(
        RequestAddress topic,
        string subscription,
        string messageType,
        [NotNullWhen(true)] out SubscriberRegistration? registration)
    {
        registration = null;
        return _subscriptions.TryGetValue(new TopicSubscription(topic, subscription), out var subscribers)
            && subscribers.TryGetValue(messageType, out registration);
    }

    /// <summary>
    /// Resolves a handler within the endpoint that received the request. Registrations are
    /// endpoint-scoped so an envelope delivered to the wrong endpoint is not executed there.
    /// </summary>
    public bool TryGetHandler(
        RequestAddress endpoint,
        string messageType,
        [NotNullWhen(true)] out HandlerRegistration? registration)
    {
        registration = null;
        return _endpoints.TryGetValue(endpoint, out var handlers)
            && handlers.TryGetValue(messageType, out registration);
    }
}

internal sealed record HandlerRegistration(
    string MessageType,
    Type RequestType,
    Type ResponseType,
    Type ExecutorType);

internal sealed record SubscriberRegistration(
    string MessageType,
    Type EventType,
    Type ExecutorType)
{
    /// <summary>Handlers for this event type within one subscription, in registration order.</summary>
    public List<Type> HandlerTypes { get; } = [];
}

/// <summary>A named consumer of a topic. Distinct names on one topic each receive every event.</summary>
internal readonly record struct TopicSubscription(RequestAddress Topic, string Name);
