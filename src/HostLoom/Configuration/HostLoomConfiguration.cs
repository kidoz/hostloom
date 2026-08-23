using System.Diagnostics.CodeAnalysis;

namespace HostLoom;

internal sealed class HostLoomConfiguration
{
    private readonly Dictionary<RequestAddress, Dictionary<string, HandlerRegistration>> _endpoints = [];
    private readonly HashSet<string> _messageTypes = new(StringComparer.Ordinal);

    public IReadOnlyCollection<RequestAddress> Endpoints => _endpoints.Keys;

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
