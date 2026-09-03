namespace HostLoom.AspNetCore.WebSockets;

/// <summary>Provides read-only snapshots of active WebSocket sessions.</summary>
public interface IWebSocketSessionDirectory
{
    /// <summary>Gets the current number of active sessions.</summary>
    int Count { get; }

    /// <summary>Returns a point-in-time snapshot of every active session.</summary>
    IReadOnlyList<WebSocketSessionInfo> GetSessions();

    /// <summary>Returns a point-in-time snapshot of sessions for an exact subject.</summary>
    IReadOnlyList<WebSocketSessionInfo> GetSessionsBySubject(string subject);
}

/// <summary>Describes a WebSocket session without retaining its credential principal.</summary>
/// <param name="SessionId">The opaque server-generated session identifier.</param>
/// <param name="Subject">The configured subject claim value, when present.</param>
/// <param name="Protocol">The negotiated HostLoom WebSocket subprotocol.</param>
/// <param name="ConnectedAt">The UTC time at which the session was registered.</param>
/// <param name="ExpiresAt">The UTC time at which the session will be closed.</param>
/// <param name="SubscriptionCount">The number of currently active subscriptions.</param>
public sealed record WebSocketSessionInfo(
    Guid SessionId,
    string? Subject,
    string Protocol,
    DateTimeOffset ConnectedAt,
    DateTimeOffset ExpiresAt,
    int SubscriptionCount
);
