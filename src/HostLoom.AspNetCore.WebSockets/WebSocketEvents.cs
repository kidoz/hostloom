using Microsoft.Extensions.Logging;

namespace HostLoom.AspNetCore.WebSockets;

/// <summary>
/// Stable event ids for WebSocket gateway logs, so logging pipelines can filter or alert without
/// matching message text.
/// </summary>
public static class WebSocketEvents
{
    /// <summary>Information: an authenticated or anonymous gateway session was accepted.</summary>
    public static readonly EventId SessionOpened = new(4100, "WebSocketSessionOpened");

    /// <summary>Information: a gateway session completed or was aborted.</summary>
    public static readonly EventId SessionClosed = new(4101, "WebSocketSessionClosed");

    /// <summary>Warning: a requested subscription was rejected before registration.</summary>
    public static readonly EventId SubscriptionDenied = new(4102, "WebSocketSubscriptionDenied");

    /// <summary>Warning: outbound capacity was exhausted and the slow session was aborted.</summary>
    public static readonly EventId SlowClientAborted = new(4103, "WebSocketSlowClientAborted");

    /// <summary>Warning: an upgrade request was rejected by the gateway handler.</summary>
    public static readonly EventId HandshakeRejected = new(4104, "WebSocketHandshakeRejected");

    /// <summary>Error: a registered request operation failed before producing a response.</summary>
    public static readonly EventId OperationFailed = new(4105, "WebSocketOperationFailed");

    /// <summary>Error: a registered topic snapshot provider failed.</summary>
    public static readonly EventId SnapshotFailed = new(4106, "WebSocketSnapshotFailed");
}
