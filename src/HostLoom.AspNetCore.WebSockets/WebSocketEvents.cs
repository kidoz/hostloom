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

    /// <summary>
    /// Warning: snapshot initialization exceeded the configured timeout and the subscription was
    /// removed.
    /// </summary>
    public static readonly EventId SnapshotStalled = new(4107, "WebSocketSnapshotStalled");

    /// <summary>
    /// Error: evaluating a registered authorization policy threw; the caller received
    /// <c>forbidden</c>.
    /// </summary>
    public static readonly EventId AuthorizationFailed = new(4108, "WebSocketAuthorizationFailed");

    /// <summary>
    /// Error: the session expiry timer failed; the session still ran its ordinary cleanup.
    /// </summary>
    public static readonly EventId SessionExpiryFailed = new(4109, "WebSocketSessionExpiryFailed");

    /// <summary>
    /// Warning: an encoded response exceeded the maximum message size and was replaced by a
    /// <c>message_too_large</c> fault.
    /// </summary>
    public static readonly EventId ResponseTooLarge = new(4110, "WebSocketResponseTooLarge");
}
