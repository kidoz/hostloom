using System.Net.WebSockets;
using Microsoft.Extensions.Logging;

namespace HostLoom.AspNetCore.WebSockets;

internal static class WebSocketLog
{
    public const string HandshakeCategory = "HostLoom.AspNetCore.WebSockets.Handshake";

    private static readonly Action<
        ILogger,
        Guid,
        string,
        string?,
        Exception?
    > SessionOpenedMessage = LoggerMessage.Define<Guid, string, string?>(
        LogLevel.Information,
        WebSocketEvents.SessionOpened,
        "WebSocket session {SessionId} opened with protocol {Protocol} for subject {Subject}."
    );

    private static readonly Action<
        ILogger,
        Guid,
        string,
        string?,
        string,
        WebSocketCloseStatus,
        double,
        Exception?
    > SessionClosedMessage = LoggerMessage.Define<
        Guid,
        string,
        string?,
        string,
        WebSocketCloseStatus,
        double
    >(
        LogLevel.Information,
        WebSocketEvents.SessionClosed,
        "WebSocket session {SessionId} using protocol {Protocol} for subject {Subject} closed with reason {CloseReason} and status {CloseStatus} after {DurationMilliseconds} ms."
    );

    private static readonly Action<
        ILogger,
        Guid,
        string?,
        string,
        Exception?
    > SubscriptionDeniedMessage = LoggerMessage.Define<Guid, string?, string>(
        LogLevel.Warning,
        WebSocketEvents.SubscriptionDenied,
        "WebSocket session {SessionId} subscription to registered topic {Topic} was denied with reason {Reason}."
    );

    private static readonly Action<
        ILogger,
        Guid,
        HubFrameKind,
        string?,
        int,
        int,
        Exception?
    > SlowClientAbortedMessage = LoggerMessage.Define<Guid, HubFrameKind, string?, int, int>(
        LogLevel.Warning,
        WebSocketEvents.SlowClientAborted,
        "WebSocket session {SessionId} was aborted after outbound capacity was exhausted while queueing {FrameKind} for registered topic {Topic}; limits are {MaximumQueuedFrames} frames and {MaximumQueuedBytes} bytes."
    );

    private static readonly Action<ILogger, string, Exception?> HandshakeRejectedMessage =
        LoggerMessage.Define<string>(
            LogLevel.Warning,
            WebSocketEvents.HandshakeRejected,
            "WebSocket handshake was rejected with reason {Reason}."
        );

    private static readonly Action<ILogger, string, Exception?> OperationFailedMessage =
        LoggerMessage.Define<string>(
            LogLevel.Error,
            WebSocketEvents.OperationFailed,
            "WebSocket operation {Operation} failed before a response was produced."
        );

    private static readonly Action<ILogger, string, Guid, Exception?> SnapshotFailedMessage =
        LoggerMessage.Define<string, Guid>(
            LogLevel.Error,
            WebSocketEvents.SnapshotFailed,
            "WebSocket topic {Topic} snapshot failed for session {SessionId}."
        );

    private static readonly Action<
        ILogger,
        string,
        Guid,
        double,
        Exception?
    > SnapshotStalledMessage = LoggerMessage.Define<string, Guid, double>(
        LogLevel.Warning,
        WebSocketEvents.SnapshotStalled,
        "WebSocket topic {Topic} snapshot for session {SessionId} did not finish within {TimeoutMilliseconds} ms; the subscription was removed."
    );

    private static readonly Action<ILogger, string, Exception?> AuthorizationFailedMessage =
        LoggerMessage.Define<string>(
            LogLevel.Error,
            WebSocketEvents.AuthorizationFailed,
            "WebSocket authorization policy {Policy} threw during evaluation; the caller was denied."
        );

    private static readonly Action<ILogger, Guid, Exception?> SessionExpiryFailedMessage =
        LoggerMessage.Define<Guid>(
            LogLevel.Error,
            WebSocketEvents.SessionExpiryFailed,
            "WebSocket session {SessionId} expiry timer failed; session cleanup still ran."
        );

    private static readonly Action<
        ILogger,
        Guid,
        string?,
        int,
        int,
        Exception?
    > ResponseTooLargeMessage = LoggerMessage.Define<Guid, string?, int, int>(
        LogLevel.Warning,
        WebSocketEvents.ResponseTooLarge,
        "WebSocket session {SessionId} response for registered operation {Operation} encoded to {EncodedBytes} bytes, above the {MaximumMessageSize} byte limit, and was replaced by a fault."
    );

    public static void SessionOpened(
        ILogger logger,
        Guid sessionId,
        string protocol,
        string? subject
    ) => SessionOpenedMessage(logger, sessionId, protocol, subject, null);

    public static void SessionClosed(
        ILogger logger,
        Guid sessionId,
        string protocol,
        string? subject,
        string closeReason,
        WebSocketCloseStatus closeStatus,
        double durationMilliseconds
    ) =>
        SessionClosedMessage(
            logger,
            sessionId,
            protocol,
            subject,
            closeReason,
            closeStatus,
            durationMilliseconds,
            null
        );

    public static void SubscriptionDenied(
        ILogger logger,
        Guid sessionId,
        string? topic,
        string reason
    ) => SubscriptionDeniedMessage(logger, sessionId, topic, reason, null);

    public static void SlowClientAborted(
        ILogger logger,
        Guid sessionId,
        HubFrameKind frameKind,
        string? topic,
        int maximumQueuedFrames,
        int maximumQueuedBytes
    ) =>
        SlowClientAbortedMessage(
            logger,
            sessionId,
            frameKind,
            topic,
            maximumQueuedFrames,
            maximumQueuedBytes,
            null
        );

    public static void HandshakeRejected(ILogger logger, string reason) =>
        HandshakeRejectedMessage(logger, reason, null);

    public static void OperationFailed(ILogger logger, string operation, Exception exception) =>
        OperationFailedMessage(logger, operation, exception);

    public static void SnapshotFailed(
        ILogger logger,
        string topic,
        Guid sessionId,
        Exception exception
    ) => SnapshotFailedMessage(logger, topic, sessionId, exception);

    public static void SnapshotStalled(
        ILogger logger,
        string topic,
        Guid sessionId,
        double timeoutMilliseconds
    ) => SnapshotStalledMessage(logger, topic, sessionId, timeoutMilliseconds, null);

    public static void AuthorizationFailed(ILogger logger, string policy, Exception exception) =>
        AuthorizationFailedMessage(logger, policy, exception);

    public static void SessionExpiryFailed(ILogger logger, Guid sessionId, Exception exception) =>
        SessionExpiryFailedMessage(logger, sessionId, exception);

    public static void ResponseTooLarge(
        ILogger logger,
        Guid sessionId,
        string? operation,
        int encodedBytes,
        int maximumMessageSize
    ) =>
        ResponseTooLargeMessage(
            logger,
            sessionId,
            operation,
            encodedBytes,
            maximumMessageSize,
            null
        );
}
