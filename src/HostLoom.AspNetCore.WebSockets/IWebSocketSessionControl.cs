namespace HostLoom.AspNetCore.WebSockets;

/// <summary>Disconnects active WebSocket sessions after logout or authorization changes.</summary>
/// <remarks>
/// A disconnect sends a 1008 close frame and waits for the peer to answer it, or for
/// <see cref="HostLoomWebSocketOptions.CloseTimeout"/> to abort the connection, before the session
/// lifecycle finishes.
/// </remarks>
public interface IWebSocketSessionControl
{
    /// <summary>Disconnects one session and waits for its lifecycle to finish.</summary>
    ValueTask<bool> DisconnectAsync(
        Guid sessionId,
        string reason,
        CancellationToken cancellationToken = default
    );

    /// <summary>Disconnects every session for an exact subject and returns the matched count.</summary>
    ValueTask<int> DisconnectSubjectAsync(
        string subject,
        string reason,
        CancellationToken cancellationToken = default
    );
}
