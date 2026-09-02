namespace HostLoom.AspNetCore.WebSockets;

/// <summary>Disconnects active WebSocket sessions after logout or authorization changes.</summary>
public interface IWebSocketSessionControl
{
    /// <summary>Disconnects one session and waits for its lifecycle to finish.</summary>
    ValueTask<bool> DisconnectAsync(
        string sessionId,
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
