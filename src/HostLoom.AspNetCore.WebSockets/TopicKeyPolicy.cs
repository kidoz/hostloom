namespace HostLoom.AspNetCore.WebSockets;

/// <summary>Built-in authorization policies for client-selected topic keys.</summary>
public static class TopicKeyPolicy
{
    /// <summary>
    /// Requires a nonempty subscription key equal to the configured subject claim
    /// <see cref="HostLoomWebSocketOptions.SubjectClaimType"/>.
    /// </summary>
    public const string SubjectOnly = "HostLoom.WebSockets.TopicKey.SubjectOnly";
}
