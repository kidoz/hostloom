namespace HostLoom.AspNetCore.WebSockets;

/// <summary>Determines how the gateway validates the HTTP Origin handshake header.</summary>
public enum WebSocketOriginMode
{
    /// <summary>Does not validate the Origin header.</summary>
    Disabled = 0,

    /// <summary>Requires a supplied Origin to match the effective request origin.</summary>
    SameOrigin = 1,

    /// <summary>Requires a supplied Origin to match a configured allowlist entry.</summary>
    AllowList = 2,
}
