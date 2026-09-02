using Microsoft.AspNetCore.Http;

namespace HostLoom.AspNetCore.WebSockets;

/// <summary>Validates the Origin header before a WebSocket upgrade is accepted.</summary>
public interface IWebSocketOriginValidator
{
    /// <summary>Returns whether the request's Origin is acceptable.</summary>
    ValueTask<bool> IsAllowedAsync(HttpContext context, CancellationToken cancellationToken);
}
