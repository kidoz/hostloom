using Microsoft.AspNetCore.Http;

namespace HostLoom.AspNetCore.WebSockets;

/// <summary>Resolves the credential expiry that bounds a WebSocket session.</summary>
public interface IWebSocketSessionLifetimeResolver
{
    /// <summary>
    /// Resolves the credential expiry for an accepted HTTP request, or <see langword="null"/> when
    /// the credential carries no expiry.
    /// </summary>
    ValueTask<DateTimeOffset?> ResolveExpirationAsync(
        HttpContext context,
        CancellationToken cancellationToken = default
    );
}
