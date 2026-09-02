using System.Net.WebSockets;
using System.Security.Claims;
using Microsoft.Extensions.Logging;

namespace HostLoom.AspNetCore.WebSockets;

internal sealed class WebSocketSessionFactory(
    GatewayConfiguration configuration,
    WebSocketRequestRouter router,
    WebSocketSessionRegistry registry,
    TimeProvider timeProvider,
    ILogger<WebSocketSession> logger
)
{
    public WebSocketSession Create(
        WebSocket socket,
        IWebSocketHubProtocol protocol,
        ClaimsPrincipal user,
        DateTimeOffset? credentialExpiration = null
    )
    {
        var connectedAt = timeProvider.GetUtcNow();
        var remainingCalendar = DateTimeOffset.MaxValue - connectedAt;
        var maximumExpiration =
            configuration.Options.MaximumSessionLifetime >= remainingCalendar
                ? DateTimeOffset.MaxValue
                : connectedAt + configuration.Options.MaximumSessionLifetime;
        var credentialExpiresAt = credentialExpiration?.ToUniversalTime();
        var expiresAt =
            credentialExpiresAt is { } expiration && expiration < maximumExpiration
                ? expiration
                : maximumExpiration;
        var subject = user.FindFirstValue(configuration.Options.SubjectClaimType);
        if (string.IsNullOrWhiteSpace(subject))
        {
            subject = null;
        }
        return new(
            socket,
            protocol,
            user,
            configuration,
            router,
            registry,
            timeProvider,
            connectedAt,
            expiresAt,
            subject,
            logger
        );
    }
}
