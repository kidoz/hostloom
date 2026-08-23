using System.Net.WebSockets;
using System.Security.Claims;

namespace HostLoom.AspNetCore.WebSockets;

internal sealed class WebSocketSessionFactory(
    GatewayConfiguration configuration,
    WebSocketRequestRouter router,
    WebSocketSessionRegistry registry
)
{
    public WebSocketSession Create(
        WebSocket socket,
        IWebSocketHubProtocol protocol,
        ClaimsPrincipal user
    ) => new(socket, protocol, user, configuration, router, registry);
}
