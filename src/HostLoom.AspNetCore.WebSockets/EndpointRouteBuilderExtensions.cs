using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.AspNetCore.WebSockets;

public static class EndpointRouteBuilderExtensions
{
    public static IEndpointConventionBuilder MapHostLoomWebSocketHub(
        this IEndpointRouteBuilder endpoints,
        string pattern = "/hostloom"
    )
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(pattern);

        var configuration = endpoints.ServiceProvider.GetRequiredService<GatewayConfiguration>();
        var route = endpoints.MapGet(pattern, HandleAsync);
        return configuration.Options.RequireAuthenticatedUser
            ? route.RequireAuthorization()
            : route;
    }

    private static async Task HandleAsync(HttpContext context)
    {
        var configuration = context.RequestServices.GetRequiredService<GatewayConfiguration>();
        if (
            configuration.Options.RequireAuthenticatedUser
            && context.User.Identity?.IsAuthenticated is not true
        )
        {
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            return;
        }

        if (!context.WebSockets.IsWebSocketRequest)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context
                .Response.WriteAsync("A WebSocket upgrade is required.", context.RequestAborted)
                .ConfigureAwait(false);
            return;
        }

        var protocols = context
            .RequestServices.GetServices<IWebSocketHubProtocol>()
            .ToDictionary(static protocol => protocol.SubProtocol, StringComparer.Ordinal);
        var requested = context.WebSockets.WebSocketRequestedProtocols;
        var selected = configuration
            .Options.ProtocolPreference.Where(protocols.ContainsKey)
            .FirstOrDefault(requested.Contains);
        if (selected is null)
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context
                .Response.WriteAsync(
                    "A supported HostLoom WebSocket subprotocol is required.",
                    context.RequestAborted
                )
                .ConfigureAwait(false);
            return;
        }

        using var socket = await context
            .WebSockets.AcceptWebSocketAsync(selected)
            .ConfigureAwait(false);
        var factory = context.RequestServices.GetRequiredService<WebSocketSessionFactory>();
        var session = factory.Create(socket, protocols[selected], context.User);
        await session.RunAsync(context.RequestAborted).ConfigureAwait(false);
    }
}
