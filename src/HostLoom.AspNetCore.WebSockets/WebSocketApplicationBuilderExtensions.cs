using Microsoft.AspNetCore.Builder;

namespace HostLoom.AspNetCore.WebSockets;

public static class WebSocketApplicationBuilderExtensions
{
    /// <summary>
    /// Adds ASP.NET Core WebSocket upgrade handling with a 20-second keep-alive interval and a
    /// 10-second Pong timeout.
    /// </summary>
    public static IApplicationBuilder UseHostLoomWebSockets(this IApplicationBuilder application) =>
        application.UseHostLoomWebSockets(
            new WebSocketOptions
            {
                KeepAliveInterval = TimeSpan.FromSeconds(20),
                KeepAliveTimeout = TimeSpan.FromSeconds(10),
            }
        );

    /// <summary>Adds ASP.NET Core WebSocket upgrade handling with caller-supplied options.</summary>
    /// <remarks>
    /// Do not call this helper when the application already called ASP.NET Core
    /// <see cref="WebSocketMiddlewareExtensions.UseWebSockets(IApplicationBuilder, WebSocketOptions)"/>.
    /// Middleware-presence detection is not reliable during pipeline composition.
    /// </remarks>
    public static IApplicationBuilder UseHostLoomWebSockets(
        this IApplicationBuilder application,
        WebSocketOptions options
    )
    {
        ArgumentNullException.ThrowIfNull(application);
        ArgumentNullException.ThrowIfNull(options);
        return application.UseWebSockets(options);
    }
}
