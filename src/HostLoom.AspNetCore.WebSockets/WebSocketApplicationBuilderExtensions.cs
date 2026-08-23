using Microsoft.AspNetCore.Builder;

namespace HostLoom.AspNetCore.WebSockets;

public static class WebSocketApplicationBuilderExtensions
{
    /// <summary>Adds ASP.NET Core WebSocket upgrade handling with production-oriented keep-alive defaults.</summary>
    public static IApplicationBuilder UseHostLoomWebSockets(this IApplicationBuilder application)
    {
        ArgumentNullException.ThrowIfNull(application);
        return application.UseWebSockets(
            new WebSocketOptions
            {
                KeepAliveInterval = TimeSpan.FromSeconds(20),
                KeepAliveTimeout = TimeSpan.FromSeconds(10),
            }
        );
    }
}
