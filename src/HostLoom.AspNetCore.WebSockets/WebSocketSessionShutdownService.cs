using System.Net.WebSockets;
using Microsoft.Extensions.Hosting;

namespace HostLoom.AspNetCore.WebSockets;

internal sealed class WebSocketSessionShutdownService(WebSocketSessionRegistry registry)
    : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) =>
        registry.DisconnectAllAsync(
            WebSocketCloseStatus.EndpointUnavailable,
            "server_shutdown",
            cancellationToken
        );
}
