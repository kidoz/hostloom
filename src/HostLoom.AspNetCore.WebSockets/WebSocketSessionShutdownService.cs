using System.Net.WebSockets;
using Microsoft.Extensions.Hosting;

namespace HostLoom.AspNetCore.WebSockets;

/// <summary>
/// Closes every gateway session with 1001 <c>server_shutdown</c> while the host stops. Closing
/// starts in <see cref="StoppingAsync"/>, which the host calls on every lifecycle service before
/// any service stops. A <c>WebApplication</c> registers its web server last, so the server stops
/// first and waits for every upgraded request; sessions that only started closing in
/// <see cref="StopAsync"/> would hold that wait open until the shutdown timeout aborted them.
/// </summary>
internal sealed class WebSocketSessionShutdownService(WebSocketSessionRegistry registry)
    : IHostedLifecycleService
{
    private const WebSocketCloseStatus Status = WebSocketCloseStatus.EndpointUnavailable;
    private const string Reason = "server_shutdown";

    public Task StartingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppingAsync(CancellationToken cancellationToken)
    {
        registry.BeginShutdown(Status, Reason);
        return Task.CompletedTask;
    }

    // Joins every session, including any accepted after StoppingAsync, before the broker
    // listeners registered ahead of this service stop.
    public Task StopAsync(CancellationToken cancellationToken) =>
        registry.DisconnectAllAsync(Status, Reason, cancellationToken);

    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
