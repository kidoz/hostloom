using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace HostLoom.AspNetCore.WebSockets;

/// <summary>
/// Proves at host start that every authorization policy a request or topic references can be
/// resolved, so a misspelled or unregistered name fails the deployment instead of surfacing as an
/// exception on the first client that reaches the route.
/// </summary>
internal sealed class WebSocketGatewayStartupValidation(
    GatewayConfiguration configuration,
    IServiceScopeFactory scopeFactory
) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var usages = configuration.GetAuthorizationPolicyUsages();
        if (usages.Count == 0)
        {
            return;
        }

        var scope = scopeFactory.CreateAsyncScope();
        try
        {
            var policies = scope.ServiceProvider.GetRequiredService<IAuthorizationPolicyProvider>();
            var missing = new List<string>();
            foreach (var usage in usages)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (await policies.GetPolicyAsync(usage.Policy).ConfigureAwait(false) is null)
                {
                    missing.Add($"'{usage.Policy}' (used by {string.Join(", ", usage.Routes)})");
                }
            }

            if (missing.Count > 0)
            {
                throw new InvalidOperationException(
                    "The WebSocket gateway references authorization policies that are not registered: "
                        + string.Join("; ", missing)
                        + ". Register each policy with AddAuthorization before the host starts."
                );
            }
        }
        finally
        {
            await scope.DisposeAsync().ConfigureAwait(false);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
