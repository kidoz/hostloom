using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HostLoom.Valkey;

internal static class ValkeyRegistration
{
    internal static void AddConnection(
        IServiceCollection services,
        Action<ValkeyOptions>? configure
    )
    {
        services
            .AddOptions<ValkeyOptions>()
            .Validate(static options =>
            {
                _ = options.Snapshot();
                return true;
            })
            .ValidateOnStart();
        if (configure is not null)
            services.Configure(configure);
        services.TryAddSingleton(static provider => new ValkeyConnection(
            provider.GetRequiredService<IOptions<ValkeyOptions>>().Value
        ));
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, ValkeyConnectionStarter>()
        );
    }
}

internal sealed class ValkeyConnectionStarter(
    ValkeyConnection connection,
    ILogger<ValkeyConnectionStarter>? logger = null
) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            await connection.PingAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
            when (!connection.Settings.FailFast && exception is not OperationCanceledException)
        {
            // Do not log SDK exception text: server errors can contain caller data.
            logger?.LogWarning(
                "Valkey startup PING failed ({Failure}); readiness is degraded until recovery.",
                exception.GetType().Name
            );
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
