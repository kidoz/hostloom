using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HostLoom.Redis;

/// <summary>What both <c>UseRedis</c> extensions share: options, validation, the one connection, and its startup.</summary>
internal static class RedisRegistration
{
    public static void AddConnection(IServiceCollection services, Action<RedisOptions>? configure)
    {
        var registered = false;
        foreach (var descriptor in services)
        {
            if (descriptor.ServiceType == typeof(RedisConnection))
            {
                registered = true;
                break;
            }
        }

        if (!registered)
        {
            services.AddOptions<RedisOptions>().ValidateOnStart();
            services.TryAddEnumerable(
                ServiceDescriptor.Singleton<IValidateOptions<RedisOptions>, RedisOptionsValidator>()
            );
            services.TryAddSingleton(static provider => new RedisConnection(
                provider.GetRequiredService<IOptions<RedisOptions>>().Value,
                provider.GetService<ILoggerFactory>()?.CreateLogger<RedisConnection>()
            ));
            services.TryAddEnumerable(
                ServiceDescriptor.Singleton<IHostedService, RedisConnectionStarter>()
            );
        }

        if (configure is not null)
        {
            services.Configure(configure);
        }
    }
}

/// <summary>Runs <see cref="RedisOptions.Validate"/> at startup.</summary>
internal sealed class RedisOptionsValidator : IValidateOptions<RedisOptions>
{
    public ValidateOptionsResult Validate(string? name, RedisOptions options)
    {
        var problems = options.Validate();
        return problems.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(problems);
    }
}

/// <summary>
/// Establishes the connection when the host starts and logs where it points, with credentials
/// redacted. With <see cref="RedisOptions.FailFast"/> an unreachable Redis fails startup;
/// otherwise the service starts and the cache and lock report degraded until Redis recovers.
/// </summary>
internal sealed class RedisConnectionStarter(
    RedisConnection connection,
    ILogger<RedisConnectionStarter> logger
) : IHostedService
{
    private static readonly EventId Ready = new(1303, "RedisConnectionReady");

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            await connection.GetMultiplexerAsync(cancellationToken).ConfigureAwait(false);
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation(
                    Ready,
                    "Redis connection {State}: {Description}.",
                    connection.IsConnected
                        ? "established"
                        : "pending (reconnecting in the background)",
                    connection.Describe()
                );
            }
        }
        catch (Exception exception)
            when (!connection.Options.FailFast && exception is not OperationCanceledException)
        {
            logger.LogWarning(
                new EventId(1304, "RedisUnreachableAtStartup"),
                exception,
                "Redis is unreachable at startup ({Description}); Redis:FailFast is false, so the service starts, readiness reports unhealthy, and caches serve from factories until it recovers.",
                connection.Describe()
            );
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
