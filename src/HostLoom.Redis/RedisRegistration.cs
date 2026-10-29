using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HostLoom.Redis;

/// <summary>What both <c>UseRedis</c> extensions share: options, validation, one connection per profile, and its startup.</summary>
internal static class RedisRegistration
{
    public static void AddConnection(
        IServiceCollection services,
        string? profile,
        string capability,
        Action<RedisOptions>? configure
    )
    {
        var registration = Find(services, profile);
        if (registration is not null)
        {
            if (configure is null)
            {
                return;
            }

            var configured = Configure(configure, out var clientNameConfigured);
            if (!registration.IsConfigured)
            {
                registration.Options = configured;
                registration.ClientNameConfigured = clientNameConfigured;
                registration.IsConfigured = true;
                services.Configure(OptionsName(profile), configure);
                return;
            }

            var conflicts = Conflicts(registration, configured, clientNameConfigured);
            if (conflicts.Count > 0)
            {
                throw new InvalidOperationException(
                    $"Redis connection settings conflict between {registration.Owner} and {Describe(capability, profile)}: "
                        + $"{string.Join(", ", conflicts)}. Configure the shared profile once or attach without settings."
                );
            }

            return;
        }

        registration = new RedisConnectionRegistration(profile, capability);
        if (configure is not null)
        {
            registration.Options = Configure(configure, out var clientNameConfigured);
            registration.ClientNameConfigured = clientNameConfigured;
            registration.IsConfigured = true;
        }

        if (profile is null)
        {
            services.AddSingleton(registration);
        }
        else
        {
            services.AddKeyedSingleton(profile, registration);
        }

        var options = services.AddOptions<RedisOptions>(OptionsName(profile)).ValidateOnStart();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<RedisOptions>, RedisOptionsValidator>()
        );
        if (profile is null)
        {
            services.TryAddSingleton(static provider => new RedisConnection(
                provider.GetRequiredService<IOptions<RedisOptions>>().Value,
                provider.GetService<ILoggerFactory>()?.CreateLogger<RedisConnection>()
            ));
            services.TryAddEnumerable(
                ServiceDescriptor.Singleton<IHostedService, RedisConnectionStarter>()
            );
        }
        else
        {
            services.TryAddKeyedSingleton(
                profile,
                static (provider, key) =>
                    new RedisConnection(
                        provider
                            .GetRequiredService<IOptionsMonitor<RedisOptions>>()
                            .Get((string)key!),
                        provider.GetService<ILoggerFactory>()?.CreateLogger<RedisConnection>()
                    )
            );
            services.AddSingleton<IHostedService>(provider => new RedisConnectionStarter(
                provider.GetRequiredKeyedService<RedisConnection>(profile),
                provider.GetService<ILoggerFactory>()?.CreateLogger<RedisConnectionStarter>()
                    ?? Microsoft
                        .Extensions
                        .Logging
                        .Abstractions
                        .NullLogger<RedisConnectionStarter>
                        .Instance
            ));
        }
    }

    private static RedisOptions Configure(
        Action<RedisOptions> configure,
        out bool clientNameConfigured
    )
    {
        var options = new RedisOptions();
        var defaultClientName = options.ClientName;
        configure(options);
        clientNameConfigured = !string.Equals(
            options.ClientName,
            defaultClientName,
            StringComparison.Ordinal
        );
        return options;
    }

    private static List<string> Conflicts(
        RedisConnectionRegistration registration,
        RedisOptions other,
        bool otherClientNameConfigured
    )
    {
        var current = registration.Options;
        List<string> conflicts = [];
        AddIf(
            conflicts,
            !string.Equals(current.Configuration, other.Configuration, StringComparison.Ordinal),
            "Redis:Configuration"
        );
        AddIf(
            conflicts,
            !ConfigurationEquals(current.ConfigurationOptions, other.ConfigurationOptions),
            "Redis:ConfigurationOptions"
        );
        AddIf(
            conflicts,
            current.ConnectionFactory != other.ConnectionFactory,
            "Redis:ConnectionFactory"
        );
        AddIf(conflicts, current.DatabaseIndex != other.DatabaseIndex, "Redis:DatabaseIndex");
        AddIf(conflicts, current.UseHashTags != other.UseHashTags, "Redis:UseHashTags");
        AddIf(conflicts, current.ConnectTimeout != other.ConnectTimeout, "Redis:ConnectTimeout");
        AddIf(conflicts, current.CommandTimeout != other.CommandTimeout, "Redis:CommandTimeout");
        AddIf(conflicts, current.HealthTimeout != other.HealthTimeout, "Redis:HealthTimeout");
        AddIf(conflicts, current.FailFast != other.FailFast, "Redis:FailFast");
        AddIf(
            conflicts,
            registration.ClientNameConfigured != otherClientNameConfigured
                || (
                    registration.ClientNameConfigured
                    && !string.Equals(
                        current.ClientName,
                        other.ClientName,
                        StringComparison.Ordinal
                    )
                ),
            "Redis:ClientName"
        );
        AddIf(
            conflicts,
            current.MaxClientCommandRetries != other.MaxClientCommandRetries,
            "Redis:MaxClientCommandRetries"
        );
        AddIf(
            conflicts,
            current.InitialRetryDelay != other.InitialRetryDelay,
            "Redis:InitialRetryDelay"
        );
        return conflicts;
    }

    private static bool ConfigurationEquals(
        StackExchange.Redis.ConfigurationOptions? left,
        StackExchange.Redis.ConfigurationOptions? right
    ) =>
        ReferenceEquals(left, right)
        || (
            left is not null
            && right is not null
            && string.Equals(
                left.ToString(includePassword: true),
                right.ToString(includePassword: true),
                StringComparison.Ordinal
            )
        );

    private static void AddIf(List<string> conflicts, bool condition, string name)
    {
        if (condition)
        {
            conflicts.Add(name);
        }
    }

    private static string Describe(string capability, string? profile) =>
        $"{capability} profile '{profile ?? "(default)"}'";

    private static string OptionsName(string? profile) => profile ?? Options.DefaultName;

    private static RedisConnectionRegistration? Find(IServiceCollection services, string? profile)
    {
        foreach (var descriptor in services)
        {
            if (descriptor.ServiceType != typeof(RedisConnectionRegistration))
            {
                continue;
            }

            var instance = descriptor.IsKeyedService
                ? descriptor.KeyedImplementationInstance
                : descriptor.ImplementationInstance;
            if (
                instance is RedisConnectionRegistration registration
                && registration.Profile == profile
            )
            {
                return registration;
            }
        }

        return null;
    }
}

internal sealed class RedisConnectionRegistration(string? profile, string capability)
{
    public string? Profile { get; } = profile;

    public string Owner { get; } = $"{capability} profile '{profile ?? "(default)"}'";

    public RedisOptions Options { get; set; } = new();

    public bool ClientNameConfigured { get; set; }

    public bool IsConfigured { get; set; }
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
            when (!connection.Options.FailFast
                && exception is not OperationCanceledException and not ArgumentException
            )
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
