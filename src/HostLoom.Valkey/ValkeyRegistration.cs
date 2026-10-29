using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ValkeyDotNet;

namespace HostLoom.Valkey;

internal static class ValkeyRegistration
{
    internal static void AddConnection(
        IServiceCollection services,
        string? profile,
        string capability,
        Action<ValkeyOptions>? configure
    )
    {
        var registration = Find(services, profile);
        if (registration is not null)
        {
            if (configure is null)
            {
                return;
            }

            var configured = new ValkeyOptions();
            configure(configured);
            if (!registration.IsConfigured)
            {
                registration.Options = configured;
                registration.IsConfigured = true;
                services.Configure(OptionsName(profile), configure);
                return;
            }

            var conflicts = Conflicts(registration.Options, configured);
            if (conflicts.Count > 0)
            {
                throw new InvalidOperationException(
                    $"Valkey connection settings conflict between {registration.Owner} and {Describe(capability, profile)}: "
                        + $"{string.Join(", ", conflicts)}. Configure the shared profile once or attach without settings."
                );
            }

            return;
        }

        registration = new ValkeyConnectionRegistration(profile, capability);
        if (configure is not null)
        {
            configure(registration.Options);
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

        var options = services
            .AddOptions<ValkeyOptions>(OptionsName(profile))
            .Validate(static options =>
            {
                _ = options.Snapshot();
                return true;
            })
            .ValidateOnStart();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        if (profile is null)
        {
            services.TryAddSingleton(static provider => new ValkeyConnection(
                provider.GetRequiredService<IOptions<ValkeyOptions>>().Value
            ));
            services.TryAddEnumerable(
                ServiceDescriptor.Singleton<IHostedService, ValkeyConnectionStarter>()
            );
        }
        else
        {
            services.TryAddKeyedSingleton(
                profile,
                static (provider, key) =>
                    new ValkeyConnection(
                        provider
                            .GetRequiredService<IOptionsMonitor<ValkeyOptions>>()
                            .Get((string)key!)
                    )
            );
            services.AddSingleton<IHostedService>(provider => new ValkeyConnectionStarter(
                provider.GetRequiredKeyedService<ValkeyConnection>(profile),
                provider.GetService<ILogger<ValkeyConnectionStarter>>()
            ));
        }
    }

    private static List<string> Conflicts(ValkeyOptions current, ValkeyOptions other)
    {
        List<string> conflicts = [];
        AddIf(
            conflicts,
            !ConnectionEquals(current.Connection, other.Connection),
            "Valkey:Connection"
        );
        AddIf(
            conflicts,
            current.ConnectionOwner != other.ConnectionOwner,
            "Valkey:ConnectionOwner"
        );
        AddIf(conflicts, current.CommandTimeout != other.CommandTimeout, "Valkey:CommandTimeout");
        AddIf(conflicts, current.HealthTimeout != other.HealthTimeout, "Valkey:HealthTimeout");
        AddIf(conflicts, current.FailFast != other.FailFast, "Valkey:FailFast");
        AddIf(
            conflicts,
            current.InvalidationQueueCapacity != other.InvalidationQueueCapacity,
            "Valkey:InvalidationQueueCapacity"
        );
        AddIf(
            conflicts,
            current.InvalidationProbeInterval != other.InvalidationProbeInterval,
            "Valkey:InvalidationProbeInterval"
        );
        AddIf(
            conflicts,
            current.EnableTelemetry != other.EnableTelemetry,
            "Valkey:EnableTelemetry"
        );
        return conflicts;
    }

    private static bool ConnectionEquals(ValkeyClientOptions? left, ValkeyClientOptions? right) =>
        ReferenceEquals(left, right)
        || (
            left is not null
            && right is not null
            && left.Host == right.Host
            && left.Port == right.Port
            && left.Username == right.Username
            && left.Password == right.Password
            && left.ClientName == right.ClientName
            && left.Database == right.Database
            && left.Protocol == right.Protocol
            && left.UseTls == right.UseTls
            && left.CertificateValidationCallback == right.CertificateValidationCallback
            && left.ConnectTimeout == right.ConnectTimeout
            && left.ResponseDrainTimeout == right.ResponseDrainTimeout
            && left.MaxPendingRequests == right.MaxPendingRequests
            && left.MaxResponseBytes == right.MaxResponseBytes
            && left.MaxResponseElements == right.MaxResponseElements
            && left.MaxNestingDepth == right.MaxNestingDepth
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

    private static ValkeyConnectionRegistration? Find(IServiceCollection services, string? profile)
    {
        foreach (var descriptor in services)
        {
            if (descriptor.ServiceType != typeof(ValkeyConnectionRegistration))
            {
                continue;
            }

            var instance = descriptor.IsKeyedService
                ? descriptor.KeyedImplementationInstance
                : descriptor.ImplementationInstance;
            if (
                instance is ValkeyConnectionRegistration registration
                && registration.Profile == profile
            )
            {
                return registration;
            }
        }

        return null;
    }
}

internal sealed class ValkeyConnectionRegistration(string? profile, string capability)
{
    public string? Profile { get; } = profile;

    public string Owner { get; } = $"{capability} profile '{profile ?? "(default)"}'";

    public ValkeyOptions Options { get; set; } = new();

    public bool IsConfigured { get; set; }
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
