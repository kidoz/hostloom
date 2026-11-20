using System.Security.Cryptography.X509Certificates;
using HostLoom.Redis;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.DataProtection.StackExchangeRedis;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace HostLoom.AspNetCore.DataProtection;

/// <summary>Configures ASP.NET Core Data Protection to share a key ring through Redis with optional certificate encryption.</summary>
public static class DataProtectionServiceCollectionExtensions
{
    /// <summary>Uses Microsoft's Redis key repository on a HostLoom connection profile and optionally encrypts newly generated keys with a certificate.</summary>
    /// <param name="services">The application's services.</param>
    /// <param name="applicationName">Stable Data Protection application discriminator shared by compatible replicas.</param>
    /// <param name="redisKey">Exact Redis list key for the ring in the profile's configured database.</param>
    /// <param name="keyEncryptionCertificate">Optional application-owned RSA certificate for encrypting new key-ring entries. When omitted, the application's Data Protection key-encryption configuration applies.</param>
    /// <param name="redisProfile">Existing or new named connection profile, or null for the default connection.</param>
    /// <param name="configureRedis">Connection settings when configuring this profile here. Omit when attaching to a configured profile.</param>
    public static IDataProtectionBuilder AddHostLoomDataProtection(
        this IServiceCollection services,
        string applicationName,
        string redisKey,
        X509Certificate2? keyEncryptionCertificate = null,
        string? redisProfile = null,
        Action<RedisOptions>? configureRedis = null
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(applicationName);
        ArgumentException.ThrowIfNullOrWhiteSpace(redisKey);

        services.AddHostLoomRedisConnection(redisProfile, configureRedis);
        var builder = services.AddDataProtection().SetApplicationName(applicationName);
        if (keyEncryptionCertificate is not null)
        {
            builder.ProtectKeysWithCertificate(keyEncryptionCertificate);
        }
        services
            .AddOptions<KeyManagementOptions>()
            .Configure<IServiceProvider>(
                (options, provider) =>
                {
                    var connection = redisProfile is null
                        ? provider.GetRequiredService<RedisConnection>()
                        : provider.GetRequiredKeyedService<RedisConnection>(redisProfile);
                    options.XmlRepository = new RedisXmlRepository(
                        () => GetDatabase(connection),
                        redisKey
                    );
                }
            );
        return builder;
    }

    private static IDatabase GetDatabase(RedisConnection connection)
    {
        // Microsoft's repository has a synchronous database factory. On a cold connection,
        // an application-supplied async Redis factory must be free to run its continuations.
        Task<IDatabase> ConnectAsync() => connection.GetDatabaseAsync().AsTask();
        var task =
            SynchronizationContext.Current is null && TaskScheduler.Current == TaskScheduler.Default
                ? ConnectAsync()
                : Task.Run(ConnectAsync);
        return task.GetAwaiter().GetResult();
    }
}
