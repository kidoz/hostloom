using System.Security.Cryptography.X509Certificates;
using HostLoom.Redis;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.DataProtection.StackExchangeRedis;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace HostLoom.AspNetCore.DataProtection;

/// <summary>Configures ASP.NET Core Data Protection to share a certificate-protected key ring through Redis.</summary>
public static class DataProtectionServiceCollectionExtensions
{
    /// <summary>Uses Microsoft's Redis key repository on a HostLoom connection profile and encrypts newly generated keys with the supplied certificate.</summary>
    /// <param name="services">The application's services.</param>
    /// <param name="applicationName">Stable Data Protection application discriminator shared by compatible replicas.</param>
    /// <param name="redisKey">Exact Redis list key for the ring in the profile's configured database.</param>
    /// <param name="keyEncryptionCertificate">Application-owned RSA certificate. Replicas need access to the private key for decryption.</param>
    /// <param name="redisProfile">Existing or new named connection profile, or null for the default connection.</param>
    /// <param name="configureRedis">Connection settings when configuring this profile here. Omit when attaching to a configured profile.</param>
    public static IDataProtectionBuilder AddHostLoomDataProtection(
        this IServiceCollection services,
        string applicationName,
        string redisKey,
        X509Certificate2 keyEncryptionCertificate,
        string? redisProfile = null,
        Action<RedisOptions>? configureRedis = null
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(applicationName);
        ArgumentException.ThrowIfNullOrWhiteSpace(redisKey);
        ArgumentNullException.ThrowIfNull(keyEncryptionCertificate);

        services.AddHostLoomRedisConnection(redisProfile, configureRedis);
        var builder = services
            .AddDataProtection()
            .SetApplicationName(applicationName)
            .ProtectKeysWithCertificate(keyEncryptionCertificate);
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
