using System.Threading.RateLimiting;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.RateLimiting.HttpClient;

/// <summary>Outgoing HTTP registration for HostLoom rate limiters.</summary>
public static class HttpClientRateLimitingExtensions
{
    /// <summary>
    /// Applies the exact named limiter to every physical request attempt. Register this after
    /// retry or hedging handlers to place it closest to the network.
    /// </summary>
    public static IHttpClientBuilder AddHostLoomRateLimit(
        this IHttpClientBuilder builder,
        string profile,
        Action<HttpRateLimitOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        RateLimitKey.Validate(profile);
        var options = new HttpRateLimitOptions();
        configure?.Invoke(options);
        options.ThrowIfInvalid();
        builder.AddHttpMessageHandler(provider => new RateLimitedHttpMessageHandler(
            provider.GetRequiredKeyedService<RateLimiter>(profile),
            options,
            provider.GetService<TimeProvider>() ?? TimeProvider.System
        ));
        return builder;
    }
}
