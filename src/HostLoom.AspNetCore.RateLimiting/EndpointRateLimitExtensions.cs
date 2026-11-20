using System.Security.Claims;
using HostLoom.RateLimiting;
using HostLoom.RateLimiting.DependencyInjection;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.AspNetCore.RateLimiting;

/// <summary>Registers incoming HTTP policies over HostLoom rate limiter profiles.</summary>
public static class EndpointRateLimitExtensions
{
    /// <summary>Adds an endpoint policy that reads the tenant claim from the primary authenticated identity.</summary>
    public static RateLimitingBuilder AddTenantRateLimitPolicy(
        this RateLimitingBuilder builder,
        string tenantClaimType,
        Func<OnRejectedContext, CancellationToken, ValueTask>? onRejected = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantClaimType);
        return builder.AddEndpointRateLimitPolicy(
            context =>
            {
                if (context.User.Identity is not ClaimsIdentity { IsAuthenticated: true } identity)
                    return null;

                string? tenant = null;
                foreach (var claim in identity.FindAll(tenantClaimType))
                {
                    if (
                        tenant is not null
                        && !string.Equals(tenant, claim.Value, StringComparison.Ordinal)
                    )
                        return null;
                    tenant = claim.Value;
                }
                return tenant;
            },
            onRejected
        );
    }

    /// <summary>Adds an endpoint policy named after this profile. The selector supplies a bounded, non-secret store partition per request.
    /// A missing or invalid key rejects the request before contacting the store. The optional callback can override the default rejection response.</summary>
    public static RateLimitingBuilder AddEndpointRateLimitPolicy(
        this RateLimitingBuilder builder,
        Func<HttpContext, string?> partitionKeySelector,
        Func<OnRejectedContext, CancellationToken, ValueTask>? onRejected = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(partitionKeySelector);
        var profile =
            builder.Profile
            ?? throw new InvalidOperationException(
                "An endpoint policy requires a named rate limiter profile."
            );
        builder.Services.AddHttpContextAccessor();
        builder.Services.AddRateLimiter();
        builder
            .Services.AddOptions<RateLimiterOptions>()
            .Configure<IServiceProvider>(
                (options, services) =>
                    options.AddPolicy(
                        profile,
                        new EndpointRateLimitPolicy(
                            services.GetRequiredKeyedService<DistributedRateLimiter>(profile),
                            services.GetRequiredService<IHttpContextAccessor>(),
                            partitionKeySelector,
                            onRejected
                        )
                    )
            );
        return builder;
    }
}
