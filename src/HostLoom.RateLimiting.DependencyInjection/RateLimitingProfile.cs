using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.RateLimiting.DependencyInjection;

internal static class RateLimitingProfile
{
    public static string HealthName(string name, string? profile) =>
        profile is null ? name : $"{name}-{profile}";

    public static T? Get<T>(IServiceProvider provider, string? profile)
        where T : class =>
        profile is null ? provider.GetService<T>() : provider.GetKeyedService<T>(profile);

    public static T GetRequired<T>(IServiceProvider provider, string? profile)
        where T : class =>
        profile is null
            ? provider.GetRequiredService<T>()
            : provider.GetRequiredKeyedService<T>(profile);
}
