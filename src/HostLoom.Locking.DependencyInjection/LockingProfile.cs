using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace HostLoom.Locking.DependencyInjection;

internal static class LockingProfile
{
    public static string OptionsName(string? profile) => profile ?? Options.DefaultName;

    public static string HealthName(string component, string? profile) =>
        profile is null ? component : $"{component}:{profile}";

    public static T? Get<T>(IServiceProvider provider, string? profile)
        where T : class =>
        profile is null ? provider.GetService<T>() : provider.GetKeyedService<T>(profile);

    public static T GetRequired<T>(IServiceProvider provider, string? profile)
        where T : class =>
        profile is null
            ? provider.GetRequiredService<T>()
            : provider.GetRequiredKeyedService<T>(profile);

    public static string PrefixValidation(string? profile, IEnumerable<string> problems) =>
        profile is null
            ? string.Join(" ", problems)
            : $"Profile '{profile}': {string.Join(" ", problems)}";
}
