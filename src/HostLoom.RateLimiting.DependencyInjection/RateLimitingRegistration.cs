using Microsoft.Extensions.Options;

namespace HostLoom.RateLimiting.DependencyInjection;

internal sealed class RateLimitingRegistration
{
    public string? Profile { get; init; }

    public string OptionsName => Profile ?? Options.DefaultName;

    public string? StoreName { get; set; }

    public bool HealthChecksAdded { get; set; }
}
