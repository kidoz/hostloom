using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Composition;

/// <summary>Applies explicit composition plans to the built-in DI abstractions.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>Applies a plan and returns the collection. Use ApplyTo when its report is needed.</summary>
    public static IServiceCollection AddHostLoomComposition(
        this IServiceCollection services,
        CompositionPlan plan
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(plan);
        plan.ApplyTo(services);
        return services;
    }
}
