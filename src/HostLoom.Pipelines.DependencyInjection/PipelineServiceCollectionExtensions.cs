using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace HostLoom.Pipelines.DependencyInjection;

public static class PipelineServiceCollectionExtensions
{
    /// <summary>
    /// Registers a named pipeline over <typeparamref name="TContext"/>. Filters are registered
    /// transient and resolved per run from a dedicated scope, so they can take repositories,
    /// producers, and loggers through their constructors. Every pipeline is validated and its
    /// topology logged when the host starts.
    /// </summary>
    public static IServiceCollection AddPipeline<TContext>(
        this IServiceCollection services,
        string name,
        Action<PipelineBuilder<TContext>> configure
    )
        where TContext : class, IPipeContext
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        if (
            services.Any(descriptor =>
                descriptor.ServiceType == typeof(IPipelineDefinition)
                && descriptor.ImplementationInstance is IPipelineDefinition existing
                && string.Equals(existing.Name, name, StringComparison.Ordinal)
            )
        )
        {
            throw new InvalidOperationException(
                $"A pipeline named '{name}' is already registered. Pipeline names must be unique per service collection."
            );
        }

        var builder = new PipelineBuilder<TContext>(name);
        configure(builder);
        var definition = builder.Build();

        foreach (var filterType in definition.FilterTypes)
        {
            // TryAdd would silently keep a conflicting registration, so a filter the caller had
            // already registered as a singleton would outlive the run it is documented to belong to
            // — sharing mutable state across concurrent runs, or capturing a scoped dependency.
            // Failing here is louder than either, and the caller can drop their registration.
            var existing = services.FirstOrDefault(descriptor =>
                descriptor.ServiceType == filterType
            );
            if (existing is null)
            {
                services.AddTransient(filterType);
                continue;
            }

            if (existing.Lifetime != ServiceLifetime.Transient)
            {
                throw new InvalidOperationException(
                    $"Filter '{filterType.Name}' in pipeline '{name}' is already registered as "
                        + $"{existing.Lifetime}, but pipeline filters are resolved per run and must be "
                        + "transient. Remove the existing registration, or register the filter as transient."
                );
            }
        }

        services.AddSingleton<IPipelineDefinition>(definition);
        services.AddKeyedSingleton<IPipelineRunner<TContext>>(
            name,
            (provider, _) =>
                new PipelineRunner<TContext>(
                    definition,
                    provider.GetRequiredService<IServiceScopeFactory>()
                )
        );
        services.TryAddSingleton<IPipelineRunner<TContext>>(ResolveSingle<TContext>);
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, PipelineStartupValidator>()
        );
        return services;
    }

    private static IPipelineRunner<TContext> ResolveSingle<TContext>(IServiceProvider provider)
        where TContext : class, IPipeContext
    {
        var names = provider
            .GetServices<IPipelineDefinition>()
            .Where(definition => definition.ContextType == typeof(TContext))
            .Select(definition => definition.Name)
            .ToList();
        return names.Count == 1
            ? provider.GetRequiredKeyedService<IPipelineRunner<TContext>>(names[0])
            : throw new InvalidOperationException(
                $"{names.Count} pipelines are registered for context '{typeof(TContext).Name}'"
                    + $"{(names.Count == 0 ? "" : $" ({string.Join(", ", names)})")}. "
                    + "Resolve the runner by its pipeline name as a keyed service."
            );
    }
}
