using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace HostLoom.Scheduling.DependencyInjection;

/// <summary>Registers schedules and chooses the guard for one service collection.</summary>
public sealed class SchedulingBuilder
{
    private readonly SchedulingRegistration _registration;

    internal SchedulingBuilder(IServiceCollection services, SchedulingRegistration registration)
    {
        Services = services;
        _registration = registration;
    }

    /// <summary>The service collection receiving scheduling registrations.</summary>
    public IServiceCollection Services { get; }

    /// <summary>
    /// Registers a schedule whose job is <typeparamref name="TJob"/>, resolved from a fresh
    /// dependency-injection scope for every run, so a job takes scoped dependencies through its
    /// constructor like a request handler does. The job type is registered scoped if nothing
    /// registered it already.
    /// </summary>
    /// <param name="name">The schedule name, unique within the service collection.</param>
    /// <param name="trigger">When the schedule runs.</param>
    /// <param name="configure">Per-schedule settings.</param>
    /// <exception cref="InvalidOperationException">The name was already registered.</exception>
    public SchedulingBuilder AddSchedule<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TJob
    >(string name, ScheduleTrigger trigger, Action<ScheduleOptions>? configure = null)
        where TJob : class, IScheduledJob
    {
        Services.TryAddScoped<TJob>();
        return AddSchedule(
            name,
            trigger,
            static async (provider, run, cancellationToken) =>
            {
                var scope = provider.GetRequiredService<IServiceScopeFactory>().CreateAsyncScope();
                await using (scope.ConfigureAwait(false))
                {
                    await scope
                        .ServiceProvider.GetRequiredService<TJob>()
                        .ExecuteAsync(run, cancellationToken)
                        .ConfigureAwait(false);
                }
            },
            configure
        );
    }

    /// <summary>
    /// Registers a schedule whose work is <paramref name="run"/>. The delegate receives the root
    /// provider; create a scope inside it when the work needs scoped services.
    /// </summary>
    /// <param name="name">The schedule name, unique within the service collection.</param>
    /// <param name="trigger">When the schedule runs.</param>
    /// <param name="run">The work.</param>
    /// <param name="configure">Per-schedule settings.</param>
    /// <exception cref="InvalidOperationException">The name was already registered.</exception>
    public SchedulingBuilder AddSchedule(
        string name,
        ScheduleTrigger trigger,
        Func<IServiceProvider, ScheduledRun, CancellationToken, ValueTask> run,
        Action<ScheduleOptions>? configure = null
    )
    {
        ScheduleName.Validate(name);
        ArgumentNullException.ThrowIfNull(trigger);
        ArgumentNullException.ThrowIfNull(run);
        var options = new ScheduleOptions();
        configure?.Invoke(options);

        // Validated here as well as in the definition, so a bad option fails at registration
        // rather than when the host resolves the scheduler.
        var problems = options.Validate(name);
        if (problems.Count > 0)
        {
            throw new ArgumentException(string.Join(" ", problems), nameof(configure));
        }

        if (!_registration.Names.Add(name))
        {
            throw new InvalidOperationException(
                $"A schedule named '{name}' is already registered. Schedule names are unique "
                    + "within a service collection."
            );
        }

        if (options.Exclusive)
        {
            _registration.ExclusiveSchedules.Add(name);
        }

        // A plain AddSingleton: TryAddEnumerable compares factory return types, which would keep
        // only the first definition.
        Services.AddSingleton(provider => new ScheduleDefinition(
            name,
            trigger,
            (scheduledRun, cancellationToken) => run(provider, scheduledRun, cancellationToken),
            options
        ));
        return this;
    }

    /// <summary>
    /// Uses <typeparamref name="TGuard"/> as the one guard for this service collection, the
    /// analogue of <c>LockingBuilder.UseProvider</c>. Adapter packages call this from their own
    /// <c>Use*</c> extension.
    /// </summary>
    /// <param name="name">How the choice is reported by the exactly-one rule and the probe.</param>
    /// <exception cref="InvalidOperationException">
    /// A guard was already chosen, or <see cref="IScheduleGuard"/> was already registered by
    /// something else, such as a test double left in the composition root; the message names
    /// both. To stand in for the chosen guard deliberately, register the double after this call,
    /// or register the guard type itself before it and choose that type here.
    /// </exception>
    public SchedulingBuilder UseGuard<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TGuard
    >(string name)
        where TGuard : class, IScheduleGuard
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (_registration.GuardName is { } existing)
        {
            throw new InvalidOperationException(
                $"HostLoom scheduling already uses the '{existing}' guard, so '{name}' cannot be "
                    + "chosen as well. Configure exactly one schedule guard per service collection."
            );
        }

        // A TryAdd would keep an earlier registration and let it win silently while the probe
        // reported the guard chosen here; refuse instead and name what is there.
        if (FindForeignGuard(Services) is { } foreign)
        {
            throw new InvalidOperationException(
                $"HostLoom scheduling cannot choose the '{name}' guard: IScheduleGuard is already "
                    + $"registered as {Describe(foreign)}, which would have taken precedence. "
                    + "Remove that registration, or register it after the builder to replace the "
                    + "chosen guard deliberately."
            );
        }

        _registration.GuardName = name;
        Services.TryAddSingleton<TGuard>();
        Services.TryAddSingleton<IScheduleGuard>(static provider =>
            provider.GetRequiredService<TGuard>()
        );
        return this;
    }

    private static ServiceDescriptor? FindForeignGuard(IServiceCollection services)
    {
        for (var i = 0; i < services.Count; i++)
        {
            var descriptor = services[i];
            if (descriptor.ServiceType == typeof(IScheduleGuard) && !descriptor.IsKeyedService)
            {
                return descriptor;
            }
        }

        return null;
    }

    private static string Describe(ServiceDescriptor descriptor)
    {
        var type = descriptor.ImplementationType ?? descriptor.ImplementationInstance?.GetType();
        return type is null
            ? "a factory registration"
            : (type.FullName ?? type.Name)
                + (descriptor.ImplementationInstance is null ? "" : " (an instance)");
    }
}
