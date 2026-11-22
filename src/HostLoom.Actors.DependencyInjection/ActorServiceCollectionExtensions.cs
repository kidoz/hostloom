using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Actors.DependencyInjection;

public static class ActorServiceCollectionExtensions
{
    /// <summary>Register one actor system for the host. Register its actors before building the host.</summary>
    public static IServiceCollection AddLocalActorSystem(
        this IServiceCollection services,
        int maxActors,
        int mailboxCapacity,
        TimeSpan shutdownGracePeriod
    )
    {
        if (
            services.Any(service =>
                service.ServiceType == typeof(ActorSystem) && !service.IsKeyedService
            )
        )
            throw new InvalidOperationException("The local actor system is already registered.");
        services.AddSingleton(provider => new ActorSystem(
            maxActors,
            mailboxCapacity,
            shutdownGracePeriod,
            provider.GetService<TimeProvider>()
        ));
        services.AddHostedService<ActorHostedService>();
        return services;
    }

    /// <summary>Starts one scoped implementation and exposes its typed reference when host startup completes.</summary>
    public static IServiceCollection AddLocalActor<
        TMessage,
        TReply,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TActor
    >(this IServiceCollection services)
        where TActor : class, IActor<TMessage, TReply>
    {
        if (
            services.Any(service =>
                service.ServiceType == typeof(IActorRef<TMessage, TReply>)
                && !service.IsKeyedService
            )
        )
            throw new InvalidOperationException(
                "An actor reference for this message and reply type is already registered."
            );
        // A private key gives this registration its own scoped instance even if the application
        // also registers TActor with another lifetime.
        var key = new object();
        services.AddKeyedScoped<IActor<TMessage, TReply>, TActor>(key);
        services.AddSingleton(provider => new ActorRegistration<TMessage, TReply>(
            provider.GetRequiredService<IServiceScopeFactory>(),
            key
        ));
        services.AddSingleton<IActorRegistration>(provider =>
            provider.GetRequiredService<ActorRegistration<TMessage, TReply>>()
        );
        services.AddSingleton<IActorRef<TMessage, TReply>>(provider =>
            provider.GetRequiredService<ActorRegistration<TMessage, TReply>>().Reference
        );
        services.AddSingleton(provider =>
            provider.GetRequiredService<ActorRegistration<TMessage, TReply>>().Reference.Handle
        );
        return services;
    }
}
