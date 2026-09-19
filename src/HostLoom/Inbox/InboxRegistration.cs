using System.Diagnostics.CodeAnalysis;
using HostLoom.Pipelines;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace HostLoom;

/// <summary>Adds the inbox filter to the receive pipeline.</summary>
public static class InboxHostLoomBuilderExtensions
{
    /// <summary>
    /// Runs each event's handlers at most once per (topic, subscription, message id) inside
    /// <paramref name="window"/>, recording deliveries in <typeparamref name="TStore"/>. The
    /// filter is appended to the receive pipeline at this point in registration order, so call
    /// it before <c>ConfigureReceivePipeline</c> adds a retry when a failed run should be retried.
    /// </summary>
    public static HostLoomBuilder UseInbox<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TStore
    >(this HostLoomBuilder builder, TimeSpan window)
        where TStore : class, IInboxStore
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.TryAddSingleton<IInboxStore, TStore>();
        return builder.UseInbox(
            static provider => provider.GetRequiredService<IInboxStore>(),
            window
        );
    }

    /// <summary>
    /// Runs each event's handlers at most once per (topic, subscription, message id) inside
    /// <paramref name="window"/>, recording deliveries in the store <paramref name="store"/>
    /// resolves, for example <see cref="InboxStore.FromClaim"/> over a cache.
    /// </summary>
    public static HostLoomBuilder UseInbox(
        this HostLoomBuilder builder,
        Func<IServiceProvider, IInboxStore> store,
        TimeSpan window
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(store);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(window, TimeSpan.Zero);
        return builder.ConfigureReceivePipeline(
            (pipe, provider) =>
                pipe.UseInbox(store(provider), window, provider.GetService<ILogger<InboxFilter>>())
        );
    }

    /// <summary>
    /// Deduplicates redeliveries to this process with an <see cref="InMemoryInboxStore"/>, for
    /// tests and single-process deployments.
    /// </summary>
    public static HostLoomBuilder UseInMemoryInbox(this HostLoomBuilder builder, TimeSpan window)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.TryAddSingleton(TimeProvider.System);
        builder.Services.TryAddSingleton<InMemoryInboxStore>(
            static provider => new InMemoryInboxStore(provider.GetRequiredService<TimeProvider>())
        );
        builder.Services.TryAddSingleton<IInboxStore>(static provider =>
            provider.GetRequiredService<InMemoryInboxStore>()
        );
        return builder.UseInbox(
            static provider => provider.GetRequiredService<IInboxStore>(),
            window
        );
    }
}

/// <summary>Adds the inbox filter to a receive pipe composed by hand.</summary>
public static class InboxPipeBuilderExtensions
{
    /// <summary>Appends an <see cref="InboxFilter"/> over <paramref name="store"/> with <paramref name="window"/>.</summary>
    public static PipeBuilder<ReceiveContext> UseInbox(
        this PipeBuilder<ReceiveContext> builder,
        IInboxStore store,
        TimeSpan window,
        ILogger<InboxFilter>? logger = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.Use(new InboxFilter(store, window, logger));
    }
}
