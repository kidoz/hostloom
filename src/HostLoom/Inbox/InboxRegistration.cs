using System.Diagnostics.CodeAnalysis;
using HostLoom.Pipelines;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace HostLoom;

/// <summary>
/// Adds the inbox filter to the receive pipeline. An application has one inbox: each of these
/// methods may be called once, and a second call, of any of them, throws.
/// </summary>
public static class InboxHostLoomBuilderExtensions
{
    /// <summary>
    /// Skips redeliveries of an event whose handlers completed for the same (topic, subscription,
    /// message id) inside <paramref name="window"/>, recording deliveries in
    /// <typeparamref name="TStore"/>; a run that fails releases its key. The filter is appended
    /// to the receive pipeline at this point in registration order, so call it before
    /// <c>ConfigureReceivePipeline</c> adds a retry to keep in-process retries inside one
    /// recorded run.
    /// </summary>
    /// <exception cref="InvalidOperationException">The inbox is already enabled.</exception>
    public static HostLoomBuilder UseInbox<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TStore
    >(this HostLoomBuilder builder, TimeSpan window)
        where TStore : class, IInboxStore
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(window, TimeSpan.Zero);
        builder.Configuration.EnableInbox();
        builder.Services.TryAddSingleton<IInboxStore, TStore>();
        return builder.AddInboxFilter(
            static provider => provider.GetRequiredService<IInboxStore>(),
            window
        );
    }

    /// <summary>
    /// Skips redeliveries of an event whose handlers completed for the same (topic, subscription,
    /// message id) inside <paramref name="window"/>, recording deliveries in the store
    /// <paramref name="store"/> resolves, for example
    /// <see cref="InboxStore.FromClaim(Func{string, TimeSpan, CancellationToken, ValueTask{bool}}, Func{string, CancellationToken, ValueTask})"/>
    /// over a cache.
    /// </summary>
    /// <exception cref="InvalidOperationException">The inbox is already enabled.</exception>
    public static HostLoomBuilder UseInbox(
        this HostLoomBuilder builder,
        Func<IServiceProvider, IInboxStore> store,
        TimeSpan window
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(store);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(window, TimeSpan.Zero);
        builder.Configuration.EnableInbox();
        return builder.AddInboxFilter(store, window);
    }

    /// <summary>
    /// Deduplicates redeliveries to this process with an <see cref="InMemoryInboxStore"/>, for
    /// tests and single-process deployments.
    /// </summary>
    /// <exception cref="InvalidOperationException">The inbox is already enabled.</exception>
    public static HostLoomBuilder UseInMemoryInbox(this HostLoomBuilder builder, TimeSpan window)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(window, TimeSpan.Zero);
        builder.Configuration.EnableInbox();
        builder.Services.TryAddSingleton(TimeProvider.System);
        builder.Services.TryAddSingleton<InMemoryInboxStore>(
            static provider => new InMemoryInboxStore(provider.GetRequiredService<TimeProvider>())
        );
        builder.Services.TryAddSingleton<IInboxStore>(static provider =>
            provider.GetRequiredService<InMemoryInboxStore>()
        );
        return builder.AddInboxFilter(
            static provider => provider.GetRequiredService<IInboxStore>(),
            window
        );
    }

    private static HostLoomBuilder AddInboxFilter(
        this HostLoomBuilder builder,
        Func<IServiceProvider, IInboxStore> store,
        TimeSpan window
    ) =>
        builder.ConfigureReceivePipeline(
            (pipe, provider) =>
                pipe.UseInbox(store(provider), window, provider.GetService<ILogger<InboxFilter>>())
        );
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
