using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HostLoom;

/// <summary>Turns on the transactional outbox for every publish.</summary>
public static class OutboxHostLoomBuilderExtensions
{
    /// <summary>
    /// Routes every <see cref="IPublishEndpoint.PublishAsync{TEvent}"/> through
    /// <typeparamref name="TStore"/> and starts the relay with the host. Register the store
    /// scoped (the default) when it writes through the same unit of work as the handlers, which
    /// is what makes the outbox transactional; a singleton store is fine when it needs no scope.
    /// </summary>
    /// <param name="builder">The HostLoom builder.</param>
    /// <param name="configure">Configures <see cref="OutboxOptions"/>; validated when the host starts.</param>
    /// <param name="lifetime">The store's lifetime.</param>
    public static HostLoomBuilder UseOutbox<TStore>(
        this HostLoomBuilder builder,
        Action<OutboxOptions>? configure = null,
        ServiceLifetime lifetime = ServiceLifetime.Scoped
    )
        where TStore : class, IOutboxStore
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.TryAdd(
            new ServiceDescriptor(typeof(IOutboxStore), typeof(TStore), lifetime)
        );
        return builder.UseOutboxCore(configure);
    }

    /// <summary>
    /// Routes every publish through a per-process <see cref="InMemoryOutboxStore"/>, for tests and
    /// single-process deployments. It joins no transaction.
    /// </summary>
    public static HostLoomBuilder UseInMemoryOutbox(
        this HostLoomBuilder builder,
        Action<OutboxOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.TryAddSingleton(TimeProvider.System);
        builder.Services.TryAddSingleton<InMemoryOutboxStore>(
            static provider => new InMemoryOutboxStore(provider.GetRequiredService<TimeProvider>())
        );
        builder.Services.TryAddSingleton<IOutboxStore>(static provider =>
            provider.GetRequiredService<InMemoryOutboxStore>()
        );
        return builder.UseOutboxCore(configure);
    }

    private static HostLoomBuilder UseOutboxCore(
        this HostLoomBuilder builder,
        Action<OutboxOptions>? configure
    )
    {
        var services = builder.Services;
        var options = services.AddOptions<OutboxOptions>();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        options.ValidateOnStart();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<OutboxOptions>, OutboxOptionsValidator>()
        );
        services.TryAddSingleton(TimeProvider.System);
        services.Replace(ServiceDescriptor.Transient<IPublishEndpoint, OutboxPublishEndpoint>());
        services.TryAddSingleton(static provider =>
        {
            var broker = provider.GetRequiredService<IRequestBroker>();
            if (broker is not IEventBroker events)
            {
                throw new NotSupportedException(
                    $"The configured transport '{broker.GetType().Name}' supports request/response only; "
                        + $"the outbox relay needs a transport that implements {nameof(IEventBroker)}."
                );
            }

            return new OutboxRelay(
                new ScopedOutboxStore(provider.GetRequiredService<IServiceScopeFactory>()),
                events,
                provider.GetRequiredService<IOptions<OutboxOptions>>().Value,
                provider.GetRequiredService<TimeProvider>(),
                provider.GetService<ILogger<OutboxRelay>>()
            );
        });
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, OutboxRelayHostedService>()
        );
        return builder;
    }
}

/// <summary>Runs <see cref="OutboxOptions.Validate"/> when the host starts.</summary>
internal sealed class OutboxOptionsValidator : IValidateOptions<OutboxOptions>
{
    public ValidateOptionsResult Validate(string? name, OutboxOptions options)
    {
        var problems = options.Validate();
        return problems.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(problems);
    }
}

/// <summary>Starts the relay with the host and stops it with the host.</summary>
internal sealed class OutboxRelayHostedService(OutboxRelay relay) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) =>
        relay.StartAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken) =>
        relay.StopAsync(cancellationToken);
}

/// <summary>
/// The store as the relay sees it: every call resolves the registered store from a fresh scope,
/// so a scoped, database-backed store works from the singleton relay the same way it works from a
/// handler's scope.
/// </summary>
internal sealed class ScopedOutboxStore(IServiceScopeFactory scopeFactory) : IOutboxStore
{
    public async ValueTask AppendAsync(
        OutboxMessage message,
        CancellationToken cancellationToken = default
    )
    {
        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            await Resolve(scope).AppendAsync(message, cancellationToken).ConfigureAwait(false);
        }
    }

    public async ValueTask<IReadOnlyList<OutboxMessage>> ClaimAsync(
        int batchSize,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            return await Resolve(scope)
                .ClaimAsync(batchSize, lease, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public async ValueTask MarkPublishedAsync(
        Guid messageId,
        CancellationToken cancellationToken = default
    )
    {
        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            await Resolve(scope)
                .MarkPublishedAsync(messageId, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public async ValueTask MarkFailedAsync(
        Guid messageId,
        string error,
        CancellationToken cancellationToken = default
    )
    {
        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            await Resolve(scope)
                .MarkFailedAsync(messageId, error, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private static IOutboxStore Resolve(AsyncServiceScope scope) =>
        scope.ServiceProvider.GetRequiredService<IOutboxStore>();
}
