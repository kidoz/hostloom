using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace HostLoom.Actors.DependencyInjection;

internal interface IActorRegistration
{
    ValueTask StartAsync(ActorSystem system);
}

// This sender may outlive the host. Keep the service provider and construction scope off it.
internal sealed class HostedActorRef<TMessage, TReply> : IActorRef<TMessage, TReply>
{
    private ActorHandle<TMessage, TReply>? _handle;
    public ActorHandle<TMessage, TReply> Handle =>
        Volatile.Read(ref _handle)
        ?? throw new InvalidOperationException("The actor has not started.");

    public void Bind(ActorHandle<TMessage, TReply> handle) => Volatile.Write(ref _handle, handle);

    public ActorAdmission TryTell(TMessage message) => Handle.Ref.TryTell(message);

    public ValueTask<TReply> AskAsync(
        TMessage message,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    ) => Handle.Ref.AskAsync(message, timeout, cancellationToken);
}

internal sealed class ActorRegistration<TMessage, TReply>(IServiceScopeFactory scopes, object key)
    : IActorRegistration
{
    internal HostedActorRef<TMessage, TReply> Reference { get; } = new();

    public async ValueTask StartAsync(ActorSystem system)
    {
        var handle = await system.SpawnAsync<TMessage, TReply>(CreateAsync).ConfigureAwait(false);
        Reference.Bind(handle);
    }

    private async ValueTask<IActor<TMessage, TReply>> CreateAsync()
    {
        var scope = scopes.CreateAsyncScope();
        try
        {
            var actor = scope.ServiceProvider.GetRequiredKeyedService<IActor<TMessage, TReply>>(
                key
            );
            return new ScopedActor(actor, scope);
        }
        catch (Exception construction)
        {
            try
            {
                await scope.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception cleanup)
            {
                throw new AggregateException(construction, cleanup);
            }
            throw;
        }
    }

    private sealed class ScopedActor(IActor<TMessage, TReply> actor, AsyncServiceScope scope)
        : IActor<TMessage, TReply>,
            IAsyncDisposable
    {
        public ValueTask<TReply> ReceiveAsync(TMessage message, CancellationToken stopping) =>
            actor.ReceiveAsync(message, stopping);

        public ValueTask DisposeAsync() => scope.DisposeAsync();
    }
}

internal sealed class ActorHostedService(
    ActorSystem system,
    IEnumerable<IActorRegistration> registrations,
    ILogger<ActorHostedService>? logger = null
) : IHostedService
{
    private int _started;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
            throw new InvalidOperationException("Actor host startup has already been requested.");
        try
        {
            foreach (var registration in registrations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await registration.StartAsync(system).ConfigureAwait(false);
            }
        }
        catch (Exception startup)
        {
            try
            {
                await system.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception cleanup)
            {
                throw new AggregateException(startup, cleanup);
            }
            throw;
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await system.StopAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            logger?.LogWarning(
                "Actor shutdown wait ended with {ActorCount} actors still owned.",
                system.Snapshot.Actors
            );
            throw;
        }
    }
}
