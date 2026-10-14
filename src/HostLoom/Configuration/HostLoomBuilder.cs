using HostLoom.Pipelines;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace HostLoom;

public sealed class HostLoomBuilder
{
    internal HostLoomBuilder(IServiceCollection services, HostLoomConfiguration configuration)
    {
        Services = services;
        Configuration = configuration;
    }

    public IServiceCollection Services { get; }

    internal HostLoomConfiguration Configuration { get; }

    public HostLoomBuilder AddHandler<TRequest, TResponse, THandler>(RequestAddress endpoint)
        where TRequest : class, IRequest<TResponse>
        where THandler : class, IRequestHandler<TRequest, TResponse>
    {
        Configuration.AddHandler(
            new HandlerRegistration(
                MessageTypeName.For<TRequest>(),
                typeof(TRequest),
                typeof(TResponse),
                typeof(RequestExecutor<TRequest, TResponse>)
            ),
            endpoint
        );

        Services.AddScoped<IRequestHandler<TRequest, TResponse>, THandler>();
        Services.AddScoped<RequestExecutor<TRequest, TResponse>>();
        Services.TryAddTransient<
            IRequestClient<TRequest, TResponse>,
            RequestClient<TRequest, TResponse>
        >();

        return this;
    }

    /// <summary>
    /// Registers <typeparamref name="THandler"/> for <typeparamref name="TRequest"/> on
    /// <paramref name="endpoint"/>. The handler is scoped, one scope per enumeration, and
    /// <see cref="IStreamRequestClient{TRequest, TItem}"/> becomes resolvable.
    /// </summary>
    /// <remarks>
    /// The stream does not pass through the receive pipeline or request behaviors. Those wrap
    /// one completion per delivery, and a stream completes when the caller finishes reading.
    /// A transport that does not implement <see cref="IStreamRequestBroker"/> fails host startup
    /// when any stream endpoint is registered, and a client call fails before it waits.
    /// </remarks>
    public HostLoomBuilder AddStreamHandler<TRequest, TItem, THandler>(RequestAddress endpoint)
        where TRequest : class, IStreamRequest<TItem>
        where THandler : class, IStreamRequestHandler<TRequest, TItem>
    {
        Configuration.AddStreamHandler(
            new StreamHandlerRegistration(
                MessageTypeName.For<TRequest>(),
                typeof(TRequest),
                typeof(TItem),
                typeof(StreamExecutor<TRequest, TItem>)
            ),
            endpoint
        );

        Services.AddScoped<IStreamRequestHandler<TRequest, TItem>, THandler>();
        Services.AddScoped(provider => new StreamExecutor<TRequest, TItem>(
            provider.GetRequiredService<IStreamRequestHandler<TRequest, TItem>>()
        ));
        Services.TryAddTransient<
            IStreamRequestClient<TRequest, TItem>,
            StreamRequestClient<TRequest, TItem>
        >();
        return this;
    }

    /// <summary>
    /// Subscribes <typeparamref name="THandler"/> to <typeparamref name="TEvent"/> on
    /// <paramref name="topic"/>. Subscriptions are named: two names on one topic each receive every
    /// event, while two handlers under one name share a delivery and a scope.
    /// </summary>
    public HostLoomBuilder AddSubscriber<TEvent, THandler>(
        RequestAddress topic,
        string subscription = "default"
    )
        where TEvent : class, IEvent
        where THandler : class, IEventHandler<TEvent>
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subscription);
        Configuration.AddSubscriber(
            new SubscriberRegistration(
                MessageTypeName.For<TEvent>(),
                typeof(TEvent),
                typeof(EventExecutor<TEvent>)
            ),
            new TopicSubscription(topic, subscription),
            typeof(THandler)
        );

        // Registered as the concrete type: the subscription decides which handlers run, so the
        // container must not be able to hand one subscription another's handlers.
        Services.TryAddScoped<THandler>();
        Services.TryAddScoped<EventExecutor<TEvent>>();
        return this;
    }

    public HostLoomBuilder AddBehavior<TRequest, TResponse, TBehavior>()
        where TRequest : class, IRequest<TResponse>
        where TBehavior : class, IRequestBehavior<TRequest, TResponse>
    {
        Services.AddScoped<IRequestBehavior<TRequest, TResponse>, TBehavior>();
        return this;
    }

    public HostLoomBuilder AddRequestClient<TRequest, TResponse>()
        where TRequest : class, IRequest<TResponse>
    {
        Services.TryAddTransient<
            IRequestClient<TRequest, TResponse>,
            RequestClient<TRequest, TResponse>
        >();
        return this;
    }

    /// <summary>
    /// Makes <see cref="IStreamRequestClient{TRequest, TItem}"/> resolvable for an application
    /// that sends the stream and does not host its handler.
    /// </summary>
    public HostLoomBuilder AddStreamClient<TRequest, TItem>()
        where TRequest : class, IStreamRequest<TItem>
    {
        Services.TryAddTransient<
            IStreamRequestClient<TRequest, TItem>,
            StreamRequestClient<TRequest, TItem>
        >();
        return this;
    }

    /// <summary>
    /// Adds filters that wrap handler execution for every inbound request, on every transport.
    /// Handler faults reach these filters as exceptions, before they are encoded as fault
    /// envelopes, so <c>UseRetry</c> and <c>UseCircuitBreaker</c> apply to them. Call more than
    /// once to append; filters run in registration order.
    /// </summary>
    public HostLoomBuilder ConfigureReceivePipeline(Action<PipeBuilder<ReceiveContext>> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        Configuration.ConfigureReceivePipeline(configure);
        return this;
    }

    /// <summary>
    /// Adds filters that need services from the container, such as a store. The pipeline is
    /// composed once, when the runtime resolves it, so <paramref name="configure"/> receives the
    /// root provider and its filters are shared by every delivery like any other.
    /// </summary>
    public HostLoomBuilder ConfigureReceivePipeline(
        Action<PipeBuilder<ReceiveContext>, IServiceProvider> configure
    )
    {
        ArgumentNullException.ThrowIfNull(configure);
        Configuration.ConfigureReceivePipeline(configure);
        return this;
    }

    /// <summary>
    /// Registers HostLoom's liveness and readiness checks, tagged <c>live</c> and <c>ready</c> so
    /// they can be mapped to separate probe endpoints. Liveness never contacts the broker.
    /// </summary>
    public HostLoomBuilder AddHealthChecks(
        string livenessName = "hostloom-live",
        string readinessName = "hostloom-ready"
    )
    {
        Services
            .AddHealthChecks()
            .AddCheck<HostLoomLivenessCheck>(livenessName, HealthStatus.Unhealthy, ["live"])
            .AddCheck<HostLoomReadinessCheck>(readinessName, HealthStatus.Unhealthy, ["ready"]);
        return this;
    }

    public HostLoomBuilder UseTransport<TBroker>()
        where TBroker : class, IRequestBroker
    {
        if (Services.Any(static descriptor => descriptor.ServiceType == typeof(IRequestBroker)))
        {
            throw new InvalidOperationException(
                "HostLoom already has a request transport. Configure exactly one transport per service provider."
            );
        }

        Services.AddSingleton<IRequestBroker, TBroker>();
        return this;
    }
}
