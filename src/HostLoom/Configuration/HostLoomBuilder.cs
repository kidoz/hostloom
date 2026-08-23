using HostLoom.Pipelines;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

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
                typeof(RequestExecutor<TRequest, TResponse>)),
            endpoint);

        Services.AddScoped<IRequestHandler<TRequest, TResponse>, THandler>();
        Services.AddScoped<RequestExecutor<TRequest, TResponse>>();
        Services.TryAddTransient<IRequestClient<TRequest, TResponse>, RequestClient<TRequest, TResponse>>();

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
        Services.TryAddTransient<IRequestClient<TRequest, TResponse>, RequestClient<TRequest, TResponse>>();
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

    public HostLoomBuilder UseTransport<TBroker>()
        where TBroker : class, IRequestBroker
    {
        if (Services.Any(static descriptor => descriptor.ServiceType == typeof(IRequestBroker)))
        {
            throw new InvalidOperationException("HostLoom already has a request transport. Configure exactly one transport per service provider.");
        }

        Services.AddSingleton<IRequestBroker, TBroker>();
        return this;
    }
}
