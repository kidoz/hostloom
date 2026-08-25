using HostLoom.Pipelines.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Pipelines.Testing;

public static class PipelineHarness
{
    /// <summary>
    /// Builds a service provider from <paramref name="configureServices"/>, validates every
    /// registered pipeline exactly as host startup would, and returns a harness around the named
    /// runner. Register fakes for the filters' dependencies in the same callback.
    /// </summary>
    public static async ValueTask<PipelineHarness<TContext>> CreateAsync<TContext>(
        string pipelineName,
        Action<IServiceCollection> configureServices,
        CancellationToken cancellationToken = default
    )
        where TContext : class, IPipeContext
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(pipelineName);
        ArgumentNullException.ThrowIfNull(configureServices);

        var services = new ServiceCollection();
        configureServices(services);
        var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateScopes = true }
        );
        try
        {
            await PipelineValidator
                .ValidateAsync(provider, cancellationToken)
                .ConfigureAwait(false);
            var runner = provider.GetRequiredKeyedService<IPipelineRunner<TContext>>(pipelineName);
            return new PipelineHarness<TContext>(provider, runner);
        }
        catch
        {
            await provider.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}

public sealed class PipelineHarness<TContext> : IAsyncDisposable
    where TContext : class, IPipeContext
{
    private readonly ServiceProvider _provider;

    internal PipelineHarness(ServiceProvider provider, IPipelineRunner<TContext> runner)
    {
        _provider = provider;
        Runner = runner;
    }

    public IPipelineRunner<TContext> Runner { get; }

    public PipelineTopology Topology => Runner.Topology;

    /// <summary>Runs the pipeline and captures instead of throwing, so a test asserts on the result either way.</summary>
    public async ValueTask<PipeSendResult<TContext>> RunAsync(TContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        try
        {
            await Runner.RunAsync(context).ConfigureAwait(false);
            return new PipeSendResult<TContext>(context, null);
        }
        catch (Exception exception)
        {
            return new PipeSendResult<TContext>(context, exception);
        }
    }

    public ValueTask DisposeAsync() => _provider.DisposeAsync();
}
