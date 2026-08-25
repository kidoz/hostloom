namespace HostLoom.Pipelines.DependencyInjection;

/// <summary>
/// Declares one pipeline: named stages in execution order, each holding filters in registration
/// order, plus optional wrappers around the whole run. Instances are scoped to a single
/// <c>AddPipeline</c> call, so registration state can never leak between hosts or tests.
/// </summary>
public sealed class PipelineBuilder<TContext>
    where TContext : class, IPipeContext
{
    private readonly List<PipelineStageDefinition> _stages = [];
    private readonly List<Action<PipeBuilder<TContext>>> _outerFilters = [];
    private bool _instrumented = true;

    internal PipelineBuilder(string name) => Name = name;

    internal string Name { get; }

    /// <summary>Appends a named stage. Stages execute in declaration order; names must be unique.</summary>
    public PipelineBuilder<TContext> Stage(
        string name,
        Action<PipelineStageBuilder<TContext>> configure
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);
        if (_stages.Any(stage => string.Equals(stage.Name, name, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException(
                $"Pipeline '{Name}' already has a stage named '{name}'."
            );
        }

        var builder = new PipelineStageBuilder<TContext>();
        configure(builder);
        var filters = builder.TakeFilters();
        if (filters.Count == 0)
        {
            throw new InvalidOperationException(
                $"Stage '{name}' of pipeline '{Name}' declares no filters."
            );
        }

        _stages.Add(new PipelineStageDefinition(name, filters));
        return this;
    }

    /// <summary>
    /// Retries the whole run per <paramref name="policy"/> when it faults. Wrappers added through
    /// this method and <c>WithTimeout</c> nest in declaration order, first outermost.
    /// </summary>
    public PipelineBuilder<TContext> WithRetry(
        RetryPolicy policy,
        Func<Exception, bool>? shouldRetry = null,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(policy);
        _outerFilters.Add(builder => builder.UseRetry(policy, shouldRetry, timeProvider));
        return this;
    }

    /// <summary>Disables the per-filter duration, failure, and tracing instrumentation for this pipeline.</summary>
    public PipelineBuilder<TContext> WithoutInstrumentation()
    {
        _instrumented = false;
        return this;
    }

    internal void AddOuterFilter(Action<PipeBuilder<TContext>> configure) =>
        _outerFilters.Add(configure);

    internal PipelineDefinition<TContext> Build()
    {
        if (_stages.Count == 0)
        {
            throw new InvalidOperationException($"Pipeline '{Name}' declares no stages.");
        }

        var duplicates = _stages
            .SelectMany(stage => stage.Filters)
            .GroupBy(filter => filter.Name, StringComparer.Ordinal)
            .Where(group => group.Count() > 1)
            .Select(group => group.Key)
            .ToList();
        if (duplicates.Count > 0)
        {
            throw new InvalidOperationException(
                $"Pipeline '{Name}' has duplicate filter names: {string.Join(", ", duplicates)}. "
                    + "Give each registration a unique name with WithName."
            );
        }

        return new PipelineDefinition<TContext>(
            Name,
            _stages.ToArray(),
            _outerFilters.ToArray(),
            _instrumented
        );
    }
}

/// <summary>Adds filters to one stage; execution order inside the stage is registration order.</summary>
public sealed class PipelineStageBuilder<TContext>
    where TContext : class, IPipeContext
{
    private readonly List<PipelineFilterDefinition> _filters = [];

    internal PipelineStageBuilder() { }

    /// <summary>
    /// Appends a filter resolved from the container per run. The default diagnostic name is the
    /// filter type's name; use <see cref="PipelineFilterBuilder.WithName"/> for a domain name.
    /// </summary>
    public PipelineStageBuilder<TContext> AddFilter<TFilter>(
        Action<PipelineFilterBuilder>? configure = null
    )
        where TFilter : class, IFilter<TContext>
    {
        var builder = new PipelineFilterBuilder(typeof(TFilter).Name);
        configure?.Invoke(builder);
        _filters.Add(builder.Build(typeof(TFilter)));
        return this;
    }

    internal IReadOnlyList<PipelineFilterDefinition> TakeFilters() => _filters.ToArray();
}

/// <summary>Per-filter registration options.</summary>
public sealed class PipelineFilterBuilder
{
    private string _name;
    private Func<IServiceProvider, bool>? _enabledWhen;

    internal PipelineFilterBuilder(string defaultName) => _name = defaultName;

    /// <summary>Sets the name used in metrics, traces, and topology output.</summary>
    public PipelineFilterBuilder WithName(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        _name = name;
        return this;
    }

    /// <summary>
    /// Evaluated once per run against the run's service scope; when false the filter is left out
    /// of the composed pipe. Backed by options or configuration, this turns a filter on and off
    /// per environment without redeploying.
    /// </summary>
    public PipelineFilterBuilder EnabledWhen(Func<IServiceProvider, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        _enabledWhen = predicate;
        return this;
    }

    internal PipelineFilterDefinition Build(Type filterType) =>
        new(filterType, _name, _enabledWhen);
}
