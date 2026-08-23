namespace HostLoom.Pipelines;

public sealed class PipeBuilder<TContext> where TContext : class, IPipeContext
{
    private readonly List<IFilter<TContext>> _filters = [];
    private bool _built;

    public PipeBuilder<TContext> Use(IFilter<TContext> filter)
    {
        ObjectDisposedException.ThrowIf(_built, this);
        ArgumentNullException.ThrowIfNull(filter);
        _filters.Add(filter);
        return this;
    }

    public PipeBuilder<TContext> Use(Func<TContext, IPipe<TContext>, ValueTask> filter, string name = "delegate")
    {
        ArgumentNullException.ThrowIfNull(filter);
        ValidateName(name);
        return Use(new DelegateFilter<TContext>(name, filter));
    }

    public PipeBuilder<TContext> UseExecute(Func<TContext, ValueTask> action, string name = "execute")
    {
        ArgumentNullException.ThrowIfNull(action);
        ValidateName(name);
        return Use(new ExecuteFilter<TContext>(name, action));
    }

    public PipeBuilder<TContext> UseTerminal(Func<TContext, ValueTask> action, string name = "terminal")
    {
        ArgumentNullException.ThrowIfNull(action);
        ValidateName(name);
        return Use(new TerminalFilter<TContext>(name, action));
    }

    public PipeBuilder<TContext> UseWhen(
        Func<TContext, bool> predicate,
        Action<PipeBuilder<TContext>> configure,
        string name = "conditional")
    {
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(configure);
        ValidateName(name);
        var branch = new PipeBuilder<TContext>();
        configure(branch);
        return Use(new ConditionalFilter<TContext>(name, predicate, branch.TakeFilters()));
    }

    public PipeBuilder<TContext> UseConcurrencyLimit(int limit) => Use(new ConcurrencyLimitFilter<TContext>(limit));

    public IPipe<TContext> Build()
    {
        ObjectDisposedException.ThrowIf(_built, this);
        _built = true;
        return PipeComposer.Compose(_filters, Pipe.Empty<TContext>());
    }

    internal IReadOnlyList<IFilter<TContext>> TakeFilters()
    {
        ObjectDisposedException.ThrowIf(_built, this);
        _built = true;
        return _filters.ToArray();
    }

    private static void ValidateName(string name) => ArgumentException.ThrowIfNullOrWhiteSpace(name);
}
