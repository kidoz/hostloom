using System.Collections.Concurrent;

namespace HostLoom.Pipelines.Testing;

/// <summary>A thread-safe ordered record of which named test filters ran.</summary>
public sealed class ExecutionLog
{
    private readonly ConcurrentQueue<string> _entries = new();

    public IReadOnlyList<string> Entries => _entries.ToArray();

    public void Record(string entry)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(entry);
        _entries.Enqueue(entry);
    }
}

/// <summary>A pass-through filter that records its name in an <see cref="ExecutionLog"/>, for order assertions.</summary>
public sealed class RecordingFilter<TContext> : IFilter<TContext>
    where TContext : class, IPipeContext
{
    private readonly string _name;
    private readonly ExecutionLog _log;

    public RecordingFilter(string name, ExecutionLog log)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(log);
        _name = name;
        _log = log;
    }

    public ValueTask SendAsync(TContext context, IPipe<TContext> next)
    {
        ArgumentNullException.ThrowIfNull(next);
        _log.Record(_name);
        return next.SendAsync(context);
    }

    public void Probe(IProbeContext context) => context.CreateScope(_name);
}
