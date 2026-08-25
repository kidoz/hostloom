using System.Diagnostics;

namespace HostLoom.Pipelines;

/// <summary>
/// Wraps a filter with a duration histogram, a failure counter, and a tracing span. The recorded
/// duration is the filter's own work: time spent inside the downstream pipe is measured separately
/// and subtracted, so a slow filter is visible even at the head of a slow pipeline. When no meter
/// or trace listener is enabled the wrapper delegates directly and measures nothing.
/// </summary>
public sealed class InstrumentedFilter<TContext> : IFilter<TContext>
    where TContext : class, IPipeContext
{
    private const string SuccessOutcome = "success";
    private const string FailureOutcome = "failure";
    private const string CanceledOutcome = "canceled";

    private readonly IFilter<TContext> _filter;
    private readonly TimeProvider _timeProvider;
    private readonly string _pipelineName;
    private readonly string? _stageName;
    private readonly string _filterName;
    private readonly TagList _tags;

    public InstrumentedFilter(
        IFilter<TContext> filter,
        string pipelineName,
        string? stageName,
        string filterName,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(filter);
        ArgumentException.ThrowIfNullOrWhiteSpace(pipelineName);
        ArgumentException.ThrowIfNullOrWhiteSpace(filterName);
        _filter = filter;
        _pipelineName = pipelineName;
        _stageName = stageName;
        _filterName = filterName;
        _timeProvider = timeProvider ?? TimeProvider.System;

        var tags = new TagList
        {
            { "hostloom.pipeline.name", pipelineName },
            { "hostloom.pipeline.filter", filterName },
        };
        if (stageName is not null)
        {
            tags.Add("hostloom.pipeline.stage", stageName);
        }

        _tags = tags;
    }

    public async ValueTask SendAsync(TContext context, IPipe<TContext> next)
    {
        if (
            !PipelineDiagnostics.FilterDuration.Enabled
            && !PipelineDiagnostics.FilterFailures.Enabled
            && !PipelineDiagnostics.ActivitySource.HasListeners()
        )
        {
            await _filter.SendAsync(context, next).ConfigureAwait(false);
            return;
        }

        using var activity = PipelineDiagnostics.ActivitySource.StartActivity(
            "hostloom pipeline filter"
        );
        activity?.SetTag("hostloom.pipeline.name", _pipelineName);
        activity?.SetTag("hostloom.pipeline.filter", _filterName);
        if (_stageName is not null)
        {
            activity?.SetTag("hostloom.pipeline.stage", _stageName);
        }

        var downstream = new DownstreamTimer(next, _timeProvider);
        var start = _timeProvider.GetTimestamp();
        var outcome = SuccessOutcome;
        try
        {
            await _filter.SendAsync(context, downstream).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            outcome = CanceledOutcome;
            throw;
        }
        catch (Exception exception)
        {
            outcome = FailureOutcome;
            activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
            PipelineDiagnostics.FilterFailures.Add(1, _tags);
            throw;
        }
        finally
        {
            var selfTime = _timeProvider.GetElapsedTime(start) - downstream.Elapsed;
            if (selfTime < TimeSpan.Zero)
            {
                selfTime = TimeSpan.Zero;
            }

            var tags = _tags;
            tags.Add("hostloom.pipeline.outcome", outcome);
            PipelineDiagnostics.FilterDuration.Record(selfTime.TotalSeconds, tags);
        }
    }

    public void Probe(IProbeContext context) => _filter.Probe(context);

    private sealed class DownstreamTimer(IPipe<TContext> next, TimeProvider timeProvider)
        : IPipe<TContext>
    {
        public TimeSpan Elapsed { get; private set; }

        public async ValueTask SendAsync(TContext context)
        {
            var start = timeProvider.GetTimestamp();
            try
            {
                await next.SendAsync(context).ConfigureAwait(false);
            }
            finally
            {
                Elapsed += timeProvider.GetElapsedTime(start);
            }
        }

        public void Probe(IProbeContext context) => next.Probe(context);
    }
}
