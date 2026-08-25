using HostLoom.Pipelines;
using Microsoft.Extensions.Logging;

namespace HostLoom.Examples.Pipelines;

/// <summary>
/// An analysis filter in the shape of a per-item lookup: fan out over the batch with a bounded
/// degree of parallelism, compute one metric per document, and isolate per-item failures so one
/// bad document never fails the batch.
/// </summary>
internal sealed class WordCountFilter(ILogger<WordCountFilter> logger) : IFilter<IndexingContext>
{
    public async ValueTask SendAsync(IndexingContext context, IPipe<IndexingContext> next)
    {
        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = context.MaxAnalysisParallelism,
            CancellationToken = context.CancellationToken,
        };
        await Parallel
            .ForEachAsync(
                context.Documents,
                options,
                async (document, token) =>
                {
                    try
                    {
                        var count = await CountWordsAsync(document.Content, token)
                            .ConfigureAwait(false);
                        document.Metrics["word_count"] = count;
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception exception)
                    {
                        // Per-item isolation: log, skip the document, keep the batch alive.
                        logger.LogWarning(
                            exception,
                            "Counting words failed for {Document}.",
                            document.Name
                        );
                    }
                }
            )
            .ConfigureAwait(false);

        await next.SendAsync(context).ConfigureAwait(false);
    }

    // Stands in for a remote text-analysis call; honours the context token like real IO must.
    private static ValueTask<decimal> CountWordsAsync(
        string content,
        CancellationToken cancellationToken
    )
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(
            (decimal)content.Split(' ', StringSplitOptions.RemoveEmptyEntries).Length
        );
    }
}

/// <summary>
/// A second analysis filter, registered behind a feature toggle: when disabled it is simply
/// absent from the composed pipe for that run, so nothing logs warnings about it.
/// </summary>
internal sealed class SentenceCountFilter : IFilter<IndexingContext>
{
    public async ValueTask SendAsync(IndexingContext context, IPipe<IndexingContext> next)
    {
        foreach (var document in context.Documents)
        {
            document.Metrics["sentence_count"] = document.Content.Count(character =>
                character == '.'
            );
        }

        await next.SendAsync(context).ConfigureAwait(false);
    }
}
