using System.Globalization;
using HostLoom.Pipelines;
using Microsoft.Extensions.Logging;

namespace HostLoom.Examples.Pipelines;

/// <summary>
/// A derived-value filter: pure per-item computation over metrics the analyze stage produced. A
/// missing metric is "no insight" (null), never an error — stage ordering guarantees the word
/// count filter already ran, but a document it skipped must not fail summarization.
/// </summary>
internal sealed class ReadingTimeFilter : IFilter<IndexingContext>
{
    public ValueTask SendAsync(IndexingContext context, IPipe<IndexingContext> next)
    {
        foreach (var document in context.Documents)
        {
            document.Insights["reading_minutes"] = document.Metrics.TryGetValue(
                "word_count",
                out var words
            )
                ? Math.Round(words / 200m, 2)
                : null;
        }

        return next.SendAsync(context);
    }
}

/// <summary>What the terminal filter persists through; the example implementation just logs.</summary>
internal interface IDocumentStore
{
    ValueTask SaveAsync(Document document, CancellationToken cancellationToken);
}

internal sealed class LoggingDocumentStore(ILogger<LoggingDocumentStore> logger) : IDocumentStore
{
    public ValueTask SaveAsync(Document document, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        logger.LogInformation(
            "Stored {Document}: metrics [{Metrics}] insights [{Insights}]",
            document.Name,
            string.Join(
                ", ",
                document
                    .Metrics.OrderBy(pair => pair.Key, StringComparer.Ordinal)
                    .Select(pair => $"{pair.Key}={pair.Value}")
            ),
            string.Join(
                ", ",
                document
                    .Insights.OrderBy(pair => pair.Key, StringComparer.Ordinal)
                    .Select(pair =>
                        $"{pair.Key}={pair.Value?.ToString(CultureInfo.InvariantCulture) ?? "null"}"
                    )
            )
        );
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// The terminal filter at the tail of the pipe: persists every item, isolating per-item
/// persistence failures, then calls next so anything composed after it still runs.
/// </summary>
internal sealed class StoreDocumentFilter(IDocumentStore store, ILogger<StoreDocumentFilter> logger)
    : IFilter<IndexingContext>
{
    public async ValueTask SendAsync(IndexingContext context, IPipe<IndexingContext> next)
    {
        foreach (var document in context.Documents)
        {
            try
            {
                await store.SaveAsync(document, context.CancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception exception)
            {
                logger.LogWarning(
                    exception,
                    "Storing {Document} failed; the rest of the batch continues.",
                    document.Name
                );
            }
        }

        await next.SendAsync(context).ConfigureAwait(false);
    }
}
