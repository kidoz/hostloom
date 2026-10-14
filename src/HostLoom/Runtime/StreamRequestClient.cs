using System.Runtime.CompilerServices;
using Microsoft.Extensions.Options;

namespace HostLoom;

internal sealed class StreamRequestClient<TRequest, TItem>(
    IRequestBroker broker,
    IOptions<HostLoomOptions> options
) : IStreamRequestClient<TRequest, TItem>
    where TRequest : class, IStreamRequest<TItem>
{
    public IAsyncEnumerable<TItem> StreamAsync(
        RequestAddress destination,
        TRequest request,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(request);
        var effectiveTimeout = timeout ?? options.Value.RequestTimeout;
        if (effectiveTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeout),
                "The request timeout must be positive."
            );
        }

        if (broker is not IStreamRequestBroker streams)
        {
            throw new NotSupportedException(
                $"The configured transport '{broker.GetType().Name}' does not stream. "
                    + "IStreamRequest is delivered only by an in-process transport."
            );
        }

        return Read(streams, effectiveTimeout, cancellationToken);

        // The compiler links this argument with the token passed to GetAsyncEnumerator, so a
        // caller can pass the token to StreamAsync or through WithCancellation.
        async IAsyncEnumerable<TItem> Read(
            IStreamRequestBroker transport,
            TimeSpan budget,
            [EnumeratorCancellation] CancellationToken enumeratorToken = default
        )
        {
            using var activity = HostLoomDiagnostics.ActivitySource.StartActivity(
                "hostloom stream"
            );
            activity?.SetTag("messaging.operation.type", "send");
            activity?.SetTag("messaging.destination.name", destination.Value);
            activity?.SetTag("messaging.message.type", MessageTypeName.For<TRequest>());

            await foreach (
                var item in transport
                    .StreamAsync(destination, request, budget, enumeratorToken)
                    .ConfigureAwait(false)
                    .WithCancellation(enumeratorToken)
            )
            {
                yield return item is null ? default! : (TItem)item;
            }
        }
    }
}
