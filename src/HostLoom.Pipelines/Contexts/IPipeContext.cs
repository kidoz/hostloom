namespace HostLoom.Pipelines;

/// <summary>Carries cancellation and strongly typed, lazily created payloads through a pipeline.</summary>
public interface IPipeContext
{
    CancellationToken CancellationToken { get; }
    bool HasPayload(Type payloadType);
    bool TryGetPayload<TPayload>(out TPayload? payload)
        where TPayload : class;
    TPayload GetOrAddPayload<TPayload>(Func<TPayload> payloadFactory)
        where TPayload : class;

    /// <summary>Adds a payload or updates the existing value.</summary>
    /// <remarks>
    /// For <see cref="PipeContext"/>, a payload implemented by the context itself is updated
    /// in place: the update factory must return that same context instance, not a replacement.
    /// </remarks>
    TPayload AddOrUpdatePayload<TPayload>(
        Func<TPayload> addFactory,
        Func<TPayload, TPayload> updateFactory
    )
        where TPayload : class;
}
