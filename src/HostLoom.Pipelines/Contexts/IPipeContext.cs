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
    TPayload AddOrUpdatePayload<TPayload>(
        Func<TPayload> addFactory,
        Func<TPayload, TPayload> updateFactory
    )
        where TPayload : class;
}
