namespace HostLoom.Pipelines;

/// <summary>Default pipeline context with a thread-safe payload collection.</summary>
public class PipeContext(CancellationToken cancellationToken = default) : IPipeContext
{
    private readonly Lock _payloadLock = new();
    private Dictionary<Type, object>? _payloads;
    private CancellationToken _cancellationToken = cancellationToken;

    public CancellationToken CancellationToken => _cancellationToken;

    /// <summary>
    /// Replaces the token seen by downstream filters until the returned scope is disposed.
    /// A context must not be shared by concurrent sends, so the swap needs no synchronization.
    /// </summary>
    internal CancellationTokenSwap SwapCancellationToken(CancellationToken replacement)
    {
        var previous = _cancellationToken;
        _cancellationToken = replacement;
        return new CancellationTokenSwap(this, previous);
    }

    internal readonly struct CancellationTokenSwap(PipeContext context, CancellationToken previous)
        : IDisposable
    {
        public void Dispose() => context._cancellationToken = previous;
    }

    public bool HasPayload(Type payloadType)
    {
        ArgumentNullException.ThrowIfNull(payloadType);
        if (payloadType.IsInstanceOfType(this))
            return true;
        lock (_payloadLock)
            return FindPayload(payloadType) is not null;
    }

    public bool TryGetPayload<TPayload>(out TPayload? payload)
        where TPayload : class
    {
        if (this is TPayload contextPayload)
        {
            payload = contextPayload;
            return true;
        }

        lock (_payloadLock)
        {
            payload = FindPayload(typeof(TPayload)) as TPayload;
            return payload is not null;
        }
    }

    public TPayload GetOrAddPayload<TPayload>(Func<TPayload> payloadFactory)
        where TPayload : class
    {
        ArgumentNullException.ThrowIfNull(payloadFactory);
        if (this is TPayload contextPayload)
            return contextPayload;

        lock (_payloadLock)
        {
            if (FindPayload(typeof(TPayload)) is TPayload existing)
                return existing;
            var payload =
                payloadFactory()
                ?? throw new InvalidOperationException("A payload factory cannot return null.");
            (_payloads ??= [])[typeof(TPayload)] = payload;
            return payload;
        }
    }

    public TPayload AddOrUpdatePayload<TPayload>(
        Func<TPayload> addFactory,
        Func<TPayload, TPayload> updateFactory
    )
        where TPayload : class
    {
        ArgumentNullException.ThrowIfNull(addFactory);
        ArgumentNullException.ThrowIfNull(updateFactory);

        lock (_payloadLock)
        {
            var payload = FindPayload(typeof(TPayload)) is TPayload existing
                ? updateFactory(existing)
                : addFactory();
            if (payload is null)
            {
                throw new InvalidOperationException("A payload factory cannot return null.");
            }

            (_payloads ??= [])[typeof(TPayload)] = payload;
            return payload;
        }
    }

    private object? FindPayload(Type payloadType)
    {
        if (_payloads is null)
            return null;
        if (_payloads.TryGetValue(payloadType, out var exactPayload))
            return exactPayload;
        return _payloads.Values.FirstOrDefault(payloadType.IsInstanceOfType);
    }
}
