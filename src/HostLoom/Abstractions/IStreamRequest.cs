namespace HostLoom;

/// <summary>
/// Marks a message as a request whose handler yields a sequence of
/// <typeparamref name="TItem"/> instead of one response.
/// </summary>
/// <typeparam name="TItem">One element of the sequence.</typeparam>
/// <remarks>
/// A stream is an in-process delivery. It is not encoded as a wire envelope and is not
/// carried by a broker transport. The request and each item are the instances the handler
/// produced, passed through in the same process.
/// </remarks>
public interface IStreamRequest<out TItem>;
