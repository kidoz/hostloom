namespace HostLoom.Logging;

/// <summary>
/// Writes formatted bytes to a stream. Called only from the pipeline's single writer thread, so it
/// needs no synchronisation of its own.
/// </summary>
public sealed class StreamLogSink(Stream stream, bool leaveOpen = false) : ILogSink
{
    private readonly Stream _stream = stream ?? throw new ArgumentNullException(nameof(stream));

    // CA2000: ownership of the stream transfers to the sink, which disposes it.
#pragma warning disable CA2000
    public static StreamLogSink Console() => new(System.Console.OpenStandardOutput());
#pragma warning restore CA2000

    public void Write(ReadOnlySpan<byte> payload) => _stream.Write(payload);

    public async ValueTask FlushAsync(CancellationToken cancellationToken) =>
        await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);

    public async ValueTask DisposeAsync()
    {
        await _stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
        if (!leaveOpen)
        {
            await _stream.DisposeAsync().ConfigureAwait(false);
        }
    }
}
