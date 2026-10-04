namespace HostLoom.Generators.Testing;

/// <summary>
/// A thread-safe, process-local numeric sequence for tests: the first <see cref="NextAsync"/>
/// call returns <c>seed + increment</c>, and every later call returns the previous value plus
/// <c>increment</c>. Values are handed out with <see cref="Interlocked"/>, so concurrent calls
/// never receive the same value.
/// <para>
/// There is no distributed uniqueness: two processes (or two instances) with the same seed
/// produce the same sequence. The seed, the increment, and the concurrency behavior are part of
/// the contract, so a test can pin exact expected values.
/// </para>
/// </summary>
public sealed class AsyncNumericSequence
{
    private readonly long _increment;
    private long _current;

    /// <summary>
    /// Creates a sequence whose first value is <paramref name="seed"/> + <paramref name="increment"/>.
    /// </summary>
    public AsyncNumericSequence(long seed = 0, long increment = 1)
    {
        _increment = increment;
        _current = seed;
    }

    /// <summary>
    /// Returns the next value in the sequence: <c>seed + increment</c> on the first call, then
    /// successive multiples of <c>increment</c> above it. Cancellation is honored before a value
    /// is handed out, so a canceled call neither advances the sequence nor returns a value.
    /// </summary>
    /// <exception cref="OperationCanceledException">
    /// <paramref name="cancellationToken"/> is canceled; the caller's token is preserved.
    /// </exception>
    public ValueTask<long> NextAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(Interlocked.Add(ref _current, _increment));
    }
}
