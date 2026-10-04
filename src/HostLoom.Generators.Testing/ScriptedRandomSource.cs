namespace HostLoom.Generators.Testing;

/// <summary>
/// An <see cref="IRandomSource"/> that plays back a script: every <see cref="NextInt32"/> and
/// <see cref="NextInt64"/> call dequeues the next scripted value, and every <see cref="Fill"/>
/// call consumes the next scripted bytes supplied through <see cref="WithBytes"/>.
/// <para>
/// This is how a test forces endpoints, specific characters, and collisions: script the exact
/// values the generator under test must draw, then assert the exact output. Consumption is
/// strict — dequeuing past the end of the value script, drawing a value outside the requested
/// range, or filling past the end of the byte script fails loudly, so a test that under-scripts
/// cannot pass by accident.
/// </para>
/// </summary>
public sealed class ScriptedRandomSource : IRandomSource
{
    private readonly Queue<long> _values;
    private readonly Queue<byte> _bytes = new();

    /// <summary>
    /// Creates a source whose value script is <paramref name="values"/>, consumed one value per
    /// <see cref="NextInt32"/> or <see cref="NextInt64"/> call in the order supplied.
    /// </summary>
    public ScriptedRandomSource(params long[] values)
    {
        _values = new Queue<long>(values);
    }

    /// <summary>
    /// Appends <paramref name="bytes"/> to the byte script that <see cref="Fill"/> consumes and
    /// returns this source, so the script can be built fluently.
    /// </summary>
    public ScriptedRandomSource WithBytes(params byte[] bytes)
    {
        foreach (var value in bytes)
        {
            _bytes.Enqueue(value);
        }

        return this;
    }

    /// <summary>
    /// Copies the next <c>destination.Length</c> scripted bytes into
    /// <paramref name="destination"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Fewer scripted bytes remain than <paramref name="destination"/> requires.
    /// </exception>
    public void Fill(Span<byte> destination)
    {
        if (_bytes.Count < destination.Length)
        {
            throw new InvalidOperationException(
                $"The byte script holds {_bytes.Count} bytes but {destination.Length} were requested; script more bytes with WithBytes."
            );
        }

        for (var index = 0; index < destination.Length; index++)
        {
            destination[index] = _bytes.Dequeue();
        }
    }

    /// <summary>
    /// Dequeues the next scripted value and returns it as an <see cref="int"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">The value script is exhausted.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The scripted value lies outside [<paramref name="minInclusive"/>,
    /// <paramref name="maxExclusive"/>); script an in-range value instead.
    /// </exception>
    public int NextInt32(int minInclusive, int maxExclusive)
    {
        var value = Dequeue(minInclusive, maxExclusive);
        return (int)value;
    }

    /// <summary>
    /// Dequeues the next scripted value and returns it. Equal bounds return
    /// <paramref name="minInclusive"/> without consuming a scripted value, matching
    /// <see cref="SecureRandomSource"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">The value script is exhausted.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The scripted value lies outside [<paramref name="minInclusive"/>,
    /// <paramref name="maxExclusive"/>); script an in-range value instead.
    /// </exception>
    public long NextInt64(long minInclusive, long maxExclusive)
    {
        // Match SecureRandomSource: an empty range returns the bound and draws nothing.
        if (minInclusive == maxExclusive)
        {
            return minInclusive;
        }

        return Dequeue(minInclusive, maxExclusive);
    }

    private long Dequeue(long minInclusive, long maxExclusive)
    {
        if (_values.Count == 0)
        {
            throw new InvalidOperationException(
                "The value script is exhausted; script more values for the calls the generator under test makes."
            );
        }

        var value = _values.Dequeue();
        if (value < minInclusive || value >= maxExclusive)
        {
            throw new ArgumentOutOfRangeException(
                nameof(minInclusive),
                value,
                $"The scripted value {value} lies outside [{minInclusive}, {maxExclusive}); the script must supply in-range values."
            );
        }

        return value;
    }
}
