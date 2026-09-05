namespace HostLoom.Composition.Testing;

/// <summary>An assertion failed while inspecting passive composition data.</summary>
public sealed class CompositionAssertionException : Exception
{
    /// <summary>Creates an assertion failure.</summary>
    public CompositionAssertionException() { }

    /// <summary>Creates an assertion failure with mismatch details.</summary>
    public CompositionAssertionException(string message)
        : base(message) { }

    /// <summary>Creates an assertion failure preserving its cause.</summary>
    public CompositionAssertionException(string message, Exception innerException)
        : base(message, innerException) { }
}
