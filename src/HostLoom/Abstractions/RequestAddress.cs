namespace HostLoom;

public readonly record struct RequestAddress
{
    public RequestAddress(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        Value = value.Trim();
    }

    public string Value { get; }

    public override string ToString() => Value;

    public static implicit operator RequestAddress(string value) => new(value);

    /// <summary>Named alternate for the implicit conversion, for languages without operator support.</summary>
    public static RequestAddress FromString(string value) => new(value);
}
