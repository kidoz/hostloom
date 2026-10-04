using System.Globalization;

namespace HostLoom.Generators;

/// <summary>
/// An <see cref="IStringIdGenerator"/> over BCL <see cref="Guid"/> generation only — no bespoke
/// GUID algorithm. These are compatibility identifiers for formats that already store GUID
/// strings; they are never secrets.
/// </summary>
public sealed class GuidStringIdGenerator : IStringIdGenerator
{
    private readonly string _format;

    private GuidStringIdGenerator(string format)
    {
        _format = format;
    }

    /// <summary>Dashed GUID format (<c>"D"</c>), producing 36 characters.</summary>
    public static GuidStringIdGenerator Dashed { get; } = new("D");

    /// <summary>Compact GUID format (<c>"N"</c>), producing 32 characters.</summary>
    public static GuidStringIdGenerator Compact { get; } = new("N");

    /// <inheritdoc />
    public string CreateStringId()
    {
        return Guid.NewGuid().ToString(_format, CultureInfo.InvariantCulture);
    }
}
