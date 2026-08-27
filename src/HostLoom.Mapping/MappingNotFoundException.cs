namespace HostLoom.Mapping;

/// <summary>Thrown when no mapping is registered for a requested type pair.</summary>
public sealed class MappingNotFoundException : InvalidOperationException
{
    /// <summary>Creates an exception for the missing source and destination pair.</summary>
    public MappingNotFoundException(Type sourceType, Type destinationType)
        : base(CreateMessage(sourceType, destinationType))
    {
        SourceType = sourceType;
        DestinationType = destinationType;
    }

    private static string CreateMessage(Type sourceType, Type destinationType)
    {
        ArgumentNullException.ThrowIfNull(sourceType);
        ArgumentNullException.ThrowIfNull(destinationType);
        return $"No mapping is registered from '{sourceType.FullName}' to '{destinationType.FullName}'. "
            + "Register one with AddHostLoomMapping and MappingBuilder.Add.";
    }

    /// <summary>The requested source type.</summary>
    public Type SourceType { get; }

    /// <summary>The requested destination type.</summary>
    public Type DestinationType { get; }
}
