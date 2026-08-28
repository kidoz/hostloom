namespace HostLoom.Mapping;

/// <summary>Thrown when no mapping is registered for a requested type pair.</summary>
public sealed class MappingNotFoundException : InvalidOperationException
{
    /// <summary>Creates an exception for the missing source and destination pair.</summary>
    public MappingNotFoundException(Type sourceType, Type destinationType)
        : this(sourceType, destinationType, []) { }

    /// <summary>
    /// Creates an exception that also reports what the source type is registered to map to.
    /// </summary>
    /// <remarks>
    /// The near miss is the useful part of this failure: a destination named one letter differently,
    /// or a pair registered in the other direction, is the usual cause. Listing the registered
    /// destinations turns reading the message into the diagnosis.
    /// </remarks>
    public MappingNotFoundException(
        Type sourceType,
        Type destinationType,
        IReadOnlyList<Type> registeredDestinations
    )
        : base(CreateMessage(sourceType, destinationType, registeredDestinations))
    {
        SourceType = sourceType;
        DestinationType = destinationType;
        RegisteredDestinations = registeredDestinations;
    }

    private static string CreateMessage(
        Type sourceType,
        Type destinationType,
        IReadOnlyList<Type> registeredDestinations
    )
    {
        ArgumentNullException.ThrowIfNull(sourceType);
        ArgumentNullException.ThrowIfNull(destinationType);
        ArgumentNullException.ThrowIfNull(registeredDestinations);

        var message =
            $"No mapping is registered from '{sourceType.FullName}' to '{destinationType.FullName}'. ";
        if (registeredDestinations.Count == 0)
        {
            return message + "Register one with AddHostLoomMapping and MappingBuilder.Add.";
        }

        return message
            + $"'{sourceType.FullName}' is registered to map to "
            + string.Join(", ", registeredDestinations.Select(type => $"'{type.FullName}'"))
            + ". Register the requested pair with AddHostLoomMapping and MappingBuilder.Add.";
    }

    /// <summary>The requested source type.</summary>
    public Type SourceType { get; }

    /// <summary>The requested destination type.</summary>
    public Type DestinationType { get; }

    /// <summary>
    /// The destinations the source type is registered to map to, empty when the registered pairs
    /// are not known at the point of failure.
    /// </summary>
    public IReadOnlyList<Type> RegisteredDestinations { get; }
}
