using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Composition;

/// <summary>How many implementations a projected service permits.</summary>
public enum CompositionCardinality
{
    /// <summary>Exactly one unkeyed descriptor must remain after application.</summary>
    One,

    /// <summary>Distinct implementations may be registered enumerably.</summary>
    Many,
}

/// <summary>How an incoming registration handles the current service collection.</summary>
public enum CompositionRegistrationStrategy
{
    /// <summary>Throw for One; append distinct implementations for Many.</summary>
    Default,

    /// <summary>Append a distinct activation, subject to cardinality and lifetime checks.</summary>
    Append,

    /// <summary>Keep existing registrations of this service and report the incoming entry skipped.</summary>
    Skip,

    /// <summary>Reject any existing unkeyed registration of this service.</summary>
    Throw,

    /// <summary>Remove descriptors selected by the replacement behavior, then append.</summary>
    Replace,
}

/// <summary>Predicates selecting unkeyed descriptors for replacement.</summary>
[Flags]
public enum CompositionReplacementBehavior
{
    /// <summary>Remove descriptors with the incoming service type.</summary>
    ServiceType = 1,

    /// <summary>Remove type-backed descriptors with the incoming implementation type.</summary>
    ImplementationType = 2,

    /// <summary>Remove the union of service-type and implementation-type matches.</summary>
    All = ServiceType | ImplementationType,
}

/// <summary>An explicit DI descriptor and its authored registration policy.</summary>
/// <remarks>
/// Construction is passive. Type compatibility and constructor graphs are validated by generated
/// rules or the final provider, not by runtime reflection here. Factory bodies remain opaque.
/// </remarks>
public sealed class CompositionRegistration
{
    /// <summary>Creates an unkeyed registration with explicit cardinality and provenance.</summary>
    public CompositionRegistration(
        ServiceDescriptor descriptor,
        CompositionCardinality cardinality,
        CompositionOrigin origin,
        CompositionRegistrationStrategy strategy = CompositionRegistrationStrategy.Default,
        CompositionReplacementBehavior replacementBehavior =
            CompositionReplacementBehavior.ServiceType
    )
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        ArgumentNullException.ThrowIfNull(origin);
        if (descriptor.IsKeyedService)
        {
            throw new ArgumentException(
                "Composition declarations must be unkeyed.",
                nameof(descriptor)
            );
        }
        if (!Enum.IsDefined(cardinality))
        {
            throw new ArgumentOutOfRangeException(nameof(cardinality));
        }
        if (!Enum.IsDefined(strategy))
        {
            throw new ArgumentOutOfRangeException(nameof(strategy));
        }
        if (!Enum.IsDefined(replacementBehavior))
        {
            throw new ArgumentOutOfRangeException(nameof(replacementBehavior));
        }
        if (!Enum.IsDefined(descriptor.Lifetime))
        {
            throw new ArgumentException("The descriptor lifetime is invalid.", nameof(descriptor));
        }

        Descriptor = descriptor;
        Cardinality = cardinality;
        Origin = origin;
        Strategy = strategy;
        ReplacementBehavior = replacementBehavior;
    }

    /// <summary>The immutable DI descriptor; inspecting it never invokes its factory.</summary>
    public ServiceDescriptor Descriptor { get; }

    /// <summary>The required service cardinality.</summary>
    public CompositionCardinality Cardinality { get; }

    /// <summary>The authored rule location.</summary>
    public CompositionOrigin Origin { get; }

    /// <summary>The policy used when applying to an existing collection.</summary>
    public CompositionRegistrationStrategy Strategy { get; }

    /// <summary>The predicates used by Replace.</summary>
    public CompositionReplacementBehavior ReplacementBehavior { get; }
}
