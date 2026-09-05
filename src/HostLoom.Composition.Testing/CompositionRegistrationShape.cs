using System.Collections.ObjectModel;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Composition.Testing;

/// <summary>The activation semantics represented by a normalized registration.</summary>
public enum CompositionActivationKind
{
    /// <summary>The container constructs an implementation type.</summary>
    ImplementationType,

    /// <summary>A factory forwards to an explicitly identified self registration.</summary>
    ForwardingAlias,

    /// <summary>An opaque factory identified by the test's semantic contract.</summary>
    Factory,

    /// <summary>A prebuilt instance identified by the test's semantic contract.</summary>
    Instance,
}

/// <summary>A registration's semantic projection, excluding provenance, policy and object identity.</summary>
public sealed record CompositionRegistrationShape
{
    private CompositionRegistrationShape(
        Type serviceType,
        Type? implementationType,
        ServiceLifetime lifetime,
        CompositionActivationKind activation,
        Type? aliasTargetType,
        string? opaqueIdentity
    )
    {
        ServiceType = serviceType;
        ImplementationType = implementationType;
        Lifetime = lifetime;
        Activation = activation;
        AliasTargetType = aliasTargetType;
        OpaqueIdentity = opaqueIdentity;
    }

    /// <summary>The exact service type, including any constructed generic arguments.</summary>
    public Type ServiceType { get; }

    /// <summary>The known implementation or alias target; null for opaque activations.</summary>
    public Type? ImplementationType { get; }

    /// <summary>The declared lifetime.</summary>
    public ServiceLifetime Lifetime { get; }

    /// <summary>The construction/forwarding kind.</summary>
    public CompositionActivationKind Activation { get; }

    /// <summary>The self type a forwarding factory resolves.</summary>
    public Type? AliasTargetType { get; }

    /// <summary>A caller-defined semantic contract for an opaque factory or instance.</summary>
    public string? OpaqueIdentity { get; }

    /// <summary>Projects one registration without inspecting or executing its activation.</summary>
    public static CompositionRegistrationShape From(
        CompositionRegistration registration,
        string? opaqueIdentity = null
    )
    {
        ArgumentNullException.ThrowIfNull(registration);
        return FromDescriptor(
            registration.Descriptor,
            registration.AliasTargetType,
            opaqueIdentity
        );
    }

    /// <summary>Projects a descriptor, with caller assertions for forwarding and opaque activations.</summary>
    /// <remarks>Keyed descriptors are unsupported. An opaque semantic identity is required; delegate and instance identity are never used.</remarks>
    public static CompositionRegistrationShape FromDescriptor(
        ServiceDescriptor descriptor,
        Type? aliasTargetType = null,
        string? opaqueIdentity = null
    )
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        if (descriptor.IsKeyedService)
            throw new ArgumentException(
                "Keyed descriptors have no unkeyed composition projection.",
                nameof(descriptor)
            );
        if (aliasTargetType is not null && descriptor.ImplementationFactory is null)
            throw new ArgumentException(
                "An alias target requires a forwarding factory.",
                nameof(aliasTargetType)
            );
        var kind =
            aliasTargetType is not null ? CompositionActivationKind.ForwardingAlias
            : descriptor.ImplementationType is not null
                ? CompositionActivationKind.ImplementationType
            : descriptor.ImplementationFactory is not null ? CompositionActivationKind.Factory
            : CompositionActivationKind.Instance;
        if (kind is CompositionActivationKind.Factory or CompositionActivationKind.Instance)
        {
            if (string.IsNullOrWhiteSpace(opaqueIdentity))
                throw new ArgumentException(
                    "Supply an explicit semantic identity for opaque factories and instances; identity cannot be inferred without inspecting application behavior.",
                    nameof(opaqueIdentity)
                );
        }
        else if (opaqueIdentity is not null)
            throw new ArgumentException(
                "Opaque identities apply only to ordinary factory/instance registrations.",
                nameof(opaqueIdentity)
            );
        return new CompositionRegistrationShape(
            descriptor.ServiceType,
            descriptor.ImplementationType ?? aliasTargetType,
            descriptor.Lifetime,
            kind,
            aliasTargetType,
            opaqueIdentity
        );
    }

    /// <summary>Copies an ordered semantic projection from a passive probe.</summary>
    /// <param name="probe">The intended plan entries.</param>
    /// <param name="opaqueIdentity">Optional test-owned identity resolver, called only for opaque entries. It must not execute their factories.</param>
    public static IReadOnlyList<CompositionRegistrationShape> Project(
        CompositionPlanProbe probe,
        Func<CompositionRegistration, string>? opaqueIdentity = null
    )
    {
        ArgumentNullException.ThrowIfNull(probe);
        return new ReadOnlyCollection<CompositionRegistrationShape>(
            probe
                .Registrations.Select(entry =>
                    From(
                        entry,
                        entry.ImplementationType is null ? opaqueIdentity?.Invoke(entry) : null
                    )
                )
                .ToArray()
        );
    }
}
