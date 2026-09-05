// Fluent instance methods are declaration syntax; they intentionally have no runtime state.
#pragma warning disable CA1822
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Composition;

/// <summary>Compile-time selectors and registration policies; every member throws if executed.</summary>
public sealed class CompositionTypeRuleBuilder
{
    private CompositionTypeRuleBuilder() { }

    /// <summary>Matches a closed type, including inherited interfaces and base classes.</summary>
    public CompositionTypeRuleBuilder AssignableTo<T>() =>
        throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Matches a closed type or an open generic interface/base definition.</summary>
    public CompositionTypeRuleBuilder AssignableTo(Type type) =>
        throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Matches any of the explicit type expressions in this selector.</summary>
    public CompositionTypeRuleBuilder AssignableToAny(params Type[] types) =>
        throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Requires each eligible match to be in this namespace or a child namespace.</summary>
    public CompositionTypeRuleBuilder RequireNamespace(string name) =>
        throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Requires a marker, honoring AttributeUsage.Inherited through base classes.</summary>
    public CompositionTypeRuleBuilder WithAttribute<TAttribute>()
        where TAttribute : Attribute => throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Excludes a marker, honoring AttributeUsage.Inherited through base classes.</summary>
    public CompositionTypeRuleBuilder WithoutAttribute<TAttribute>()
        where TAttribute : Attribute => throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Explicitly projects every implemented interface, including incidental interfaces.</summary>
    public CompositionTypeRuleBuilder AsAllImplementedInterfaces() =>
        throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Registers self and forwards matched interfaces to it with the declared lifetime.</summary>
    public CompositionTypeRuleBuilder AsSelfWithInterfaces() =>
        throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Requires exactly this many distinct eligible implementations before projection.</summary>
    public CompositionTypeRuleBuilder ExpectExactly(int count) =>
        throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Requires at least this many distinct eligible implementations before projection.</summary>
    public CompositionTypeRuleBuilder ExpectAtLeast(int count) =>
        throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Appends subject to cardinality, duplicate and lifetime validation.</summary>
    public CompositionTypeRuleBuilder Append() => throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Keeps existing registrations of each projected service.</summary>
    public CompositionTypeRuleBuilder Skip() => throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Rejects existing registrations of each projected service.</summary>
    public CompositionTypeRuleBuilder Throw() => throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Replaces existing descriptors selected by a constant replacement predicate.</summary>
    public CompositionTypeRuleBuilder Replace(CompositionReplacementBehavior behavior) =>
        throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Registers each selected class as itself.</summary>
    public CompositionTypeRuleBuilder AsSelf() => throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Projects only implemented interfaces satisfying an assignability selector.</summary>
    public CompositionTypeRuleBuilder AsImplementedInterfaces() =>
        throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Registers selected classes as the explicit service type.</summary>
    public CompositionTypeRuleBuilder As<TService>() =>
        throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Projects explicit services; an open interface projects its implemented closed forms.</summary>
    public CompositionTypeRuleBuilder As(params Type[] serviceTypes) =>
        throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Specifies transient lifetime.</summary>
    public CompositionTypeRuleBuilder WithTransientLifetime() =>
        throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Specifies scoped lifetime.</summary>
    public CompositionTypeRuleBuilder WithScopedLifetime() =>
        throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Specifies singleton lifetime.</summary>
    public CompositionTypeRuleBuilder WithSingletonLifetime() =>
        throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Specifies a compile-time ServiceLifetime constant.</summary>
    public CompositionTypeRuleBuilder WithLifetime(ServiceLifetime lifetime) =>
        throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Requires one implementation per projected service type.</summary>
    public CompositionTypeRuleBuilder ExpectOne() => throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Allows distinct implementations of one lifetime per projected service type.</summary>
    public CompositionTypeRuleBuilder ExpectMany() =>
        throw CompositionRuleBuilder.DeclarationOnly();

    /// <summary>Explicitly permits an empty candidate set.</summary>
    public CompositionTypeRuleBuilder AllowEmpty() =>
        throw CompositionRuleBuilder.DeclarationOnly();
}

#pragma warning restore CA1822
