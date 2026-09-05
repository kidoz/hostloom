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
