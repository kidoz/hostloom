// Fluent instance methods are declaration syntax; they intentionally have no runtime state.
#pragma warning disable CA1822
namespace HostLoom.Composition;

/// <summary>Compile-time vocabulary for declaring a composition plan. Never evaluated at runtime.</summary>
public sealed class CompositionRuleBuilder
{
    private CompositionRuleBuilder() { }

    /// <summary>Selects classes declared in this compilation; requires an assignability selector.</summary>
    public CompositionTypeRuleBuilder AddClasses() => throw DeclarationOnly();

    /// <summary>Selects explicit typeof expressions, including accessible referenced types.</summary>
    public CompositionTypeRuleBuilder AddTypes(params Type[] types) => throw DeclarationOnly();

    /// <summary>Names an inline block of rules. Names must be unique compile-time constants.</summary>
    public void Group(string name, Action<CompositionRuleBuilder> configure) =>
        throw DeclarationOnly();

    internal static InvalidOperationException DeclarationOnly() =>
        new(
            "Composition rules are compile-time declarations. Call the generated plan factory instead."
        );
}

#pragma warning restore CA1822
