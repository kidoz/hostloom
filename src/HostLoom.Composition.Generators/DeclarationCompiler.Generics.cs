using Microsoft.CodeAnalysis;

namespace HostLoom.Composition.Generators;

internal sealed partial class DeclarationCompiler
{
    private void SelectOpenGeneric(Rule rule)
    {
        if (rule.Types.Count != 1 || rule.Services.Count != 1)
            return;
        INamedTypeSymbol implementation = rule.Types[0],
            service = rule.Services[0];
        INamedTypeSymbol definition = implementation.OriginalDefinition;
        INamedTypeSymbol contract = service.OriginalDefinition;
        string? reason = null;
        if (
            !implementation.IsUnboundGenericType
            || !service.IsUnboundGenericType
            || definition.Arity != contract.Arity
            || definition.Arity == 0
            || definition.ContainingType is not null
            || contract.ContainingType is not null
        )
            reason =
                "Use top-level open generic definitions with equal arity; nested generic definitions are unsupported.";
        else if (
            definition.TypeKind != TypeKind.Class
            || definition.IsAbstract
            || definition.IsStatic
            || definition
                .GetAttributes()
                .Any(static item =>
                    item.AttributeClass?.ToDisplayString()
                    == "System.Runtime.CompilerServices.CompilerGeneratedAttribute"
                )
            || contract.TypeKind is not (TypeKind.Class or TypeKind.Interface)
        )
            reason =
                "The implementation must be a concrete non-generated class and the service a class or interface.";
        else if (
            !definition.InstanceConstructors.Any(static constructor =>
                constructor.DeclaredAccessibility == Accessibility.Public
            )
        )
            reason = "The open implementation requires a public constructor.";
        else if (
            !_model.Compilation.IsSymbolAccessibleWithin(definition, _method.ContainingType)
            || !_model.Compilation.IsSymbolAccessibleWithin(contract, _method.ContainingType)
            || definition.IsFileLocal
            || contract.IsFileLocal
        )
            reason = "Both open definitions must be accessible from the generated factory.";
        else
        {
            INamedTypeSymbol positional = contract.Construct(
                definition.TypeParameters.Cast<ITypeSymbol>().ToArray()
            );
            if (!Matches(definition, positional))
                reason =
                    "The implementation must implement the service with its own type parameters in the same positions.";
            else if (!ConstraintsAdmitService(contract, definition))
                reason =
                    "Implementation constraints or trimming annotations are stricter than the service, or use an unsupported mapping. Supported constraint types must match after positional substitution.";
        }
        if (reason is not null)
        {
            Error(
                CompositionDiagnostics.Generic,
                rule.Syntax,
                reason,
                definition.Locations.FirstOrDefault()
            );
            return;
        }
        ValidateCounts(rule, 1);
        _registrations.Add(new Registration(implementation, service, rule));
    }

    // Conservative subset: positional generic constraints, ordinary class/struct/new constraints,
    // and exact constraint-type identities. Unsupported implication is an error, never a guess.
    private static bool ConstraintsAdmitService(
        INamedTypeSymbol service,
        INamedTypeSymbol implementation
    )
    {
        for (var i = 0; i < implementation.Arity; i++)
        {
            var required = implementation.TypeParameters[i];
            var admitted = service.TypeParameters[i];
            if (
                admitted.AllowsRefLikeType && !required.AllowsRefLikeType
                || required.HasReferenceTypeConstraint && !admitted.HasReferenceTypeConstraint
                || required.HasValueTypeConstraint && !admitted.HasValueTypeConstraint
                || required.HasUnmanagedTypeConstraint && !admitted.HasUnmanagedTypeConstraint
                || required.HasNotNullConstraint
                    && !(
                        admitted.HasNotNullConstraint
                        || admitted.HasValueTypeConstraint
                        || admitted.HasReferenceTypeConstraint
                            && admitted.ReferenceTypeConstraintNullableAnnotation
                                != NullableAnnotation.Annotated
                    )
                || required.HasConstructorConstraint
                    && !(admitted.HasConstructorConstraint || admitted.HasValueTypeConstraint)
                || (TrimmingMembers(required) & ~TrimmingMembers(admitted)) != 0
            )
                return false;
            // Substitute service constraints into the implementation's parameter space.
            foreach (ITypeSymbol constraint in required.ConstraintTypes)
                if (
                    !admitted.ConstraintTypes.Any(candidate =>
                        SymbolEqualityComparer.Default.Equals(
                            Substitute(
                                candidate,
                                service.TypeParameters,
                                implementation.TypeParameters
                            ),
                            constraint
                        )
                    )
                )
                    return false;
        }
        return true;
    }

    private static ITypeSymbol Substitute(
        ITypeSymbol type,
        System.Collections.Immutable.ImmutableArray<ITypeParameterSymbol> from,
        System.Collections.Immutable.ImmutableArray<ITypeParameterSymbol> to
    )
    {
        for (var i = 0; i < from.Length; i++)
            if (SymbolEqualityComparer.Default.Equals(type, from[i]))
                return to[i];
        if (type is INamedTypeSymbol named && named.IsGenericType && named.ContainingType is null)
            return named.OriginalDefinition.Construct(
                named.TypeArguments.Select(argument => Substitute(argument, from, to)).ToArray()
            );
        return type;
    }

    private static int TrimmingMembers(ITypeParameterSymbol parameter) =>
        parameter
            .GetAttributes()
            .Where(static attribute =>
                attribute.AttributeClass?.ToDisplayString()
                == "System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembersAttribute"
            )
            .Select(static attribute =>
                attribute.ConstructorArguments.Length == 1
                && attribute.ConstructorArguments[0].Value is int value
                    ? value
                    : 0
            )
            .FirstOrDefault();
}
