using System.Collections.Immutable;
using HostLoom.Analyzers.Infrastructure;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace HostLoom.Analyzers.Usage;

/// <summary>
/// Reports destination members an explicit map never assigns, and reports the maps whose bodies it
/// cannot check rather than passing over them.
/// </summary>
/// <remarks>
/// Every <c>Map</c> implementation lands in one of three states, so "no diagnostic" always means
/// "checked and complete" rather than "not looked at":
/// <list type="bullet">
/// <item>verified — the body is a recognised shape and every settable destination member is
/// assigned, named in <c>UnmappedMembers</c>, or supplied through the constructor;</item>
/// <item>not verifiable — the body is outside both shapes, reported as HLM0005;</item>
/// <item>not applicable — the destination has no settable public instance members, or is a
/// sequence, so there is nothing to be complete about.</item>
/// </list>
/// A map whose destination is a type parameter is the one blind spot: its members cannot be
/// enumerated, so it is skipped silently. That is documented in the analyzer README.
/// </remarks>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class MappingCompletenessAnalyzer : DiagnosticAnalyzer
{
    private const string MappingNamespace = "HostLoom.Mapping";
    private const string MapperInterface = "IMapper`2";
    private const string UnmappedMembers = "UnmappedMembersAttribute";

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } =
        ImmutableArray.Create(
            HostLoomDiagnosticDescriptors.UnassignedDestinationMember,
            HostLoomDiagnosticDescriptors.MappingNotVerifiable
        );

    public override void Initialize(AnalysisContext context)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterOperationBlockAction(AnalyzeMapBody);
    }

    private static void AnalyzeMapBody(OperationBlockAnalysisContext context)
    {
        if (
            context.OwningSymbol is not IMethodSymbol method
            || !IsMapImplementation(method)
            || method.ReturnType is not INamedTypeSymbol destination
        )
        {
            return;
        }

        ImmutableHashSet<ISymbol> required = RequiredMembers(destination);
        if (required.IsEmpty)
        {
            // Not applicable: nothing about this destination can be left unset.
            return;
        }

        IOperation? body = context.OperationBlocks.FirstOrDefault(block =>
            block.Kind is OperationKind.Block
        );
        if (body is null)
        {
            return;
        }

        MapBody classified = Classify(body, destination);
        if (classified.Reason is not null)
        {
            context.ReportDiagnostic(
                Diagnostic.Create(
                    HostLoomDiagnosticDescriptors.MappingNotVerifiable,
                    method.Locations.FirstOrDefault() ?? Location.None,
                    destination.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat),
                    classified.Reason
                )
            );
            return;
        }

        ImmutableHashSet<string> excused = DeclaredUnmapped(method.ContainingType);
        var missing = required
            .Where(member => !classified.Assigned.Contains(member, SymbolEqualityComparer.Default))
            .Where(member => !excused.Contains(member.Name))
            .Select(member => member.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        if (missing.Length == 0)
        {
            return;
        }

        context.ReportDiagnostic(
            Diagnostic.Create(
                HostLoomDiagnosticDescriptors.UnassignedDestinationMember,
                method.Locations.FirstOrDefault() ?? Location.None,
                destination.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat),
                string.Join(", ", missing)
            )
        );
    }

    /// <summary>Classifies a body into the assigned member set, or a reason it cannot be read.</summary>
    private static MapBody Classify(IOperation body, INamedTypeSymbol destination)
    {
        IReturnOperation[] returns = body.Descendants()
            .OfType<IReturnOperation>()
            .Where(candidate => candidate.ReturnedValue is not null)
            .ToArray();

        if (returns.Length == 0)
        {
            return MapBody.NotVerifiable("it returns no destination this analysis can follow");
        }

        // Shape A: every return hands back a freshly constructed destination.
        IObjectCreationOperation?[] created = returns
            .Select(candidate => Unwrap(candidate.ReturnedValue!) as IObjectCreationOperation)
            .ToArray();
        if (created.All(creation => creation is not null))
        {
            ImmutableHashSet<ISymbol>.Builder assigned = ImmutableHashSet.CreateBuilder<ISymbol>(
                SymbolEqualityComparer.Default
            );
            foreach (IObjectCreationOperation creation in created.Cast<IObjectCreationOperation>())
            {
                CollectFromCreation(creation, destination, assigned);
            }

            return MapBody.Verified(assigned.ToImmutable());
        }

        // Shape B: one local is constructed, assigned into, and returned.
        return ClassifyStatementForm(body, destination, returns);
    }

    private static MapBody ClassifyStatementForm(
        IOperation body,
        INamedTypeSymbol destination,
        IReturnOperation[] returns
    )
    {
        IVariableDeclaratorOperation[] declarations = body.Descendants()
            .OfType<IVariableDeclaratorOperation>()
            .Where(declarator =>
                SymbolEqualityComparer.Default.Equals(declarator.Symbol.Type, destination)
            )
            .ToArray();

        if (declarations.Length != 1)
        {
            return MapBody.NotVerifiable(
                declarations.Length == 0
                    ? "it neither returns a constructed destination nor builds one in a local"
                    : $"it builds the destination across {declarations.Length} locals rather than one"
            );
        }

        ILocalSymbol local = declarations[0].Symbol;
        if (
            Unwrap(declarations[0].Initializer?.Value ?? declarations[0])
            is not IObjectCreationOperation creation
        )
        {
            return MapBody.NotVerifiable(
                $"the local '{local.Name}' is not initialised with a new destination"
            );
        }

        foreach (IReturnOperation candidate in returns)
        {
            if (
                Unwrap(candidate.ReturnedValue!) is not ILocalReferenceOperation returned
                || !SymbolEqualityComparer.Default.Equals(returned.Local, local)
            )
            {
                return MapBody.NotVerifiable(
                    $"it returns something other than the local '{local.Name}' on some path"
                );
            }
        }

        ImmutableHashSet<ISymbol>.Builder assigned = ImmutableHashSet.CreateBuilder<ISymbol>(
            SymbolEqualityComparer.Default
        );
        CollectFromCreation(creation, destination, assigned);

        foreach (
            ILocalReferenceOperation reference in body.Descendants()
                .OfType<ILocalReferenceOperation>()
                .Where(reference => SymbolEqualityComparer.Default.Equals(reference.Local, local))
        )
        {
            // Past an escape the analysis genuinely cannot know what was assigned, so the map
            // drops to "not verifiable" rather than being reported as incomplete.
            if (Escapes(reference, out ISymbol? member))
            {
                return MapBody.NotVerifiable(
                    $"the local '{local.Name}' escapes before it is returned"
                );
            }

            if (member is not null)
            {
                assigned.Add(member);
            }
        }

        return MapBody.Verified(assigned.ToImmutable());
    }

    /// <summary>
    /// Decides whether one use of the local puts it beyond analysis. A member assignment through
    /// it, or returning it, is fine; anything else is treated as an escape, because being wrong in
    /// that direction reports a map rather than silently passing it.
    /// </summary>
    private static bool Escapes(ILocalReferenceOperation reference, out ISymbol? assignedMember)
    {
        assignedMember = null;
        IOperation? parent = reference.Parent;

        if (parent is IReturnOperation)
        {
            return false;
        }

        if (parent is IPropertyReferenceOperation or IFieldReferenceOperation)
        {
            ISymbol member = parent switch
            {
                IPropertyReferenceOperation property => property.Property,
                IFieldReferenceOperation field => field.Field,
                _ => throw new InvalidOperationException("unreachable"),
            };

            // local.Member = value assigns it; local.Member on the right-hand side only reads.
            if (
                parent.Parent is IAssignmentOperation assignment
                && ReferenceEquals(assignment.Target, parent)
            )
            {
                assignedMember = member;
            }

            return false;
        }

        return true;
    }

    private static void CollectFromCreation(
        IObjectCreationOperation creation,
        INamedTypeSymbol destination,
        ImmutableHashSet<ISymbol>.Builder assigned
    )
    {
        // A constructor argument is an assignment: it is how a record's positional members, and
        // any contract with a real constructor, receive their values.
        if (creation.Constructor is not null)
        {
            foreach (IParameterSymbol parameter in creation.Constructor.Parameters)
            {
                foreach (ISymbol member in destination.GetMembers())
                {
                    if (
                        string.Equals(
                            member.Name,
                            parameter.Name,
                            StringComparison.OrdinalIgnoreCase
                        )
                    )
                    {
                        assigned.Add(member);
                    }
                }
            }
        }

        if (creation.Initializer is null)
        {
            return;
        }

        foreach (IOperation initializer in creation.Initializer.Initializers)
        {
            if (initializer is not IAssignmentOperation assignment)
            {
                continue;
            }

            switch (assignment.Target)
            {
                case IPropertyReferenceOperation property:
                    assigned.Add(property.Property);
                    break;
                case IFieldReferenceOperation field:
                    assigned.Add(field.Field);
                    break;
                default:
                    break;
            }
        }
    }

    /// <summary>
    /// The members whose omission would be a silent loss: public instance state a caller could
    /// have written. A destination with none, or one that is itself a sequence, is not applicable.
    /// </summary>
    private static ImmutableHashSet<ISymbol> RequiredMembers(INamedTypeSymbol destination)
    {
        if (destination.TypeKind == TypeKind.TypeParameter || IsSequence(destination))
        {
            return ImmutableHashSet<ISymbol>.Empty;
        }

        ImmutableHashSet<ISymbol>.Builder builder = ImmutableHashSet.CreateBuilder<ISymbol>(
            SymbolEqualityComparer.Default
        );

        for (
            INamedTypeSymbol? type = destination;
            type is not null && type.SpecialType != SpecialType.System_Object;
            type = type.BaseType
        )
        {
            foreach (ISymbol member in type.GetMembers())
            {
                if (member.IsStatic || member.DeclaredAccessibility != Accessibility.Public)
                {
                    continue;
                }

                switch (member)
                {
                    case IPropertySymbol
                    {
                        IsIndexer: false,
                        SetMethod: { DeclaredAccessibility: Accessibility.Public },
                    }:
                        builder.Add(member);
                        break;
                    case IFieldSymbol
                    {
                        IsReadOnly: false,
                        IsConst: false,
                        IsImplicitlyDeclared: false
                    }:
                        builder.Add(member);
                        break;
                    default:
                        break;
                }
            }
        }

        return builder.ToImmutable();
    }

    private static bool IsSequence(INamedTypeSymbol destination) =>
        destination.SpecialType == SpecialType.System_String
        || destination.AllInterfaces.Any(@interface =>
            @interface.SpecialType == SpecialType.System_Collections_IEnumerable
        );

    private static ImmutableHashSet<string> DeclaredUnmapped(INamedTypeSymbol mapper)
    {
        ImmutableHashSet<string>.Builder builder = ImmutableHashSet.CreateBuilder(
            StringComparer.Ordinal
        );

        foreach (AttributeData attribute in mapper.GetAttributes())
        {
            if (
                !string.Equals(
                    attribute.AttributeClass?.MetadataName,
                    UnmappedMembers,
                    StringComparison.Ordinal
                )
                || !string.Equals(
                    attribute.AttributeClass?.ContainingNamespace?.ToDisplayString(),
                    MappingNamespace,
                    StringComparison.Ordinal
                )
            )
            {
                continue;
            }

            foreach (TypedConstant argument in attribute.ConstructorArguments)
            {
                foreach (TypedConstant value in argument.Values)
                {
                    if (value.Value is string name)
                    {
                        builder.Add(name);
                    }
                }
            }
        }

        return builder.ToImmutable();
    }

    private static bool IsMapImplementation(IMethodSymbol method)
    {
        if (
            !string.Equals(method.Name, "Map", StringComparison.Ordinal)
            || method.Parameters.Length != 1
            || method.IsStatic
        )
        {
            return false;
        }

        foreach (INamedTypeSymbol @interface in method.ContainingType.AllInterfaces)
        {
            if (
                !string.Equals(
                    @interface.OriginalDefinition.MetadataName,
                    MapperInterface,
                    StringComparison.Ordinal
                )
                || !string.Equals(
                    @interface.OriginalDefinition.ContainingNamespace?.ToDisplayString(),
                    MappingNamespace,
                    StringComparison.Ordinal
                )
            )
            {
                continue;
            }

            if (
                SymbolEqualityComparer.Default.Equals(
                    @interface.TypeArguments[1],
                    method.ReturnType
                )
                && SymbolEqualityComparer.Default.Equals(
                    @interface.TypeArguments[0],
                    method.Parameters[0].Type
                )
            )
            {
                return true;
            }
        }

        return false;
    }

    private static IOperation Unwrap(IOperation operation)
    {
        IOperation current = operation;
        while (current is IConversionOperation conversion)
        {
            current = conversion.Operand;
        }

        return current;
    }

    private readonly struct MapBody
    {
        private MapBody(ImmutableHashSet<ISymbol> assigned, string? reason)
        {
            Assigned = assigned;
            Reason = reason;
        }

        public ImmutableHashSet<ISymbol> Assigned { get; }

        /// <summary>Why the body could not be read, or null when it was.</summary>
        public string? Reason { get; }

        public static MapBody Verified(ImmutableHashSet<ISymbol> assigned) => new(assigned, null);

        public static MapBody NotVerifiable(string reason) =>
            new(ImmutableHashSet<ISymbol>.Empty, reason);
    }
}
