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

        MapBody classified = Classify(body, required);
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
    private static MapBody Classify(IOperation body, ImmutableHashSet<ISymbol> required)
    {
        IOperation[] operations = OperationsInCurrentFunction(body).ToArray();
        IReturnOperation[] returns = operations
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
                CollectFromCreation(creation, required, assigned);
            }

            return MapBody.Verified(assigned.ToImmutable());
        }

        // Shape B: one local is constructed, assigned into, and returned.
        return ClassifyStatementForm(operations, required, returns);
    }

    private static MapBody ClassifyStatementForm(
        IOperation[] operations,
        ImmutableHashSet<ISymbol> required,
        IReturnOperation[] returns
    )
    {
        if (Unwrap(returns[0].ReturnedValue!) is not ILocalReferenceOperation firstReturn)
        {
            return MapBody.NotVerifiable(
                "it neither returns a constructed destination nor returns one local on every path"
            );
        }

        // Follow the returned symbol: its declared type may be the concrete implementation of
        // an interface destination, or a derived class returned through its base contract.
        ILocalSymbol local = firstReturn.Local;
        IVariableDeclaratorOperation? declaration = operations
            .OfType<IVariableDeclaratorOperation>()
            .FirstOrDefault(candidate =>
                SymbolEqualityComparer.Default.Equals(candidate.Symbol, local)
            );
        if (
            declaration?.Initializer is null
            || Unwrap(declaration.Initializer.Value) is not IObjectCreationOperation creation
        )
        {
            return MapBody.NotVerifiable(
                $"the local '{local.Name}' is not initialised with a new destination"
            );
        }

        // A closure can fill or replace the local when invoked. Do not count its assignments
        // as writes by this method, or claim completeness without following its execution.
        if (
            operations
                .Where(operation =>
                    operation is IAnonymousFunctionOperation or ILocalFunctionOperation
                )
                .SelectMany(function => function.Descendants())
                .OfType<ILocalReferenceOperation>()
                .Any(reference => SymbolEqualityComparer.Default.Equals(reference.Local, local))
        )
        {
            return MapBody.NotVerifiable(
                $"the local '{local.Name}' is captured by a nested function"
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
        CollectFromCreation(creation, required, assigned);

        foreach (
            ILocalReferenceOperation reference in operations
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
                RecordAssignment(member, creation.Type as INamedTypeSymbol, required, assigned);
            }
        }

        return MapBody.Verified(assigned.ToImmutable());
    }

    /// <summary>Enumerates this function's operations without entering another execution body.</summary>
    private static IEnumerable<IOperation> OperationsInCurrentFunction(IOperation body)
    {
        var pending = new Stack<IOperation>();
        pending.Push(body);
        while (pending.Count > 0)
        {
            IOperation operation = pending.Pop();
            yield return operation;

            if (operation is IAnonymousFunctionOperation or ILocalFunctionOperation)
            {
                continue;
            }

            foreach (IOperation child in operation.ChildOperations)
            {
                pending.Push(child);
            }
        }
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
        while (parent is IConversionOperation { OperatorMethod: null })
        {
            parent = parent.Parent;
        }

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
        ImmutableHashSet<ISymbol> required,
        ImmutableHashSet<ISymbol>.Builder assigned
    )
    {
        // A constructor argument is an assignment: it is how a record's positional members, and
        // any contract with a real constructor, receive their values.
        if (creation.Constructor is not null)
        {
            foreach (IParameterSymbol parameter in creation.Constructor.Parameters)
            {
                foreach (ISymbol member in required)
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
                    RecordAssignment(
                        property.Property,
                        creation.Type as INamedTypeSymbol,
                        required,
                        assigned
                    );
                    break;
                case IFieldReferenceOperation field:
                    RecordAssignment(
                        field.Field,
                        creation.Type as INamedTypeSymbol,
                        required,
                        assigned
                    );
                    break;
                default:
                    break;
            }
        }
    }

    private static void RecordAssignment(
        ISymbol member,
        INamedTypeSymbol? implementation,
        ImmutableHashSet<ISymbol> required,
        ImmutableHashSet<ISymbol>.Builder assigned
    )
    {
        ISymbol canonical = CanonicalMember(member);
        assigned.Add(canonical);

        // Initializers and concrete locals name implementation properties, whereas an interface
        // destination requires interface symbols. Name equality would also excuse unrelated
        // properties beside explicit implementations, so use the compiler's implementation map.
        foreach (ISymbol contract in required)
        {
            if (
                contract.ContainingType.TypeKind == TypeKind.Interface
                && implementation?.FindImplementationForInterfaceMember(contract)
                    is ISymbol implemented
                && SymbolEqualityComparer.Default.Equals(CanonicalMember(implemented), canonical)
            )
            {
                assigned.Add(contract);
            }
        }
    }

    private static ISymbol CanonicalMember(ISymbol member)
    {
        while (member is IPropertySymbol { OverriddenProperty: { } overridden })
        {
            member = overridden;
        }

        return member;
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

        foreach (INamedTypeSymbol type in DestinationTypes(destination))
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
                        builder.Add(CanonicalMember(member));
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

    private static IEnumerable<INamedTypeSymbol> DestinationTypes(INamedTypeSymbol destination)
    {
        for (
            INamedTypeSymbol? type = destination;
            type is not null && type.SpecialType != SpecialType.System_Object;
            type = type.BaseType
        )
        {
            yield return type;
        }

        if (destination.TypeKind == TypeKind.Interface)
        {
            foreach (INamedTypeSymbol inherited in destination.AllInterfaces)
            {
                yield return inherited;
            }
        }
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
        if (method.Parameters.Length != 1 || method.IsStatic)
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

            foreach (ISymbol member in @interface.GetMembers("Map"))
            {
                if (
                    SymbolEqualityComparer.Default.Equals(
                        method.ContainingType.FindImplementationForInterfaceMember(member),
                        method
                    )
                )
                {
                    return true;
                }
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
