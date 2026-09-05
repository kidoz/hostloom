using Microsoft.CodeAnalysis;

namespace HostLoom.Composition.Generators;

internal sealed partial class DeclarationCompiler
{
    private void Select(Rule rule)
    {
        IEnumerable<INamedTypeSymbol> candidates = rule.Discover
            ? DeclaredTypes(_model.Compilation.Assembly.GlobalNamespace)
            : rule.Types;
        var selected = candidates
            .Distinct<INamedTypeSymbol>(SymbolEqualityComparer.Default)
            .Where(type =>
                !rule.Discover
                || (
                    type.TypeKind == TypeKind.Class
                    && (
                        rule.Selectors.Count != 0
                            ? rule.Selectors.All(selector =>
                                selector.Any(filter => Matches(type, filter))
                            )
                            : rule
                                .Attributes.Where(static filter => filter.Required)
                                .Any(filter => HasAttribute(type, filter.Type))
                    )
                )
            )
            .OrderBy(
                static type => type.ContainingNamespace.ToDisplayString(),
                StringComparer.Ordinal
            )
            .ThenBy(MetadataName, StringComparer.Ordinal)
            .ToArray();
        var count = 0;
        foreach (INamedTypeSymbol type in selected)
        {
            _cancellation.ThrowIfCancellationRequested();
            var reasons = new List<string>();
            if (!rule.Selectors.All(selector => selector.Any(filter => Matches(type, filter))))
                reasons.Add("Does not satisfy assignability selectors.");
            foreach (var filter in rule.Attributes)
                if (HasAttribute(type, filter.Type) != filter.Required)
                    reasons.Add(
                        (filter.Required ? "Missing required attribute: " : "Excluded attribute: ")
                            + TypeName(filter.Type)
                    );
            if (type.TypeKind != TypeKind.Class)
                reasons.Add("Not a class.");
            if (type.IsAbstract)
                reasons.Add("Abstract class.");
            if (type.IsStatic)
                reasons.Add("Static class.");
            if (ContainsParameters(type))
                reasons.Add("Open generic candidate; use AddOpenGeneric for registration.");
            if (
                type.GetAttributes()
                    .Any(static attribute =>
                        attribute.AttributeClass?.ToDisplayString()
                        == "System.Runtime.CompilerServices.CompilerGeneratedAttribute"
                    )
            )
                reasons.Add("Compiler-generated candidate.");
            if (reasons.Count != 0)
            {
                _rejections.Add(new Rejection(type, rule, reasons));
                if (
                    !rule.Discover
                    && (
                        type.TypeKind != TypeKind.Class
                        || type.IsAbstract
                        || type.IsStatic
                        || ContainsParameters(type)
                        || type.GetAttributes()
                            .Any(static attribute =>
                                attribute.AttributeClass?.ToDisplayString()
                                == "System.Runtime.CompilerServices.CompilerGeneratedAttribute"
                            )
                    )
                )
                    Error(
                        CompositionDiagnostics.Selection,
                        rule.Syntax,
                        $"Explicit candidate '{type}' must be a closed concrete non-generated class."
                    );
                continue;
            }
            foreach (string required in rule.Namespaces)
            {
                string actual = type.ContainingNamespace.IsGlobalNamespace
                    ? ""
                    : type.ContainingNamespace.ToDisplayString();
                if (
                    actual != required
                    && !actual.StartsWith(required + ".", StringComparison.Ordinal)
                )
                {
                    Error(
                        CompositionDiagnostics.Selection,
                        rule.Syntax,
                        $"Selected type '{type}' is outside required namespace '{required}'.",
                        type.Locations.FirstOrDefault()
                    );
                    reasons.Add("Outside required namespace: " + required);
                }
            }
            if (reasons.Count != 0)
                continue;
            count++;
            if (
                type.IsFileLocal
                || !_model.Compilation.IsSymbolAccessibleWithin(type, _method.ContainingType)
            )
            {
                Error(
                    CompositionDiagnostics.Selection,
                    rule.Syntax,
                    $"Selected type '{type}' is inaccessible from the generated factory.",
                    type.Locations.FirstOrDefault()
                );
                continue;
            }
            if (
                !type.InstanceConstructors.Any(static constructor =>
                    constructor.DeclaredAccessibility == Accessibility.Public
                )
            )
            {
                Error(
                    CompositionDiagnostics.Projection,
                    rule.Syntax,
                    $"Implementation '{type}' needs a public constructor; dependency resolution remains a final-provider check.",
                    type.Locations.FirstOrDefault()
                );
                continue;
            }
            var services = new List<INamedTypeSymbol>();
            if (rule.Projection == "AsSelf")
                services.Add(type);
            else if (rule.Projection is "AsImplementedInterfaces" or "AsSelfWithInterfaces")
            {
                services.AddRange(
                    type.AllInterfaces.Where(service =>
                        rule.Selectors.SelectMany(static selector => selector)
                            .Any(filter =>
                                filter.TypeKind == TypeKind.Interface
                                && TypeMatches(service, filter)
                            )
                    )
                );
            }
            else if (rule.Projection == "AsAllImplementedInterfaces")
                services.AddRange(type.AllInterfaces);
            else
            {
                foreach (INamedTypeSymbol service in rule.Services)
                {
                    if (service.IsUnboundGenericType && service.TypeKind == TypeKind.Interface)
                    {
                        INamedTypeSymbol[] closed = type
                            .AllInterfaces.Where(candidate => TypeMatches(candidate, service))
                            .ToArray();
                        if (closed.Length == 0)
                            Error(
                                CompositionDiagnostics.Projection,
                                rule.Syntax,
                                $"Implementation '{type}' exposes no closed form of '{service}'."
                            );
                        services.AddRange(closed);
                    }
                    else if (!ContainsParameters(service) && Matches(type, service))
                        services.Add(service);
                    else
                        Error(
                            CompositionDiagnostics.Projection,
                            rule.Syntax,
                            $"Implementation '{type}' cannot be registered as '{service}'."
                        );
                }
            }
            if (services.Count == 0)
                Error(
                    CompositionDiagnostics.Projection,
                    rule.Syntax,
                    $"Rule projects no service interfaces for '{type}'; choose an explicit As service or AsSelf."
                );
            if (rule.Projection == "AsSelfWithInterfaces")
                services.Add(type);
            foreach (
                INamedTypeSymbol service in services
                    .Distinct<INamedTypeSymbol>(SymbolEqualityComparer.Default)
                    .OrderBy(TypeName, StringComparer.Ordinal)
            )
            {
                if (!_model.Compilation.IsSymbolAccessibleWithin(service, _method.ContainingType))
                    Error(
                        CompositionDiagnostics.Projection,
                        rule.Syntax,
                        $"Service '{service}' is inaccessible from the generated factory."
                    );
                else
                    _registrations.Add(new Registration(type, service, rule));
            }
        }
        ValidateCounts(rule, count);
    }

    private void ValidateCounts(Rule rule, int count)
    {
        foreach (var assertion in rule.Counts)
            if (assertion.Exact ? count != assertion.Count : count < assertion.Count)
                Error(
                    CompositionDiagnostics.Count,
                    assertion.Syntax,
                    $"Rule {rule.Number} expected {(assertion.Exact ? "exactly" : "at least")} {assertion.Count} eligible implementations but found {count}."
                );
        if (count == 0 && !rule.AllowEmpty)
            Error(
                CompositionDiagnostics.Selection,
                rule.Syntax,
                "Rule matched zero eligible types. Use AllowEmpty only when that absence is intentional."
            );
    }

    private static bool HasAttribute(INamedTypeSymbol type, INamedTypeSymbol required)
    {
        for (INamedTypeSymbol? current = type; current is not null; current = current.BaseType)
            foreach (var attribute in current.GetAttributes())
                if (
                    attribute.AttributeClass is { } actual
                    && Matches(actual, required)
                    && (
                        SymbolEqualityComparer.Default.Equals(current, type)
                        || AttributeIsInherited(actual)
                    )
                )
                    return true;
        return false;
    }

    private static bool AttributeIsInherited(INamedTypeSymbol attribute)
    {
        for (INamedTypeSymbol? current = attribute; current is not null; current = current.BaseType)
            foreach (var usage in current.GetAttributes())
                if (usage.AttributeClass?.ToDisplayString() == "System.AttributeUsageAttribute")
                    return !usage.NamedArguments.Any(static argument =>
                        argument.Key == "Inherited" && argument.Value.Value is false
                    );
        return true;
    }

    private IEnumerable<INamedTypeSymbol> DeclaredTypes(INamespaceOrTypeSymbol container)
    {
        _cancellation.ThrowIfCancellationRequested();
        foreach (ISymbol member in container.GetMembers())
        {
            if (member is INamedTypeSymbol type)
            {
                if (type.DeclaringSyntaxReferences.Length != 0)
                    yield return type;
                foreach (INamedTypeSymbol nested in DeclaredTypes(type))
                    yield return nested;
            }
            else if (member is INamespaceSymbol space)
                foreach (INamedTypeSymbol nested in DeclaredTypes(space))
                    yield return nested;
        }
    }

    private void ValidateConflicts()
    {
        for (var i = 0; i < _registrations.Count; i++)
        {
            Registration current = _registrations[i];
            for (var j = 0; j < i; j++)
            {
                Registration previous = _registrations[j];
                if (
                    SymbolEqualityComparer.Default.Equals(current.Service, previous.Service)
                    && (
                        current.Rule.Cardinality == "One"
                        || previous.Rule.Cardinality == "One"
                        || current.Rule.Lifetime != previous.Rule.Lifetime
                        || SymbolEqualityComparer.Default.Equals(
                            current.Implementation,
                            previous.Implementation
                        )
                    )
                )
                    Error(
                        CompositionDiagnostics.Conflict,
                        current.Rule.Syntax,
                        $"Service '{current.Service}' conflicts between '{previous.Implementation}' (rule {previous.Rule.Number}) and '{current.Implementation}' (rule {current.Rule.Number}): duplicate, cardinality or lifetime mismatch.",
                        previous.Rule.Syntax.GetLocation()
                    );
            }
        }
    }

    private static bool TypeMatches(INamedTypeSymbol type, INamedTypeSymbol filter) =>
        SymbolEqualityComparer.Default.Equals(type, filter)
        || filter.IsUnboundGenericType
            && SymbolEqualityComparer.Default.Equals(
                type.OriginalDefinition,
                filter.OriginalDefinition
            );

    private static bool Matches(INamedTypeSymbol type, INamedTypeSymbol filter)
    {
        for (INamedTypeSymbol? current = type; current is not null; current = current.BaseType)
            if (TypeMatches(current, filter))
                return true;
        return type.AllInterfaces.Any(service => TypeMatches(service, filter));
    }

    private static bool ContainsParameters(ITypeSymbol type) =>
        type is ITypeParameterSymbol
        || type is IArrayTypeSymbol array && ContainsParameters(array.ElementType)
        || type is INamedTypeSymbol named
            && (
                named.IsUnboundGenericType
                || named.ContainingType is not null && ContainsParameters(named.ContainingType)
                || named.TypeArguments.Any(ContainsParameters)
            );
}
