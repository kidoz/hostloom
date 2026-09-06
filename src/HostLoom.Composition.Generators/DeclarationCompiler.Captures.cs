using Microsoft.CodeAnalysis;

namespace HostLoom.Composition.Generators;

internal sealed partial class DeclarationCompiler
{
    private void ValidateCaptures()
    {
        if (
            !_registrations.Any(static registration =>
                registration.Rule.Lifetime == "Singleton" && registration.Rule.Strategy != "Skip"
            )
        )
            return;
        var openServices = new Dictionary<INamedTypeSymbol, List<Registration>>(
            SymbolEqualityComparer.Default
        );
        foreach (Registration registration in _registrations)
        {
            if (!registration.Rule.OpenGeneric)
                continue;
            INamedTypeSymbol service = registration.Service.OriginalDefinition;
            if (!openServices.TryGetValue(service, out var group))
                openServices.Add(service, group = []);
            group.Add(registration);
        }
        foreach (Registration root in _registrations)
        {
            _cancellation.ThrowIfCancellationRequested();
            if (root.Rule.Lifetime != "Singleton" || root.Rule.Strategy == "Skip")
                continue;
            var visited = new HashSet<ITypeSymbol>(SymbolEqualityComparer.Default);
            Visit(
                root,
                root.Rule.OpenGeneric
                    ? root.Implementation.OriginalDefinition
                    : root.Implementation,
                [TypeName(root.Service)]
            );

            void Visit(Registration current, INamedTypeSymbol implementation, List<string> path)
            {
                _cancellation.ThrowIfCancellationRequested();
                if (current.Rule.Lifetime == "Scoped")
                {
                    Error(
                        CompositionDiagnostics.Capture,
                        root.Rule.Syntax,
                        $"Singleton '{root.Service}' captures a scoped service through {string.Join(" -> ", path)}. Only known plan edges were inspected.",
                        current.Rule.Syntax.GetLocation()
                    );
                    return;
                }
                if (!visited.Add(implementation) || path.Count > _registrations.Count + 1)
                    return;
                IMethodSymbol[] constructors = PublicConstructors(implementation);
                // With multiple constructors, external registrations can change DI's choice.
                if (constructors.Length != 1)
                    return;
                foreach (IParameterSymbol parameter in constructors[0].Parameters)
                {
                    if (
                        parameter
                            .GetAttributes()
                            .Any(static attribute =>
                                attribute.AttributeClass?.ToDisplayString()
                                    is "Microsoft.Extensions.DependencyInjection.FromKeyedServicesAttribute"
                                        or "Microsoft.Extensions.DependencyInjection.ServiceKeyAttribute"
                            )
                    )
                        continue;
                    if (parameter.Type is not INamedTypeSymbol dependency)
                        continue;
                    bool enumerable =
                        dependency.OriginalDefinition.SpecialType
                            == SpecialType.System_Collections_Generic_IEnumerable_T
                        && !_services.ContainsKey(dependency)
                        && !openServices.ContainsKey(dependency.OriginalDefinition);
                    INamedTypeSymbol? requested = enumerable
                        ? dependency.TypeArguments[0] as INamedTypeSymbol
                        : dependency;
                    if (requested is null)
                        continue;
                    foreach (
                        Registration target in CaptureTargets(requested, enumerable, openServices)
                    )
                    {
                        if (target.Rule.Strategy == "Skip")
                            continue;
                        INamedTypeSymbol targetImplementation = target.Rule.OpenGeneric
                            ? target.Implementation.OriginalDefinition.Construct(
                                requested.TypeArguments.ToArray()
                            )
                            : target.Implementation;
                        Visit(target, targetImplementation, [.. path, TypeName(requested)]);
                    }
                }
            }
        }
    }

    private IMethodSymbol[] PublicConstructors(INamedTypeSymbol implementation)
    {
        if (!_constructors.TryGetValue(implementation, out var constructors))
        {
            constructors = implementation
                .InstanceConstructors.Where(static constructor =>
                    constructor.DeclaredAccessibility == Accessibility.Public
                )
                .ToArray();
            _constructors.Add(implementation, constructors);
        }
        return constructors;
    }

    private IEnumerable<Registration> CaptureTargets(
        INamedTypeSymbol requested,
        bool enumerable,
        Dictionary<INamedTypeSymbol, List<Registration>> openServices
    )
    {
        IReadOnlyList<Registration> exact = _services.TryGetValue(requested, out var group)
            ? group.Registrations
            : Array.Empty<Registration>();
        IReadOnlyList<Registration> open =
            requested.IsGenericType
            && openServices.TryGetValue(requested.OriginalDefinition, out var openGroup)
                ? openGroup
                : Array.Empty<Registration>();
        if (!enumerable)
        {
            // An exact registration wins over an open generic; the last registration wins.
            if (exact.Count != 0)
                yield return exact[exact.Count - 1];
            else if (open.Count != 0)
                yield return open[open.Count - 1];
            yield break;
        }
        // Merge the two ordered groups without repeating a registration present in both.
        var exactIndex = 0;
        var openIndex = 0;
        while (exactIndex < exact.Count || openIndex < open.Count)
        {
            if (
                openIndex == open.Count
                || exactIndex < exact.Count && exact[exactIndex].Index <= open[openIndex].Index
            )
            {
                Registration next = exact[exactIndex++];
                if (openIndex < open.Count && next.Index == open[openIndex].Index)
                    openIndex++;
                yield return next;
            }
            else
                yield return open[openIndex++];
        }
    }
}
