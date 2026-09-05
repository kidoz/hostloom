using Microsoft.CodeAnalysis;

namespace HostLoom.Composition.Generators;

internal sealed partial class DeclarationCompiler
{
    private void ValidateCaptures()
    {
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
                IMethodSymbol[] constructors = implementation
                    .InstanceConstructors.Where(static constructor =>
                        constructor.DeclaredAccessibility == Accessibility.Public
                    )
                    .ToArray();
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
                        && !_registrations.Any(registration =>
                            SymbolEqualityComparer.Default.Equals(registration.Service, dependency)
                            || registration.Rule.OpenGeneric
                                && SymbolEqualityComparer.Default.Equals(
                                    registration.Service.OriginalDefinition,
                                    dependency.OriginalDefinition
                                )
                        );
                    INamedTypeSymbol? requested = enumerable
                        ? dependency.TypeArguments[0] as INamedTypeSymbol
                        : dependency;
                    if (requested is null)
                        continue;
                    var exact = _registrations
                        .Where(registration =>
                            SymbolEqualityComparer.Default.Equals(registration.Service, requested)
                        )
                        .ToArray();
                    var open = _registrations
                        .Where(registration =>
                            registration.Rule.OpenGeneric
                            && requested.IsGenericType
                            && SymbolEqualityComparer.Default.Equals(
                                registration.Service.OriginalDefinition,
                                requested.OriginalDefinition
                            )
                        )
                        .ToArray();
                    IEnumerable<Registration> targets =
                        enumerable
                            ? _registrations.Where(registration =>
                                exact.Contains(registration) || open.Contains(registration)
                            )
                        : exact.Length != 0 ? exact.Skip(exact.Length - 1)
                        : open.Skip(Math.Max(0, open.Length - 1));
                    foreach (Registration target in targets)
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
}
