using System.Collections.Immutable;
using HostLoom.Analyzers.Infrastructure;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace HostLoom.Analyzers.Usage;

/// <summary>
/// Reports a type that takes the scoped mapping dispatcher through its constructor and is then
/// registered as a singleton or a hosted service.
/// </summary>
/// <remarks>
/// The failure mode this exists for is the asymmetric one: capturing a scoped service in a
/// singleton throws at host build where scope validation is on — the generic host's default in
/// Development — and succeeds where it is off. Left to run time it is a Development-only failure
/// that Production would not reproduce, which is the wrong way round. Only the non-generic
/// <c>IMapper</c> is affected; a closed <c>IMapper&lt;TSource, TDestination&gt;</c> is transient by
/// default and is the injection a singleton should take.
/// </remarks>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class SingletonMapperInjectionAnalyzer : DiagnosticAnalyzer
{
    private const string MappingNamespace = "HostLoom.Mapping";
    private const string DispatcherInterface = "IMapper";

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } =
        ImmutableArray.Create(HostLoomDiagnosticDescriptors.SingletonMapperInjection);

    public override void Initialize(AnalysisContext context)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterOperationAction(AnalyzeInvocation, OperationKind.Invocation);
    }

    private static void AnalyzeInvocation(OperationAnalysisContext context)
    {
        var invocation = (IInvocationOperation)context.Operation;
        IMethodSymbol method = invocation.TargetMethod;
        if (
            !AnalyzerSymbolHelpers.IsSingletonRegistration(method)
            && !AnalyzerSymbolHelpers.IsHostedServiceRegistration(method)
        )
        {
            return;
        }

        foreach (ITypeSymbol type in AnalyzerSymbolHelpers.RegistrationTypes(invocation))
        {
            if (!TakesDispatcher(type))
            {
                continue;
            }

            context.ReportDiagnostic(
                Diagnostic.Create(
                    HostLoomDiagnosticDescriptors.SingletonMapperInjection,
                    invocation.Syntax.GetLocation(),
                    type.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat)
                )
            );
            return;
        }
    }

    private static bool TakesDispatcher(ITypeSymbol type)
    {
        if (type is not INamedTypeSymbol namedType)
        {
            return false;
        }

        foreach (IMethodSymbol constructor in namedType.InstanceConstructors)
        {
            foreach (IParameterSymbol parameter in constructor.Parameters)
            {
                if (IsDispatcher(parameter.Type))
                {
                    return true;
                }
            }
        }

        return false;
    }

    /// <summary>
    /// The non-generic dispatcher only. <c>IMapper&lt;TSource, TDestination&gt;</c> has the same
    /// name and a different arity, and is the safe injection this rule points at.
    /// </summary>
    private static bool IsDispatcher(ITypeSymbol type) =>
        type is INamedTypeSymbol { IsGenericType: false } named
        && string.Equals(named.MetadataName, DispatcherInterface, StringComparison.Ordinal)
        && string.Equals(
            named.ContainingNamespace?.ToDisplayString(),
            MappingNamespace,
            StringComparison.Ordinal
        );
}
