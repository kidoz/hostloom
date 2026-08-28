using System.Collections.Immutable;
using HostLoom.Analyzers.Infrastructure;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace HostLoom.Analyzers.Usage;

/// <summary>
/// Reports singleton dependency-injection registrations for HostLoom request handlers, event
/// handlers, and request behaviors.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class SingletonHandlerRegistrationAnalyzer : DiagnosticAnalyzer
{
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } =
        ImmutableArray.Create(HostLoomDiagnosticDescriptors.SingletonHandlerRegistration);

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
        if (!AnalyzerSymbolHelpers.IsSingletonRegistration(method))
        {
            return;
        }

        foreach (ITypeSymbol type in AnalyzerSymbolHelpers.RegistrationTypes(invocation))
        {
            if (!AnalyzerSymbolHelpers.IsHostLoomHandlerOrBehavior(type))
            {
                continue;
            }

            context.ReportDiagnostic(
                Diagnostic.Create(
                    HostLoomDiagnosticDescriptors.SingletonHandlerRegistration,
                    invocation.Syntax.GetLocation(),
                    type.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat)
                )
            );
            return;
        }
    }
}
