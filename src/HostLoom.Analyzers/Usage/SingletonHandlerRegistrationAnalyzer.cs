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
        if (!IsSingletonRegistration(method))
        {
            return;
        }

        foreach (ITypeSymbol type in RegistrationTypes(invocation))
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

    private static bool IsSingletonRegistration(IMethodSymbol method)
    {
        string? assemblyName = method.ContainingAssembly?.Name;
        if (
            assemblyName is null
            || !assemblyName.StartsWith(
                "Microsoft.Extensions.DependencyInjection",
                StringComparison.Ordinal
            )
        )
        {
            return false;
        }

        return string.Equals(method.Name, "AddSingleton", StringComparison.Ordinal)
            || string.Equals(method.Name, "TryAddSingleton", StringComparison.Ordinal)
            || string.Equals(method.Name, "AddKeyedSingleton", StringComparison.Ordinal)
            || string.Equals(method.Name, "TryAddKeyedSingleton", StringComparison.Ordinal)
            || string.Equals(method.Name, "Singleton", StringComparison.Ordinal);
    }

    private static IEnumerable<ITypeSymbol> RegistrationTypes(IInvocationOperation invocation)
    {
        foreach (ITypeSymbol type in invocation.TargetMethod.TypeArguments)
        {
            yield return type;
        }

        foreach (IArgumentOperation argument in invocation.Arguments)
        {
            IOperation value = argument.Value;
            while (value is IConversionOperation conversion)
            {
                value = conversion.Operand;
            }

            if (value is ITypeOfOperation typeOf)
            {
                yield return typeOf.TypeOperand;
            }
            else if (value is IObjectCreationOperation creation && creation.Type is not null)
            {
                yield return creation.Type;
            }
        }
    }
}
