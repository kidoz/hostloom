using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace HostLoom.Composition.Generators;

/// <summary>Rejects invocation or delegate capture of declaration-only methods.</summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class CompositionDeclarationUsageAnalyzer : DiagnosticAnalyzer
{
    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } =
        ImmutableArray.Create(CompositionDiagnostics.Declaration);

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        if (context is null)
            throw new ArgumentNullException(nameof(context));
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterOperationAction(
            Analyze,
            OperationKind.Invocation,
            OperationKind.MethodReference
        );
    }

    private static void Analyze(OperationAnalysisContext context)
    {
        IMethodSymbol? method = context.Operation switch
        {
            IInvocationOperation invocation => invocation.TargetMethod,
            IMethodReferenceOperation reference => reference.Method,
            _ => null,
        };
        if (method is null)
            return;
        for (
            IOperation? parent = context.Operation.Parent;
            parent is not null;
            parent = parent.Parent
        )
            if (parent is INameOfOperation)
                return;
        INamedTypeSymbol? marker = context.Compilation.GetTypeByMetadataName(
            CompositionGenerator.AttributeName
        );
        if (
            marker is not null
            && method
                .GetAttributes()
                .Any(attribute =>
                    SymbolEqualityComparer.Default.Equals(attribute.AttributeClass, marker)
                )
        )
        {
            context.ReportDiagnostic(
                Diagnostic.Create(
                    CompositionDiagnostics.Declaration,
                    context.Operation.Syntax.GetLocation(),
                    $"Composition declaration '{method.Name}' cannot be invoked or captured as a delegate; call its generated plan factory."
                )
            );
        }
    }
}
