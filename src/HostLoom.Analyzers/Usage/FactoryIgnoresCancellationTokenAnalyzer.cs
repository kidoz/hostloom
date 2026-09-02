using System.Collections.Immutable;
using HostLoom.Analyzers.Infrastructure;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace HostLoom.Analyzers.Usage;

/// <summary>
/// Reports a factory passed to <c>ICache.GetOrCreateAsync</c> that declares the cancellation
/// token it is given and never uses it, so the work it starts outlives the caller.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class FactoryIgnoresCancellationTokenAnalyzer : DiagnosticAnalyzer
{
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } =
        ImmutableArray.Create(HostLoomDiagnosticDescriptors.FactoryIgnoresCancellationToken);

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
        if (!AnalyzerSymbolHelpers.IsGetOrCreate(invocation.TargetMethod))
        {
            return;
        }

        foreach (IArgumentOperation argument in invocation.Arguments)
        {
            if (!string.Equals(argument.Parameter?.Name, "factory", StringComparison.Ordinal))
            {
                continue;
            }

            IOperation value = argument.Value;
            while (value is IConversionOperation conversion)
            {
                value = conversion.Operand;
            }

            if (value is IDelegateCreationOperation creation)
            {
                value = creation.Target;
            }

            switch (value)
            {
                case IAnonymousFunctionOperation lambda:
                    Inspect(context, invocation, lambda.Symbol, lambda.Body, lambda.Syntax);
                    break;
                case IMethodReferenceOperation group:
                    InspectMethodGroup(context, invocation, group);
                    break;
                default:
                    break;
            }
        }
    }

    private static void InspectMethodGroup(
        OperationAnalysisContext context,
        IInvocationOperation invocation,
        IMethodReferenceOperation group
    )
    {
        IMethodSymbol method = group.Method;
        if (method.DeclaringSyntaxReferences.Length != 1)
        {
            return;
        }

        SyntaxNode declaration = method
            .DeclaringSyntaxReferences[0]
            .GetSyntax(context.CancellationToken);
        // Only a target declared in the same file can be inspected with the model the analyzer
        // already holds; a method group into another file is left alone rather than guessed at.
        if (
            invocation.SemanticModel is not { } model
            || declaration.SyntaxTree != invocation.Syntax.SyntaxTree
        )
        {
            return;
        }

        IOperation? body = model.GetOperation(declaration, context.CancellationToken);
        if (body is null)
        {
            return;
        }

        Inspect(context, invocation, method, body, group.Syntax);
    }

    private static void Inspect(
        OperationAnalysisContext context,
        IInvocationOperation invocation,
        IMethodSymbol factory,
        IOperation body,
        SyntaxNode reportAt
    )
    {
        IParameterSymbol? token = null;
        foreach (IParameterSymbol parameter in factory.Parameters)
        {
            if (AnalyzerSymbolHelpers.IsCancellationToken(parameter.Type))
            {
                token = parameter;
                break;
            }
        }

        // A discard says "I know, and this factory has nothing to cancel"; that is the opt-out.
        if (token is null || string.Equals(token.Name, "_", StringComparison.Ordinal))
        {
            return;
        }

        foreach (IOperation operation in body.DescendantsAndSelf())
        {
            if (
                operation is IParameterReferenceOperation reference
                && SymbolEqualityComparer.Default.Equals(reference.Parameter, token)
            )
            {
                return;
            }
        }

        Location location =
            token.Locations.Length > 0 ? token.Locations[0] : reportAt.GetLocation();
        context.ReportDiagnostic(
            Diagnostic.Create(
                HostLoomDiagnosticDescriptors.FactoryIgnoresCancellationToken,
                location,
                invocation.TargetMethod.Name,
                token.Name
            )
        );
    }
}
