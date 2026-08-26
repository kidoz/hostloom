using System.Collections.Immutable;
using HostLoom.Analyzers.Infrastructure;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace HostLoom.Analyzers.Usage;

/// <summary>
/// Reports HostLoom asynchronous calls that omit a cancellation token already available through
/// an enclosing parameter or pipeline context.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class MissingCancellationTokenAnalyzer : DiagnosticAnalyzer
{
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } =
        ImmutableArray.Create(HostLoomDiagnosticDescriptors.MissingCancellationToken);

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
            !AnalyzerSymbolHelpers.IsHostLoomSymbol(method)
            || !method.Name.EndsWith("Async", StringComparison.Ordinal)
            || !AnalyzerSymbolHelpers.IsAwaitable(method.ReturnType)
            || !AcceptsCancellationToken(method)
            || PassesCancellationToken(invocation)
            || invocation.SemanticModel is not { } semanticModel
            || FindAvailableCancellation(invocation.Syntax, semanticModel)
                is not { } cancellationExpression
        )
        {
            return;
        }

        context.ReportDiagnostic(
            Diagnostic.Create(
                HostLoomDiagnosticDescriptors.MissingCancellationToken,
                invocation.Syntax.GetLocation(),
                method.Name,
                cancellationExpression
            )
        );
    }

    private static bool AcceptsCancellationToken(IMethodSymbol method)
    {
        foreach (IParameterSymbol parameter in method.Parameters)
        {
            if (AnalyzerSymbolHelpers.IsCancellationToken(parameter.Type))
            {
                return true;
            }
        }

        return false;
    }

    private static bool PassesCancellationToken(IInvocationOperation invocation)
    {
        foreach (IArgumentOperation argument in invocation.Arguments)
        {
            if (
                !argument.IsImplicit
                && argument.ArgumentKind != ArgumentKind.DefaultValue
                && AnalyzerSymbolHelpers.IsCancellationToken(argument.Parameter?.Type)
            )
            {
                return true;
            }
        }

        return false;
    }

    private static string? FindAvailableCancellation(SyntaxNode node, SemanticModel semanticModel)
    {
        foreach (SyntaxNode ancestor in node.Ancestors())
        {
            IEnumerable<ParameterSyntax>? parameters = ancestor switch
            {
                MethodDeclarationSyntax method => method.ParameterList.Parameters,
                LocalFunctionStatementSyntax localFunction => localFunction
                    .ParameterList
                    .Parameters,
                ParenthesizedLambdaExpressionSyntax lambda => lambda.ParameterList.Parameters,
                SimpleLambdaExpressionSyntax lambda => [lambda.Parameter],
                AnonymousMethodExpressionSyntax { ParameterList: { } parameterList } =>
                    parameterList.Parameters,
                _ => null,
            };
            if (parameters is null)
            {
                continue;
            }

            foreach (ParameterSyntax parameter in parameters)
            {
                if (
                    semanticModel.GetDeclaredSymbol(parameter)
                        is IParameterSymbol { Type: { } type }
                    && AnalyzerSymbolHelpers.IsCancellationToken(type)
                )
                {
                    return parameter.Identifier.ValueText;
                }
            }

            foreach (ParameterSyntax parameter in parameters)
            {
                if (
                    semanticModel.GetDeclaredSymbol(parameter)
                        is IParameterSymbol { Type: { } type }
                    && AnalyzerSymbolHelpers.IsPipeContext(type)
                )
                {
                    return parameter.Identifier.ValueText + ".CancellationToken";
                }
            }

            if (DoesNotCaptureOuterScope(ancestor))
            {
                return null;
            }
        }

        return null;
    }

    private static bool DoesNotCaptureOuterScope(SyntaxNode node) =>
        node is MethodDeclarationSyntax
        || node is LocalFunctionStatementSyntax localFunction
            && localFunction.Modifiers.Any(SyntaxKind.StaticKeyword)
        || node is ParenthesizedLambdaExpressionSyntax parenthesizedLambda
            && parenthesizedLambda.Modifiers.Any(SyntaxKind.StaticKeyword)
        || node is SimpleLambdaExpressionSyntax simpleLambda
            && simpleLambda.Modifiers.Any(SyntaxKind.StaticKeyword)
        || node is AnonymousMethodExpressionSyntax anonymousMethod
            && anonymousMethod.Modifiers.Any(SyntaxKind.StaticKeyword);
}
