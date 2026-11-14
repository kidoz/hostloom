using System.Collections.Immutable;
using HostLoom.Analyzers.Infrastructure;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace HostLoom.Analyzers.Usage;

/// <summary>
/// Reports synchronous blocking over HostLoom Task and ValueTask operations, including the cache
/// and lock contracts.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class SyncOverAsyncAnalyzer : DiagnosticAnalyzer
{
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } =
        ImmutableArray.Create(HostLoomDiagnosticDescriptors.SyncOverAsync);

    public override void Initialize(AnalysisContext context)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSyntaxNodeAction(
            AnalyzeMemberAccess,
            SyntaxKind.SimpleMemberAccessExpression
        );
        context.RegisterSyntaxNodeAction(AnalyzeInvocation, SyntaxKind.InvocationExpression);
    }

    private static void AnalyzeMemberAccess(SyntaxNodeAnalysisContext context)
    {
        var memberAccess = (MemberAccessExpressionSyntax)context.Node;
        if (
            string.Equals(
                memberAccess.Name.Identifier.ValueText,
                "Result",
                StringComparison.Ordinal
            ) && TryGetHostLoomAsyncCallName(context, memberAccess.Expression) is { } methodName
        )
        {
            Report(context, memberAccess, methodName, ".Result");
        }
    }

    private static void AnalyzeInvocation(SyntaxNodeAnalysisContext context)
    {
        var invocation = (InvocationExpressionSyntax)context.Node;
        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
        {
            return;
        }

        string memberName = memberAccess.Name.Identifier.ValueText;
        if (
            string.Equals(memberName, "Wait", StringComparison.Ordinal)
            && TryGetHostLoomAsyncCallName(context, memberAccess.Expression) is { } waitTarget
        )
        {
            Report(context, invocation, waitTarget, ".Wait()");
            return;
        }

        if (
            string.Equals(memberName, "GetResult", StringComparison.Ordinal)
            && TryGetHostLoomAsyncCallName(context, memberAccess.Expression) is { } awaiterTarget
        )
        {
            Report(context, invocation, awaiterTarget, ".GetAwaiter().GetResult()");
        }
    }

    private static string? TryGetHostLoomAsyncCallName(
        SyntaxNodeAnalysisContext context,
        ExpressionSyntax expression,
        int depth = 0
    )
    {
        if (depth > 16)
            return null;
        if (expression is ParenthesizedExpressionSyntax parenthesized)
        {
            return TryGetHostLoomAsyncCallName(context, parenthesized.Expression, depth + 1);
        }

        if (
            expression is IdentifierNameSyntax
            && context.SemanticModel.GetSymbolInfo(expression).Symbol is ILocalSymbol local
            && local
                .DeclaringSyntaxReferences.FirstOrDefault()
                ?.GetSyntax(context.CancellationToken)
                is VariableDeclaratorSyntax { Initializer: { } initializer } declaration
        )
        {
            var scope = declaration
                .Ancestors()
                .FirstOrDefault(node =>
                    node
                        is BaseMethodDeclarationSyntax
                            or AnonymousFunctionExpressionSyntax
                            or LocalFunctionStatementSyntax
                );
            // Reassigned and by-reference locals have no single provable origin.
            if (
                scope is null
                || scope
                    .DescendantNodes()
                    .Any(node =>
                        node is AssignmentExpressionSyntax assignment
                            && assignment
                                .Left.DescendantNodesAndSelf()
                                .OfType<IdentifierNameSyntax>()
                                .Any(id =>
                                    SymbolEqualityComparer.Default.Equals(
                                        context.SemanticModel.GetSymbolInfo(id).Symbol,
                                        local
                                    )
                                )
                        || node is ArgumentSyntax argument
                            && !argument.RefKindKeyword.IsKind(SyntaxKind.None)
                            && SymbolEqualityComparer.Default.Equals(
                                context.SemanticModel.GetSymbolInfo(argument.Expression).Symbol,
                                local
                            )
                    )
            )
                return null;
            return TryGetHostLoomAsyncCallName(context, initializer.Value, depth + 1);
        }

        if (expression is not InvocationExpressionSyntax invocation)
        {
            return null;
        }

        if (
            context.SemanticModel.GetSymbolInfo(invocation).Symbol is IMethodSymbol method
            && AnalyzerSymbolHelpers.IsHostLoomAsyncOperation(method)
        )
        {
            return method.Name;
        }

        if (
            invocation.Expression is MemberAccessExpressionSyntax memberAccess
            && memberAccess.Name.Identifier.ValueText
                is "AsTask"
                    or "ConfigureAwait"
                    or "GetAwaiter"
        )
        {
            return TryGetHostLoomAsyncCallName(context, memberAccess.Expression, depth + 1);
        }

        return null;
    }

    private static void Report(
        SyntaxNodeAnalysisContext context,
        SyntaxNode node,
        string methodName,
        string blockingMember
    ) =>
        context.ReportDiagnostic(
            Diagnostic.Create(
                HostLoomDiagnosticDescriptors.SyncOverAsync,
                node.GetLocation(),
                methodName,
                blockingMember
            )
        );
}
