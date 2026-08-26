using System.Collections.Immutable;
using HostLoom.Analyzers.Infrastructure;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace HostLoom.Analyzers.Usage;

/// <summary>
/// Reports synchronous blocking over HostLoom Task and ValueTask operations.
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
            && memberAccess.Expression is InvocationExpressionSyntax awaiterInvocation
            && awaiterInvocation.Expression is MemberAccessExpressionSyntax awaiterAccess
            && string.Equals(
                awaiterAccess.Name.Identifier.ValueText,
                "GetAwaiter",
                StringComparison.Ordinal
            )
            && TryGetHostLoomAsyncCallName(context, awaiterAccess.Expression) is { } awaiterTarget
        )
        {
            Report(context, invocation, awaiterTarget, ".GetAwaiter().GetResult()");
        }
    }

    private static string? TryGetHostLoomAsyncCallName(
        SyntaxNodeAnalysisContext context,
        ExpressionSyntax expression
    )
    {
        if (expression is ParenthesizedExpressionSyntax parenthesized)
        {
            return TryGetHostLoomAsyncCallName(context, parenthesized.Expression);
        }

        if (expression is not InvocationExpressionSyntax invocation)
        {
            return null;
        }

        if (
            context.SemanticModel.GetSymbolInfo(invocation).Symbol is IMethodSymbol method
            && AnalyzerSymbolHelpers.IsHostLoomSymbol(method)
            && method.Name.EndsWith("Async", StringComparison.Ordinal)
            && AnalyzerSymbolHelpers.IsAwaitable(method.ReturnType)
        )
        {
            return method.Name;
        }

        if (
            invocation.Expression is MemberAccessExpressionSyntax memberAccess
            && string.Equals(
                memberAccess.Name.Identifier.ValueText,
                "AsTask",
                StringComparison.Ordinal
            )
        )
        {
            return TryGetHostLoomAsyncCallName(context, memberAccess.Expression);
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
