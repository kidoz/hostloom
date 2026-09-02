using System.Collections.Immutable;
using HostLoom.Analyzers.Infrastructure;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace HostLoom.Analyzers.Usage;

/// <summary>
/// Reports a cache or lock key built from a value whose name says it is a credential, unless the
/// value is wrapped in <c>CacheKey.FromSensitive</c> or <c>LockKey.FromSensitive</c>.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class SensitiveCacheKeyAnalyzer : DiagnosticAnalyzer
{
    private static readonly string[] SensitiveFragments =
    [
        "token",
        "secret",
        "password",
        "refreshtoken",
        "apikey",
    ];

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } =
        ImmutableArray.Create(HostLoomDiagnosticDescriptors.SensitiveCacheKey);

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
        if (!AnalyzerSymbolHelpers.IsCacheOrLockContractMember(method))
        {
            return;
        }

        string helper = AnalyzerSymbolHelpers.SensitiveKeyHelperName(method.ContainingType);
        foreach (IArgumentOperation argument in invocation.Arguments)
        {
            string? parameterName = argument.Parameter?.Name;
            if (string.Equals(parameterName, "key", StringComparison.Ordinal))
            {
                InspectKey(context, argument.Value, method.Name, helper);
            }
            else if (string.Equals(parameterName, "keys", StringComparison.Ordinal))
            {
                foreach (IOperation element in Elements(Unwrap(argument.Value)))
                {
                    InspectKey(context, element, method.Name, helper);
                }
            }
        }
    }

    private static void InspectKey(
        OperationAnalysisContext context,
        IOperation key,
        string methodName,
        string helper
    )
    {
        key = Unwrap(key);
        if (
            key is IInvocationOperation whole
            && AnalyzerSymbolHelpers.IsSensitiveKeyHelper(whole.TargetMethod)
        )
        {
            return;
        }

        foreach (IOperation operand in CompositionOperands(key))
        {
            foreach ((IOperation reference, string name) in SensitiveReferences(operand))
            {
                context.ReportDiagnostic(
                    Diagnostic.Create(
                        HostLoomDiagnosticDescriptors.SensitiveCacheKey,
                        reference.Syntax.GetLocation(),
                        methodName,
                        name,
                        helper
                    )
                );
            }
        }
    }

    /// <summary>
    /// The pieces a key is composed from: interpolation holes, concatenation operands, and the
    /// arguments of <c>string.Format</c>, <c>string.Concat</c>, and <c>string.Join</c>.
    /// </summary>
    private static IEnumerable<IOperation> CompositionOperands(IOperation key)
    {
        switch (key)
        {
            case IInterpolatedStringOperation interpolated:
                foreach (IInterpolatedStringContentOperation part in interpolated.Parts)
                {
                    if (part is IInterpolationOperation hole)
                    {
                        yield return hole.Expression;
                    }
                }

                break;
            case IBinaryOperation { OperatorKind: BinaryOperatorKind.Add } concatenation:
                foreach (
                    IOperation operand in CompositionOperands(Unwrap(concatenation.LeftOperand))
                )
                {
                    yield return operand;
                }

                foreach (
                    IOperation operand in CompositionOperands(Unwrap(concatenation.RightOperand))
                )
                {
                    yield return operand;
                }

                break;
            case IInvocationOperation invocation when IsStringComposition(invocation.TargetMethod):
                foreach (IArgumentOperation argument in invocation.Arguments)
                {
                    foreach (IOperation element in Elements(Unwrap(argument.Value)))
                    {
                        yield return element;
                    }
                }

                break;
            default:
                yield return key;
                break;
        }
    }

    /// <summary>
    /// References inside one operand to a parameter, local, field, or property with a
    /// credential-like name that are not wrapped in the sanctioned helper.
    /// </summary>
    private static IEnumerable<(IOperation Reference, string Name)> SensitiveReferences(
        IOperation operand
    )
    {
        operand = Unwrap(operand);
        if (
            operand is IInvocationOperation invocation
            && AnalyzerSymbolHelpers.IsSensitiveKeyHelper(invocation.TargetMethod)
        )
        {
            yield break;
        }

        foreach (IOperation candidate in operand.DescendantsAndSelf())
        {
            string? name = candidate switch
            {
                IParameterReferenceOperation parameter => parameter.Parameter.Name,
                ILocalReferenceOperation local => local.Local.Name,
                IFieldReferenceOperation field => field.Field.Name,
                IPropertyReferenceOperation property => property.Property.Name,
                _ => null,
            };
            if (
                name is null
                || AnalyzerSymbolHelpers.IsCancellationToken(candidate.Type)
                || !IsSensitiveName(name)
                || IsSanitized(candidate)
            )
            {
                continue;
            }

            yield return (candidate, name);
        }
    }

    /// <summary>Whether <paramref name="reference"/> sits inside a <c>FromSensitive</c> call.</summary>
    private static bool IsSanitized(IOperation reference)
    {
        for (IOperation? parent = reference.Parent; parent is not null; parent = parent.Parent)
        {
            if (
                parent is IInvocationOperation invocation
                && AnalyzerSymbolHelpers.IsSensitiveKeyHelper(invocation.TargetMethod)
            )
            {
                return true;
            }
        }

        return false;
    }

    private static IEnumerable<IOperation> Elements(IOperation value)
    {
        switch (value)
        {
            case ICollectionExpressionOperation collection:
                foreach (IOperation element in collection.Elements)
                {
                    yield return Unwrap(element);
                }

                break;
            case IArrayCreationOperation { Initializer: { } initializer }:
                foreach (IOperation element in initializer.ElementValues)
                {
                    yield return Unwrap(element);
                }

                break;
            default:
                yield return value;
                break;
        }
    }

    private static bool IsStringComposition(IMethodSymbol method) =>
        method.ContainingType?.SpecialType == SpecialType.System_String
        && (
            string.Equals(method.Name, "Format", StringComparison.Ordinal)
            || string.Equals(method.Name, "Concat", StringComparison.Ordinal)
            || string.Equals(method.Name, "Join", StringComparison.Ordinal)
        );

    private static bool IsSensitiveName(string name)
    {
        foreach (string fragment in SensitiveFragments)
        {
            if (name.IndexOf(fragment, StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return true;
            }
        }

        return false;
    }

    private static IOperation Unwrap(IOperation operation)
    {
        while (operation is IConversionOperation conversion)
        {
            operation = conversion.Operand;
        }

        return operation;
    }
}
