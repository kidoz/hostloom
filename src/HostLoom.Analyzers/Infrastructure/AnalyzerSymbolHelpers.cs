using Microsoft.CodeAnalysis;

namespace HostLoom.Analyzers.Infrastructure;

internal static class AnalyzerSymbolHelpers
{
    private const string AssemblyMetadataAttribute = "System.Reflection.AssemblyMetadataAttribute";
    private const string FrameworkAssemblyMarker = "HostLoom.FrameworkAssembly";
    private const string HostLoomNamespace = "HostLoom";
    private const string PipelineNamespace = "HostLoom.Pipelines";

    public static bool IsHostLoomSymbol(ISymbol? symbol)
    {
        IAssemblySymbol? assembly = symbol?.ContainingAssembly;
        if (assembly is null)
        {
            return false;
        }

        foreach (AttributeData attribute in assembly.GetAttributes())
        {
            if (
                string.Equals(
                    attribute.AttributeClass?.ToDisplayString(),
                    AssemblyMetadataAttribute,
                    StringComparison.Ordinal
                )
                && attribute.ConstructorArguments.Length == 2
                && attribute.ConstructorArguments[0].Value is string key
                && attribute.ConstructorArguments[1].Value is string value
                && string.Equals(key, FrameworkAssemblyMarker, StringComparison.Ordinal)
                && string.Equals(value, "true", StringComparison.OrdinalIgnoreCase)
            )
            {
                return true;
            }
        }

        return false;
    }

    public static bool IsCancellationToken(ITypeSymbol? type) =>
        IsNamedType(type, "System.Threading", "CancellationToken");

    public static bool IsPipeContext(ITypeSymbol? type)
    {
        if (IsNamedType(type, PipelineNamespace, "IPipeContext"))
        {
            return true;
        }

        if (type is not INamedTypeSymbol namedType)
        {
            return false;
        }

        foreach (INamedTypeSymbol @interface in namedType.AllInterfaces)
        {
            if (IsNamedType(@interface, PipelineNamespace, "IPipeContext"))
            {
                return true;
            }
        }

        return false;
    }

    public static bool IsAwaitable(ITypeSymbol? type)
    {
        if (type is not INamedTypeSymbol namedType)
        {
            return false;
        }

        string metadataName = namedType.OriginalDefinition.MetadataName;
        return string.Equals(
                namedType.ContainingNamespace?.ToDisplayString(),
                "System.Threading.Tasks",
                StringComparison.Ordinal
            )
            && (
                string.Equals(metadataName, "Task", StringComparison.Ordinal)
                || string.Equals(metadataName, "Task`1", StringComparison.Ordinal)
                || string.Equals(metadataName, "ValueTask", StringComparison.Ordinal)
                || string.Equals(metadataName, "ValueTask`1", StringComparison.Ordinal)
            );
    }

    public static bool IsHostLoomHandlerOrBehavior(ITypeSymbol? type)
    {
        if (type is not INamedTypeSymbol namedType)
        {
            return false;
        }

        if (IsHandlerContract(namedType))
        {
            return true;
        }

        foreach (INamedTypeSymbol @interface in namedType.AllInterfaces)
        {
            if (IsHandlerContract(@interface))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsHandlerContract(INamedTypeSymbol type)
    {
        INamedTypeSymbol definition = type.OriginalDefinition;
        if (
            !string.Equals(
                definition.ContainingNamespace?.ToDisplayString(),
                HostLoomNamespace,
                StringComparison.Ordinal
            )
        )
        {
            return false;
        }

        return string.Equals(definition.MetadataName, "IRequestHandler`2", StringComparison.Ordinal)
            || string.Equals(definition.MetadataName, "IEventHandler`1", StringComparison.Ordinal)
            || string.Equals(
                definition.MetadataName,
                "IRequestBehavior`2",
                StringComparison.Ordinal
            );
    }

    private static bool IsNamedType(ITypeSymbol? type, string @namespace, string metadataName) =>
        type is INamedTypeSymbol namedType
        && string.Equals(
            namedType.OriginalDefinition.MetadataName,
            metadataName,
            StringComparison.Ordinal
        )
        && string.Equals(
            namedType.ContainingNamespace?.ToDisplayString(),
            @namespace,
            StringComparison.Ordinal
        );
}
