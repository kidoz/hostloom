using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Operations;

namespace HostLoom.Analyzers.Infrastructure;

internal static class AnalyzerSymbolHelpers
{
    private const string DependencyInjectionAssembly = "Microsoft.Extensions.DependencyInjection";
    private const string HostingAssembly = "Microsoft.Extensions.Hosting";

    /// <summary>Recognises the container calls that register an implementation as a singleton.</summary>
    public static bool IsSingletonRegistration(IMethodSymbol method)
    {
        if (!IsDependencyInjectionCall(method))
        {
            return false;
        }

        return string.Equals(method.Name, "AddSingleton", StringComparison.Ordinal)
            || string.Equals(method.Name, "TryAddSingleton", StringComparison.Ordinal)
            || string.Equals(method.Name, "AddKeyedSingleton", StringComparison.Ordinal)
            || string.Equals(method.Name, "TryAddKeyedSingleton", StringComparison.Ordinal)
            || string.Equals(method.Name, "Singleton", StringComparison.Ordinal);
    }

    /// <summary>
    /// Recognises hosted service registration, which is a singleton registration by another name —
    /// the host resolves an <c>IHostedService</c> once and holds it for the process. The extension
    /// ships in the hosting assembly rather than the dependency-injection one.
    /// </summary>
    public static bool IsHostedServiceRegistration(IMethodSymbol method) =>
        method.ContainingAssembly?.Name is string assemblyName
        && assemblyName.StartsWith(HostingAssembly, StringComparison.Ordinal)
        && string.Equals(method.Name, "AddHostedService", StringComparison.Ordinal);

    /// <summary>The implementation types a registration call names, by type argument or by value.</summary>
    public static IEnumerable<ITypeSymbol> RegistrationTypes(IInvocationOperation invocation)
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

    private static bool IsDependencyInjectionCall(IMethodSymbol method)
    {
        string? assemblyName = method.ContainingAssembly?.Name;
        return assemblyName is not null
            && assemblyName.StartsWith(DependencyInjectionAssembly, StringComparison.Ordinal);
    }

    private const string AssemblyMetadataAttribute = "System.Reflection.AssemblyMetadataAttribute";
    private const string FrameworkAssemblyMarker = "HostLoom.FrameworkAssembly";
    private const string HostLoomNamespace = "HostLoom";
    private const string PipelineNamespace = "HostLoom.Pipelines";
    private const string CachingNamespace = "HostLoom.Caching";
    private const string LockingNamespace = "HostLoom.Locking";

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

    /// <summary>
    /// The consumer contracts of the caching and locking kernels, recognised by metadata name so
    /// the analyzer never references those packages and works against any assembly that declares
    /// them, including a test stub.
    /// </summary>
    public static bool IsCacheOrLockContract(ITypeSymbol? type) =>
        IsNamedType(type, CachingNamespace, "ICache")
        || IsNamedType(type, LockingNamespace, "IDistributedLock")
        || IsNamedType(type, LockingNamespace, "ILockHandle");

    /// <summary>Whether <paramref name="method"/> is declared on one of the cache or lock contracts.</summary>
    public static bool IsCacheOrLockContractMember(IMethodSymbol? method) =>
        method is not null && IsCacheOrLockContract(method.ContainingType);

    /// <summary>
    /// An asynchronous HostLoom operation: an <c>…Async</c> method returning a task-like type,
    /// declared in a framework assembly or on a cache or lock contract.
    /// </summary>
    public static bool IsHostLoomAsyncOperation(IMethodSymbol? method) =>
        method is not null
        && (IsHostLoomSymbol(method) || IsCacheOrLockContractMember(method))
        && method.Name.EndsWith("Async", StringComparison.Ordinal)
        && IsAwaitable(method.ReturnType);

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
