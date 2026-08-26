using Microsoft.CodeAnalysis;

namespace HostLoom.Analyzers.Infrastructure;

/// <summary>Diagnostic identifiers reported by HostLoom analyzers.</summary>
public static class HostLoomDiagnosticDescriptors
{
    public const string MissingCancellationTokenDiagnosticId = "HLM0001";

    public const string SyncOverAsyncDiagnosticId = "HLM0002";

    public const string SingletonHandlerRegistrationDiagnosticId = "HLM0003";

    internal static readonly DiagnosticDescriptor MissingCancellationToken = new(
        MissingCancellationTokenDiagnosticId,
        "Pass an available cancellation token to HostLoom async calls",
        "Async HostLoom call '{0}' does not pass the cancellation token available as '{1}'",
        "Usage",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "HostLoom operations should observe the cancellation token already available in the enclosing method or pipeline context.",
        helpLinkUri: "https://github.com/kidoz/hostloom/tree/main/src/HostLoom.Analyzers#hlm0001"
    );

    internal static readonly DiagnosticDescriptor SyncOverAsync = new(
        SyncOverAsyncDiagnosticId,
        "Avoid blocking on HostLoom async operations",
        "Blocking on async HostLoom call '{0}' with '{1}' can deadlock; await it instead",
        "Usage",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Blocking on HostLoom Task or ValueTask results can exhaust the thread pool and deadlock applications.",
        helpLinkUri: "https://github.com/kidoz/hostloom/tree/main/src/HostLoom.Analyzers#hlm0002"
    );

    internal static readonly DiagnosticDescriptor SingletonHandlerRegistration = new(
        SingletonHandlerRegistrationDiagnosticId,
        "Register HostLoom handlers and behaviors as scoped",
        "HostLoom handler or behavior '{0}' is registered as a singleton; use a scoped registration",
        "Usage",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "HostLoom creates one dependency-injection scope per delivery and registers request handlers, event handlers, and behaviors as scoped services. Singleton registrations can share mutable state between deliveries or capture scoped dependencies.",
        helpLinkUri: "https://github.com/kidoz/hostloom/tree/main/src/HostLoom.Analyzers#hlm0003"
    );
}
