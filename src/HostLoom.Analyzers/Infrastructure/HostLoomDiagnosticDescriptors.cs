using Microsoft.CodeAnalysis;

namespace HostLoom.Analyzers.Infrastructure;

/// <summary>Diagnostic identifiers reported by HostLoom analyzers.</summary>
public static class HostLoomDiagnosticDescriptors
{
    public const string MissingCancellationTokenDiagnosticId = "HLM0001";

    public const string SyncOverAsyncDiagnosticId = "HLM0002";

    public const string SingletonHandlerRegistrationDiagnosticId = "HLM0003";

    public const string UnassignedDestinationMemberDiagnosticId = "HLM0004";

    public const string MappingNotVerifiableDiagnosticId = "HLM0005";

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

    internal static readonly DiagnosticDescriptor UnassignedDestinationMember = new(
        UnassignedDestinationMemberDiagnosticId,
        "Assign every settable member of a mapped destination",
        "Map to '{0}' never assigns {1}; assign each one or name it in UnmappedMembers",
        "Usage",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "An explicit map makes a forgotten destination member a silent data loss rather than a compile error, because nothing requires the member to be written. This reports members a map never assigns, so omission has to be deliberate and named.",
        helpLinkUri: "https://github.com/kidoz/hostloom/tree/main/src/HostLoom.Analyzers#hlm0004"
    );

    internal static readonly DiagnosticDescriptor MappingNotVerifiable = new(
        MappingNotVerifiableDiagnosticId,
        "Keep a map body in a shape completeness can be verified in",
        "Completeness of the map to '{0}' cannot be verified: {1}",
        "Usage",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Completeness analysis recognises a destination returned directly, and one local constructed and then assigned before being returned. A body outside both shapes is reported rather than skipped, so that 'not checked' is never mistaken for 'checked and complete'.",
        helpLinkUri: "https://github.com/kidoz/hostloom/tree/main/src/HostLoom.Analyzers#hlm0005"
    );
}
