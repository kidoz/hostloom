using Microsoft.CodeAnalysis;

namespace HostLoom.Composition.Generators;

internal static class CompositionDiagnostics
{
    internal static readonly DiagnosticDescriptor Declaration = Create(
        "HLM0009",
        "Invalid composition declaration"
    );
    internal static readonly DiagnosticDescriptor Selection = Create(
        "HLM0010",
        "Invalid composition selection"
    );
    internal static readonly DiagnosticDescriptor Policy = Create(
        "HLM0011",
        "Specify composition lifetime and cardinality"
    );
    internal static readonly DiagnosticDescriptor Projection = Create(
        "HLM0012",
        "Invalid composition service projection"
    );
    internal static readonly DiagnosticDescriptor Conflict = Create(
        "HLM0013",
        "Conflicting composition registrations"
    );

    internal static readonly DiagnosticDescriptor Count = Create(
        "HLM0014",
        "Composition candidate count mismatch"
    );
    internal static readonly DiagnosticDescriptor Generic = Create(
        "HLM0015",
        "Unsupported composition generic shape"
    );
    internal static readonly DiagnosticDescriptor Capture = Create(
        "HLM0016",
        "Proven singleton capture of scoped service"
    );

    private static DiagnosticDescriptor Create(string id, string title) =>
        new(
            id,
            title,
            "{0}",
            "Composition",
            DiagnosticSeverity.Error,
            isEnabledByDefault: true,
            helpLinkUri: "https://github.com/kidoz/hostloom/tree/main/src/HostLoom.Composition.Generators#diagnostics"
        );
}
