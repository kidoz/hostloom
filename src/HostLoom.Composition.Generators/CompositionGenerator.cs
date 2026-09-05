using System.Collections.Immutable;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace HostLoom.Composition.Generators;

/// <summary>Generates explicit plan factories from marked, restricted declaration methods.</summary>
[Generator(LanguageNames.CSharp)]
public sealed class CompositionGenerator : IIncrementalGenerator
{
    internal const string AttributeName = "HostLoom.Composition.CompositionRulesAttribute";
    internal const string BuilderName = "HostLoom.Composition.CompositionRuleBuilder";
    internal const string TypeBuilderName = "HostLoom.Composition.CompositionTypeRuleBuilder";
    internal const string PlanName = "HostLoom.Composition.CompositionPlan";

    /// <inheritdoc />
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var results = context.SyntaxProvider.ForAttributeWithMetadataName(
            AttributeName,
            static (node, _) => node is MethodDeclarationSyntax,
            static (syntax, cancellation) => new DeclarationCompiler(syntax, cancellation).Compile()
        );
        context.RegisterSourceOutput(
            results,
            static (output, result) =>
            {
                foreach (Diagnostic diagnostic in result.Diagnostics)
                {
                    output.ReportDiagnostic(diagnostic);
                }
            }
        );
        // Only value-equatable emitted text reaches source output. Semantic matching may rerun
        // after edits; unchanged registrations and provenance do not force source re-emission.
        var files = results
            .Select(static (result, _) => result.File)
            .Where(static file => file is not null);
        context.RegisterSourceOutput(
            files.WithTrackingName("CompositionSource"),
            static (output, file) =>
                output.AddSource(file!.HintName, SourceText.From(file.Source, Encoding.UTF8))
        );
    }
}

internal sealed class GenerationResult(GeneratedFile? file, ImmutableArray<Diagnostic> diagnostics)
{
    internal GeneratedFile? File { get; } = file;
    internal ImmutableArray<Diagnostic> Diagnostics { get; } = diagnostics;
}

internal sealed class GeneratedFile(string hintName, string source) : IEquatable<GeneratedFile>
{
    internal string HintName { get; } = hintName;
    internal string Source { get; } = source;

    public bool Equals(GeneratedFile? other) =>
        other is not null
        && string.Equals(HintName, other.HintName, StringComparison.Ordinal)
        && string.Equals(Source, other.Source, StringComparison.Ordinal);

    public override bool Equals(object? obj) => obj is GeneratedFile other && Equals(other);

    public override int GetHashCode() =>
        StringComparer.Ordinal.GetHashCode(HintName) ^ StringComparer.Ordinal.GetHashCode(Source);
}
