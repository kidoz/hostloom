using System.Collections.Immutable;
using HostLoom.Pipelines;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Analyzers.Tests;

internal static class AnalyzerTestHarness
{
    public static async Task<Diagnostic[]> AnalyzeAsync(
        string source,
        params DiagnosticAnalyzer[] analyzers
    )
    {
        CSharpCompilation compilation = CSharpCompilation.Create(
            "HostLoom.Analyzers.Tests.Target",
            [
                CSharpSyntaxTree.ParseText(
                    source,
                    CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview)
                ),
            ],
            CreateReferences(),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
        );

        Diagnostic[] compilerErrors = compilation
            .GetDiagnostics()
            .Where(diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .ToArray();
        Assert.True(
            compilerErrors.Length == 0,
            string.Join(
                Environment.NewLine,
                compilerErrors.Select(diagnostic => diagnostic.ToString())
            )
        );

        CompilationWithAnalyzers compilationWithAnalyzers = compilation.WithAnalyzers([
            .. analyzers,
        ]);
        ImmutableArray<Diagnostic> diagnostics = await compilationWithAnalyzers
            .GetAnalyzerDiagnosticsAsync()
            .ConfigureAwait(true);
        return diagnostics.OrderBy(diagnostic => diagnostic.Id, StringComparer.Ordinal).ToArray();
    }

    private static MetadataReference[] CreateReferences()
    {
        string trustedPlatformAssemblies =
            (string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") ?? string.Empty;
        string[] paths =
        [
            .. trustedPlatformAssemblies.Split(
                Path.PathSeparator,
                StringSplitOptions.RemoveEmptyEntries
            ),
            typeof(HostLoomBuilder).Assembly.Location,
            typeof(PipeContext).Assembly.Location,
            typeof(IServiceCollection).Assembly.Location,
            typeof(ServiceCollectionServiceExtensions).Assembly.Location,
        ];

        return paths
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(path => MetadataReference.CreateFromFile(path))
            .ToArray();
    }
}
