using System.Collections.Immutable;
using System.Reflection;
using HostLoom.Composition;
using HostLoom.Composition.Generators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Analyzers.Tests;

internal static class CompositionGeneratorHarness
{
    internal static readonly CSharpParseOptions ParseOptions = new(LanguageVersion.CSharp14);

    internal static CSharpCompilation Compilation(
        string source,
        string path = "Rules.cs",
        params MetadataReference[] extraReferences
    )
    {
        string platform =
            (string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") ?? string.Empty;
        IEnumerable<string> paths = platform
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            .Concat([
                typeof(CompositionPlan).Assembly.Location,
                typeof(IServiceCollection).Assembly.Location,
            ]);
        return CSharpCompilation.Create(
            "CompositionFixtures",
            [
                CSharpSyntaxTree.ParseText(
                    source,
                    ParseOptions,
                    path,
                    cancellationToken: TestContext.Current.CancellationToken
                ),
            ],
            paths
                .Distinct(StringComparer.Ordinal)
                .Select(static file => MetadataReference.CreateFromFile(file))
                .Concat(extraReferences),
            new CSharpCompilationOptions(
                OutputKind.DynamicallyLinkedLibrary,
                nullableContextOptions: NullableContextOptions.Enable
            )
        );
    }

    internal static (GeneratorDriver Driver, Microsoft.CodeAnalysis.Compilation Output) Run(
        CSharpCompilation compilation,
        GeneratorDriver? previous = null
    )
    {
        GeneratorDriver driver =
            previous
            ?? CSharpGeneratorDriver.Create(
                [new CompositionGenerator().AsSourceGenerator()],
                parseOptions: ParseOptions,
                driverOptions: new GeneratorDriverOptions(
                    IncrementalGeneratorOutputKind.None,
                    trackIncrementalGeneratorSteps: true
                )
            );
        driver = driver.RunGeneratorsAndUpdateCompilation(
            compilation,
            out Microsoft.CodeAnalysis.Compilation output,
            out _,
            TestContext.Current.CancellationToken
        );
        return (driver, output);
    }

    internal static void AssertSuccess(
        GeneratorDriver driver,
        Microsoft.CodeAnalysis.Compilation output
    )
    {
        Diagnostic[] errors = output
            .GetDiagnostics(TestContext.Current.CancellationToken)
            .Concat(driver.GetRunResult().Diagnostics)
            .Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .ToArray();
        Assert.True(
            errors.Length == 0,
            string.Join(Environment.NewLine, errors.Select(static error => error.ToString()))
        );
    }

    internal static CompositionPlan LoadPlan(
        Microsoft.CodeAnalysis.Compilation output,
        string type = "CatalogComposition",
        string method = "CreatePlan"
    )
    {
        using var stream = new MemoryStream();
        var emit = output.Emit(stream, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(emit.Success, string.Join(Environment.NewLine, emit.Diagnostics));
        Assembly assembly = Assembly.Load(stream.ToArray());
        return Assert.IsType<CompositionPlan>(
            assembly
                .GetType(type, throwOnError: true)!
                .GetMethod(
                    method,
                    BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static
                )!
                .Invoke(null, null)
        );
    }

    internal static string Source(GeneratorDriver driver) =>
        Assert
            .Single(Assert.Single(driver.GetRunResult().Results).GeneratedSources)
            .SourceText.ToString();
}
