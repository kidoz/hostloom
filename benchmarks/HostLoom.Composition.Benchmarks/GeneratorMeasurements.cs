using System.Text;
using HostLoom.Composition.Generators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Composition.Benchmarks;

internal static class GeneratorMeasurements
{
    internal static void Run(int count, string? exportDirectory = null)
    {
        var parse = new CSharpParseOptions(LanguageVersion.CSharp14);
        var types = new StringBuilder(
            "public interface ICatalog {} public abstract class CatalogBase : ICatalog {}\n"
        );
        for (var i = 0; i < count; i++)
            types
                .Append("public class Catalog")
                .Append(i)
                .Append(" : CatalogBase { public Catalog")
                .Append(i)
                .Append("() {} }\n");
        const string rules = """
            using HostLoom.Composition;
            public static partial class CatalogComposition
            {
                [CompositionRules(nameof(CreatePlan))]
                private static void Declare(CompositionRuleBuilder rules)
                {
                    rules.AddClasses().AssignableTo<ICatalog>().AsImplementedInterfaces().WithTransientLifetime().ExpectMany();
                }
                public static partial CompositionPlan CreatePlan();
            }
            """;
        var typeTree = CSharpSyntaxTree.ParseText(types.ToString(), parse, "Types.cs");
        var ruleTree = CSharpSyntaxTree.ParseText(rules, parse, "Rules.cs");
        var bodyTree = CSharpSyntaxTree.ParseText(
            "public class Unrelated { public int Read() => 1; }",
            parse,
            "Unrelated.cs"
        );
        var bodyEdit = CSharpSyntaxTree.ParseText(
            "public class Unrelated { public int Read() => 2; }",
            parse,
            "Unrelated.cs"
        );
        var ruleEdit = CSharpSyntaxTree.ParseText(
            rules.Replace("WithTransientLifetime", "WithScopedLifetime", StringComparison.Ordinal),
            parse,
            "Rules.cs"
        );
        var paths = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator)
            .Concat([
                typeof(CompositionPlan).Assembly.Location,
                typeof(IServiceCollection).Assembly.Location,
            ])
            .Distinct(StringComparer.Ordinal);
        var references = paths
            .Select(static path => MetadataReference.CreateFromFile(path))
            .ToArray();
        CSharpCompilation Compilation() =>
            CSharpCompilation.Create(
                "CompositionBenchmark",
                [typeTree, ruleTree, bodyTree],
                references,
                new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
            );
        GeneratorDriver Driver() =>
            CSharpGeneratorDriver.Create(
                [new CompositionGenerator().AsSourceGenerator()],
                parseOptions: parse,
                driverOptions: new GeneratorDriverOptions(
                    IncrementalGeneratorOutputKind.None,
                    trackIncrementalGeneratorSteps: true
                )
            );
        if (exportDirectory is not null)
        {
            var fixture = Compilation();
            var exported = Driver()
                .RunGeneratorsAndUpdateCompilation(fixture, out var compiled, out _);
            Validate(exported);
            var errors = compiled
                .GetDiagnostics()
                .Where(static error => error.Severity == DiagnosticSeverity.Error)
                .ToArray();
            if (errors.Length != 0)
                throw new InvalidOperationException(
                    string.Join("; ", errors.Select(static error => error.ToString()))
                );
            foreach (string mode in new[] { "generated", "handwritten" })
            {
                string directory = Path.Combine(exportDirectory, mode);
                Directory.CreateDirectory(directory);
                File.WriteAllText(Path.Combine(directory, "Types.cs"), types.ToString());
                File.WriteAllText(Path.Combine(directory, "Rules.cs"), rules);
                if (mode == "handwritten")
                    File.WriteAllText(Path.Combine(directory, "Factory.g.cs"), Source(exported));
                string runtime = System.Security.SecurityElement.Escape(
                    typeof(CompositionPlan).Assembly.Location
                );
                string di = System.Security.SecurityElement.Escape(
                    typeof(IServiceCollection).Assembly.Location
                );
                string generator = System.Security.SecurityElement.Escape(
                    typeof(CompositionGenerator).Assembly.Location
                );
                File.WriteAllText(
                    Path.Combine(directory, "Consumer.csproj"),
                    $"""
                    <Project Sdk="Microsoft.NET.Sdk"><PropertyGroup>
                    <TargetFramework>net10.0</TargetFramework><LangVersion>14.0</LangVersion><Nullable>enable</Nullable>
                    <AssemblyName>CompositionBenchmark</AssemblyName><TreatWarningsAsErrors>true</TreatWarningsAsErrors>
                    </PropertyGroup><ItemGroup>
                    <Reference Include="HostLoom.Composition"><HintPath>{runtime}</HintPath></Reference>
                    <Reference Include="Microsoft.Extensions.DependencyInjection.Abstractions"><HintPath>{di}</HintPath></Reference>
                    {(mode == "generated" ? $"<Analyzer Include=\"{generator}\" />" : "")}
                    </ItemGroup></Project>
                    """
                );
            }
            return;
        }
        var compilation = Compilation();
        GeneratorDriver initial = Driver();
        Measurement first = Program.Measure(() => initial = initial.RunGenerators(compilation), 1);
        Validate(initial);
        string baseline = Source(initial);
        var cases = new Dictionary<string, List<Measurement>>(StringComparer.Ordinal)
        {
            ["fresh-driver"] = [],
            ["unchanged"] = [],
            ["unrelated-edit"] = [],
            ["rule-edit"] = [],
        };
        var reasons = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < 15; i++)
        {
            var freshCompilation = Compilation();
            GeneratorDriver fresh = Driver();
            cases["fresh-driver"]
                .Add(Program.Measure(() => fresh = fresh.RunGenerators(freshCompilation), 1));
            Validate(fresh);
            cases["unchanged"]
                .Add(Program.Measure(() => fresh = fresh.RunGenerators(freshCompilation), 1));
            var edited = freshCompilation.ReplaceSyntaxTree(bodyTree, bodyEdit);
            cases["unrelated-edit"]
                .Add(Program.Measure(() => fresh = fresh.RunGenerators(edited), 1));
            Validate(fresh);
            if (Source(fresh) != baseline)
                throw new InvalidOperationException(
                    "Unrelated edit changed emitted registrations/provenance."
                );
            foreach (
                var step in fresh.GetRunResult().Results.Single().TrackedSteps["CompositionSource"]
            )
            foreach (var output in step.Outputs)
            {
                reasons.Add(output.Reason.ToString());
                if (
                    output.Reason
                    is not (IncrementalStepRunReason.Cached or IncrementalStepRunReason.Unchanged)
                )
                    throw new InvalidOperationException(
                        "Unrelated edit did not reuse source output."
                    );
            }
            edited = edited.ReplaceSyntaxTree(ruleTree, ruleEdit);
            cases["rule-edit"].Add(Program.Measure(() => fresh = fresh.RunGenerators(edited), 1));
            Validate(fresh);
            if (Source(fresh) == baseline)
                throw new InvalidOperationException("Rule edit failed to invalidate output.");
        }
        Program.Write(
            new
            {
                count,
                environment = Program.EnvironmentData(),
                first,
                cases,
                trackedReasons = reasons.Order(StringComparer.Ordinal).ToArray(),
            }
        );
    }

    private static void Validate(GeneratorDriver driver)
    {
        var errors = driver
            .GetRunResult()
            .Diagnostics.Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .ToArray();
        if (errors.Length != 0)
            throw new InvalidOperationException(
                string.Join("; ", errors.Select(static error => error.ToString()))
            );
    }

    private static string Source(GeneratorDriver driver) =>
        driver.GetRunResult().Results.Single().GeneratedSources.Single().SourceText.ToString();
}
