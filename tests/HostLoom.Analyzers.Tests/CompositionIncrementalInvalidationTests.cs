using Microsoft.CodeAnalysis.CSharp;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed class CompositionIncrementalInvalidationTests
{
    [Theory]
    [InlineData("attribute")]
    [InlineData("inheritance")]
    [InlineData("attribute-usage")]
    [InlineData("accessibility")]
    [InlineData("rule")]
    public void Semantic_edits_invalidate_output_and_reverting_restores_it(string change)
    {
        const string rules = """
            using HostLoom.Composition;
            public static partial class CatalogComposition
            {
                [CompositionRules(nameof(CreatePlan))]
                private static void Declare(CompositionRuleBuilder rules)
                {
                    rules.AddClasses().AssignableTo<ICatalog>().WithAttribute<MarkerAttribute>()
                        .AsImplementedInterfaces().WithScopedLifetime().ExpectOne().AllowEmpty();
                    rules.AddClasses().AssignableTo<ICatalog>().WithAttribute<MarkerAttribute>()
                        .AsSelf().WithScopedLifetime().ExpectOne().AllowEmpty();
                }
                public static partial CompositionPlan CreatePlan();
            }
            """;
        const string types = """
            public interface ICatalog { }
            [System.AttributeUsage(System.AttributeTargets.Class, Inherited = true)]
            public class MarkerAttribute : System.Attribute { }
            [Marker] public abstract class CatalogBase : ICatalog { }
            public class Catalog : CatalogBase { public Catalog() { } }
            """;
        var typeTree = CSharpSyntaxTree.ParseText(
            types,
            CompositionGeneratorHarness.ParseOptions,
            "Types.cs",
            cancellationToken: TestContext.Current.CancellationToken
        );
        var original = CompositionGeneratorHarness.Compilation(rules).AddSyntaxTrees(typeTree);
        var (driver, output) = CompositionGeneratorHarness.Run(original);
        CompositionGeneratorHarness.AssertSuccess(driver, output);
        string initial = CompositionGeneratorHarness.Source(driver);
        var edited =
            change == "rule"
                ? original.ReplaceSyntaxTree(
                    original.SyntaxTrees.First(),
                    CSharpSyntaxTree.ParseText(
                        rules.Replace(
                            "WithScopedLifetime",
                            "WithTransientLifetime",
                            StringComparison.Ordinal
                        ),
                        CompositionGeneratorHarness.ParseOptions,
                        "Rules.cs",
                        cancellationToken: TestContext.Current.CancellationToken
                    )
                )
                : original.ReplaceSyntaxTree(
                    typeTree,
                    CSharpSyntaxTree.ParseText(
                        change switch
                        {
                            "attribute" => types.Replace(
                                "[Marker]",
                                "        ",
                                StringComparison.Ordinal
                            ),
                            "attribute-usage" => types.Replace(
                                "Inherited = true",
                                "Inherited = false",
                                StringComparison.Ordinal
                            ),
                            "inheritance" => types.Replace(
                                ": ICatalog",
                                "          ",
                                StringComparison.Ordinal
                            ),
                            "accessibility" => types.Replace(
                                "public class Catalog :",
                                "file class Catalog :",
                                StringComparison.Ordinal
                            ),
                            _ => throw new InvalidOperationException(),
                        },
                        CompositionGeneratorHarness.ParseOptions,
                        "Types.cs",
                        cancellationToken: TestContext.Current.CancellationToken
                    )
                );
        var (updated, changedOutput) = CompositionGeneratorHarness.Run(edited, driver);
        if (change == "accessibility")
        {
            Assert.Contains(
                updated.GetRunResult().Diagnostics,
                diagnostic => diagnostic.Id == "HLM0010"
            );
            Assert.Empty(Assert.Single(updated.GetRunResult().Results).GeneratedSources);
        }
        else
        {
            CompositionGeneratorHarness.AssertSuccess(updated, changedOutput);
            Assert.NotEqual(initial, CompositionGeneratorHarness.Source(updated));
        }
        var (reverted, revertedOutput) = CompositionGeneratorHarness.Run(original, updated);
        CompositionGeneratorHarness.AssertSuccess(reverted, revertedOutput);
        Assert.Equal(initial, CompositionGeneratorHarness.Source(reverted));
    }

    [Theory]
    [InlineData("constructor", "HLM0016")]
    [InlineData("constraint", "HLM0015")]
    public void Constructor_and_constraint_edits_are_reanalyzed_and_reversible(
        string change,
        string diagnosticId
    )
    {
        string registration =
            change == "constructor"
                ? """
                    rules.AddTypes(typeof(Catalog)).AsSelf().WithSingletonLifetime().ExpectOne();
                    rules.AddTypes(typeof(Session)).AsSelf().WithTransientLifetime().ExpectOne();
                    rules.AddTypes(typeof(Scope)).AsSelf().WithScopedLifetime().ExpectOne();
                    """
                : "rules.AddOpenGeneric(typeof(IRepository<>), typeof(Repository<>)).WithTransientLifetime().ExpectOne();";
        string types =
            change == "constructor"
                ? """
                    public class Catalog { public Catalog(Session session) {} }
                    public class Session { public Session() {} }
                    public class Scope { public Scope() {} }
                    """
                : """
                    public interface IRepository<T> where T : class {}
                    public class Repository<T> : IRepository<T> where T : class { public Repository() {} }
                    """;
        string editedTypes =
            change == "constructor"
                ? types.Replace("Session()", "Session(Scope scope)", StringComparison.Ordinal)
                : types.Replace(
                    "Repository<T> : IRepository<T> where T : class",
                    "Repository<T> : IRepository<T> where T : class, System.IDisposable",
                    StringComparison.Ordinal
                );
        string rules = $$"""
            using HostLoom.Composition;
            public static partial class CatalogComposition
            {
                [CompositionRules(nameof(CreatePlan))]
                private static void Declare(CompositionRuleBuilder rules) { {{registration}} }
                public static partial CompositionPlan CreatePlan();
            }
            """;
        var original = CompositionGeneratorHarness.Compilation(rules);
        var typeTree = CSharpSyntaxTree.ParseText(
            types,
            CompositionGeneratorHarness.ParseOptions,
            "Types.cs",
            cancellationToken: TestContext.Current.CancellationToken
        );
        original = original.AddSyntaxTrees(typeTree);
        var (driver, output) = CompositionGeneratorHarness.Run(original);
        CompositionGeneratorHarness.AssertSuccess(driver, output);
        string initial = CompositionGeneratorHarness.Source(driver);
        var edited = original.ReplaceSyntaxTree(
            typeTree,
            CSharpSyntaxTree.ParseText(
                editedTypes,
                CompositionGeneratorHarness.ParseOptions,
                "Types.cs",
                cancellationToken: TestContext.Current.CancellationToken
            )
        );
        var (updated, _) = CompositionGeneratorHarness.Run(edited, driver);
        Assert.Contains(updated.GetRunResult().Diagnostics, item => item.Id == diagnosticId);
        Assert.Empty(Assert.Single(updated.GetRunResult().Results).GeneratedSources);
        var (reverted, revertedOutput) = CompositionGeneratorHarness.Run(original, updated);
        CompositionGeneratorHarness.AssertSuccess(reverted, revertedOutput);
        Assert.Equal(initial, CompositionGeneratorHarness.Source(reverted));
    }
}
