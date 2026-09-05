using Microsoft.CodeAnalysis.CSharp;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed class CompositionIncrementalInvalidationTests
{
    [Theory]
    [InlineData("attribute")]
    [InlineData("inheritance")]
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
                }
                public static partial CompositionPlan CreatePlan();
            }
            """;
        const string types = """
            public interface ICatalog { }
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
}
