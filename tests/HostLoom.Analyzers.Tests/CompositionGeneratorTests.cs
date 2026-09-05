using System.Collections.Immutable;
using HostLoom.Composition;
using HostLoom.Composition.Generators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed class CompositionGeneratorTests
{
    private const string Types = """
        public interface ICatalog { }
        public interface IInventory { }
        public class Catalog : ICatalog { public Catalog() { } }
        public class Inventory : ICatalog, IInventory { public Inventory() { } }
        """;

    private static string Fixture(string rules, string types = Types) =>
        $$"""
            using HostLoom.Composition;
            using Microsoft.Extensions.DependencyInjection;
            public static partial class CatalogComposition
            {
                [CompositionRules(nameof(CreatePlan))]
                private static void Declare(CompositionRuleBuilder rules)
                {
                    {{rules}}
                }
                public static partial CompositionPlan CreatePlan();
            }
            {{types}}
            """;

    [Theory]
    [InlineData(
        "Explicit",
        "rules.AddTypes(typeof(Catalog)).As<ICatalog>().WithScopedLifetime().ExpectOne();"
    )]
    [InlineData(
        "Self",
        "rules.AddTypes(typeof(Inventory)).AsSelf().WithSingletonLifetime().ExpectOne();"
    )]
    [InlineData(
        "Group",
        "rules.Group(\"catalog\", group => group.AddTypes(typeof(Inventory), typeof(Catalog)).As<ICatalog>().WithLifetime(ServiceLifetime.Transient).ExpectMany());"
    )]
    [InlineData(
        "Empty",
        "rules.AddTypes().AsSelf().WithSingletonLifetime().ExpectOne().AllowEmpty();"
    )]
    [InlineData(
        "Any",
        "rules.AddClasses().AssignableToAny(typeof(ICatalog), typeof(IInventory)).As(typeof(ICatalog)).WithTransientLifetime().ExpectMany();"
    )]
    public void Supported_declarations_match_reviewable_snapshots_and_execute(
        string snapshot,
        string rules
    )
    {
        var (driver, output) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(Fixture(rules))
        );
        CompositionGeneratorHarness.AssertSuccess(driver, output);
        string generated = CompositionGeneratorHarness.Source(driver);
        string expected = Path.Combine(
            AppContext.BaseDirectory,
            "Snapshots",
            "Composition",
            snapshot + ".verified.txt"
        );
        string received = Path.Combine(
            Path.GetTempPath(),
            "HostLoom.Composition." + snapshot + ".received.txt"
        );
        File.WriteAllText(received, generated);
        Assert.True(File.Exists(expected), "Missing reviewed snapshot; inspect " + received);
        Assert.Equal(
            File.ReadAllText(expected).Replace("\r\n", "\n", StringComparison.Ordinal),
            generated
        );
        CompositionPlan plan = CompositionGeneratorHarness.LoadPlan(output);
        var services = new ServiceCollection();
        plan.ApplyTo(services);
        using ServiceProvider provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );
        using IServiceScope scope = provider.CreateScope();
        foreach (CompositionRegistration registration in plan.Probe().Registrations)
            Assert.NotNull(
                scope.ServiceProvider.GetRequiredService(registration.Descriptor.ServiceType)
            );
        Assert.Throws<CompositionValidationException>(() => plan.ApplyTo(services));
    }

    [Fact]
    public void Inherited_generic_interfaces_are_closed_and_incidental_interfaces_are_excluded()
    {
        string source = Fixture(
            "rules.AddClasses().AssignableTo(typeof(IHandler<>)).AsImplementedInterfaces().WithTransientLifetime().ExpectOne();",
            """
            public interface IHandler<T> { }
            public sealed class CatalogItem { }
            public sealed class InventoryItem { }
            public abstract class HandlerBase<T> : IHandler<T> { }
            internal sealed class ZetaHandler : HandlerBase<InventoryItem>, System.IDisposable
            {
                public ZetaHandler() { }
                public void Dispose() { }
            }
            internal sealed class AlphaHandler : HandlerBase<CatalogItem> { public AlphaHandler() { } }
            """
        );
        var (driver, output) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(source)
        );
        CompositionGeneratorHarness.AssertSuccess(driver, output);
        string generated = CompositionGeneratorHarness.Source(driver);
        File.WriteAllText(
            Path.Combine(Path.GetTempPath(), "HostLoom.Composition.Inherited.received.txt"),
            generated
        );
        string expected = Path.Combine(
            AppContext.BaseDirectory,
            "Snapshots",
            "Composition",
            "Inherited.verified.txt"
        );
        Assert.True(
            File.Exists(expected),
            "Inspect /tmp/HostLoom.Composition.Inherited.received.txt"
        );
        Assert.Equal(File.ReadAllText(expected), generated);
        CompositionPlan plan = CompositionGeneratorHarness.LoadPlan(output);
        Assert.Collection(
            plan.Probe().Registrations,
            entry => Assert.Equal("AlphaHandler", entry.Descriptor.ImplementationType!.Name),
            entry => Assert.Equal("ZetaHandler", entry.Descriptor.ImplementationType!.Name)
        );
        Assert.All(
            plan.Probe().Registrations,
            entry =>
                Assert.Equal(
                    "IHandler`1",
                    entry.Descriptor.ServiceType.GetGenericTypeDefinition().Name
                )
        );
        var services = new ServiceCollection();
        plan.ApplyTo(services);
        using ServiceProvider provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true }
        );
        Assert.All(
            plan.Probe().Registrations,
            entry =>
                Assert.Equal(
                    entry.Descriptor.ImplementationType,
                    provider.GetRequiredService(entry.Descriptor.ServiceType).GetType()
                )
        );
    }

    [Theory]
    [InlineData(
        "rules.AddClasses().AssignableTo<IHandler<CatalogItem>>().AsImplementedInterfaces().WithScopedLifetime().ExpectOne();",
        1
    )]
    [InlineData(
        "rules.AddTypes(typeof(Handler)).As(typeof(IHandler<>)).WithScopedLifetime().ExpectMany();",
        2
    )]
    public void Closed_matching_and_open_interface_projection_preserve_actual_generic_arguments(
        string rule,
        int count
    )
    {
        string source = Fixture(
            rule,
            """
            public interface IHandler<T> { }
            public class CatalogItem { }
            public class InventoryItem { }
            public class Handler : IHandler<CatalogItem>, IHandler<InventoryItem> { public Handler() { } }
            """
        );
        var (driver, output) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(source)
        );
        CompositionGeneratorHarness.AssertSuccess(driver, output);
        Assert.Equal(
            count,
            CompositionGeneratorHarness.LoadPlan(output).Probe().Registrations.Count
        );
    }

    [Theory]
    [InlineData(
        "var candidate = typeof(Catalog); rules.AddTypes(candidate).AsSelf().WithTransientLifetime().ExpectOne();",
        "HLM0009"
    )]
    [InlineData(
        "if (true) rules.AddTypes(typeof(Catalog)).AsSelf().WithTransientLifetime().ExpectOne();",
        "HLM0009"
    )]
    [InlineData(
        "rules.AddTypes(new[] { typeof(Catalog) }).AsSelf().WithTransientLifetime().ExpectOne();",
        "HLM0009"
    )]
    [InlineData("rules.AddTypes(typeof(Catalog)).AsSelf().ExpectOne();", "HLM0011")]
    [InlineData("rules.AddTypes(typeof(Catalog)).AsSelf().WithTransientLifetime();", "HLM0011")]
    [InlineData(
        "rules.AddTypes(typeof(Catalog)).AsSelf().WithLifetime((ServiceLifetime)99).ExpectOne();",
        "HLM0011"
    )]
    [InlineData(
        "rules.AddTypes(typeof(Catalog)).AsSelf().WithTransientLifetime().WithScopedLifetime().ExpectOne();",
        "HLM0011"
    )]
    [InlineData(
        "rules.AddTypes(typeof(Catalog)).AsSelf().WithTransientLifetime().ExpectOne().ExpectMany();",
        "HLM0011"
    )]
    [InlineData("rules.AddClasses().AsSelf().WithTransientLifetime().ExpectMany();", "HLM0010")]
    [InlineData("rules.AddTypes().AsSelf().WithTransientLifetime().ExpectOne();", "HLM0010")]
    [InlineData(
        "rules.AddTypes(typeof(Catalog)).As<IInventory>().WithTransientLifetime().ExpectOne();",
        "HLM0012"
    )]
    [InlineData(
        "rules.AddTypes(typeof(Catalog)).AsImplementedInterfaces().WithTransientLifetime().ExpectOne();",
        "HLM0012"
    )]
    [InlineData(
        "rules.AddTypes(typeof(Catalog), typeof(Inventory)).As<ICatalog>().WithTransientLifetime().ExpectOne();",
        "HLM0013"
    )]
    [InlineData(
        "rules.AddTypes(typeof(Catalog)).As<ICatalog>().WithTransientLifetime().ExpectMany(); rules.AddTypes(typeof(Catalog)).As<ICatalog>().WithTransientLifetime().ExpectMany();",
        "HLM0013"
    )]
    [InlineData(
        "rules.Group(\"catalog\", group => group.AddTypes(typeof(Catalog)).AsSelf().WithScopedLifetime().ExpectOne()); rules.Group(\"catalog\", group => group.AddTypes(typeof(Inventory)).AsSelf().WithScopedLifetime().ExpectOne());",
        "HLM0009"
    )]
    [InlineData(
        "rules.Group(\"outer\", group => { group.Group(\"inner\", nested => nested.AddTypes(typeof(Catalog)).AsSelf().WithScopedLifetime().ExpectOne()); });",
        "HLM0009"
    )]
    public void Unsupported_or_invalid_rules_fail_at_authored_locations_without_a_factory(
        string rule,
        string id
    )
    {
        var (driver, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(Fixture(rule))
        );
        Diagnostic[] diagnostics = driver.GetRunResult().Diagnostics.ToArray();
        Assert.Contains(
            diagnostics,
            diagnostic => diagnostic.Id == id && diagnostic.Severity == DiagnosticSeverity.Error
        );
        Assert.All(
            diagnostics,
            diagnostic => Assert.Equal("Rules.cs", diagnostic.Location.SourceTree!.FilePath)
        );
        Assert.Empty(Assert.Single(driver.GetRunResult().Results).GeneratedSources);
    }

    [Theory]
    [InlineData("public class Catalog : ICatalog { private Catalog() { } }", "HLM0012")]
    [InlineData("file class Catalog : ICatalog { public Catalog() { } }", "HLM0010")]
    [InlineData(
        "public static class Container { private class Catalog : ICatalog { public Catalog() { } } }",
        "HLM0010"
    )]
    public void Selected_constructor_and_accessibility_failures_are_diagnosed(
        string implementation,
        string id
    )
    {
        string source = Fixture(
            "rules.AddClasses().AssignableTo<ICatalog>().AsImplementedInterfaces().WithTransientLifetime().ExpectOne();",
            "public interface ICatalog { } " + implementation
        );
        var (driver, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(source)
        );
        Assert.Contains(driver.GetRunResult().Diagnostics, diagnostic => diagnostic.Id == id);
        Assert.Empty(Assert.Single(driver.GetRunResult().Results).GeneratedSources);
    }

    [Theory]
    [InlineData(
        "public static partial class CatalogComposition",
        "public static class CatalogComposition"
    )]
    [InlineData("private static void Declare", "private void Declare")]
    [InlineData(
        "public static partial CompositionPlan CreatePlan();",
        "public static CompositionPlan CreatePlan() => null!;"
    )]
    [InlineData(
        "public static partial CompositionPlan CreatePlan();",
        "public static partial string CreatePlan();"
    )]
    [InlineData("private static void Declare", "private static void Declare<T>")]
    public void Invalid_declaration_or_factory_shape_has_a_generator_diagnostic(
        string original,
        string replacement
    )
    {
        string source = Fixture(
                "rules.AddTypes(typeof(Catalog)).AsSelf().WithTransientLifetime().ExpectOne();"
            )
            .Replace(original, replacement, StringComparison.Ordinal);
        var (driver, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(source)
        );
        Assert.Contains(
            driver.GetRunResult().Diagnostics,
            diagnostic => diagnostic.Id == "HLM0009"
        );
        Assert.Empty(Assert.Single(driver.GetRunResult().Results).GeneratedSources);
    }

    [Fact]
    public void Conflict_diagnostics_include_both_rule_locations()
    {
        string source = Fixture(
            """
            rules.AddTypes(typeof(Catalog)).As<ICatalog>().WithTransientLifetime().ExpectMany();
            rules.AddTypes(typeof(Inventory)).As<ICatalog>().WithScopedLifetime().ExpectMany();
            """
        );
        var (driver, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(source)
        );
        Diagnostic error = Assert.Single(
            driver.GetRunResult().Diagnostics,
            diagnostic => diagnostic.Id == "HLM0013"
        );
        Assert.Single(error.AdditionalLocations);
        Assert.NotEqual(
            error.Location.SourceSpan.Start,
            error.AdditionalLocations[0].SourceSpan.Start
        );
    }

    [Fact]
    public void Discovery_does_not_enumerate_references_but_explicit_referenced_types_are_supported()
    {
        CSharpCompilation library = CompositionGeneratorHarness
            .Compilation(
                "public interface IExternalCatalog { } public sealed class ExternalCatalog : IExternalCatalog { public ExternalCatalog() { } }"
            )
            .WithAssemblyName("ExternalCatalogTypes");
        using var stream = new MemoryStream();
        Assert.True(
            library.Emit(stream, cancellationToken: TestContext.Current.CancellationToken).Success
        );
        MetadataReference reference = MetadataReference.CreateFromImage(stream.ToArray());
        string discovery = Fixture(
            "rules.AddClasses().AssignableTo<IExternalCatalog>().AsImplementedInterfaces().WithTransientLifetime().ExpectOne();",
            ""
        );
        var (missing, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(discovery, "Rules.cs", reference)
        );
        Assert.Contains(
            missing.GetRunResult().Diagnostics,
            diagnostic => diagnostic.Id == "HLM0010"
        );
        string explicitTypes = Fixture(
            "rules.AddTypes(typeof(ExternalCatalog)).As<IExternalCatalog>().WithTransientLifetime().ExpectOne();",
            ""
        );
        var (driver, output) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(explicitTypes, "Rules.cs", reference)
        );
        CompositionGeneratorHarness.AssertSuccess(driver, output);
        Assert.Contains(
            "typeof(global::ExternalCatalog)",
            CompositionGeneratorHarness.Source(driver),
            StringComparison.Ordinal
        );
    }

    [Fact]
    public void Multiple_factories_and_nested_partial_containers_are_supported()
    {
        string source = """
            using HostLoom.Composition;
            namespace CatalogApp;
            public partial class Container
            {
                internal static partial class Plans
                {
                    [CompositionRules(nameof(First))]
                    private static void FirstRules(CompositionRuleBuilder rules) { rules.AddTypes(typeof(Catalog)).AsSelf().WithTransientLifetime().ExpectOne(); }
                    [CompositionRules(nameof(Second))]
                    private static void SecondRules(CompositionRuleBuilder rules) { rules.AddTypes(typeof(Catalog)).AsSelf().WithScopedLifetime().ExpectOne(); }
                    public static partial CompositionPlan First();
                    public static partial CompositionPlan Second();
                }
            }
            public class Catalog { public Catalog() { } }
            """;
        var (driver, output) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(source)
        );
        CompositionGeneratorHarness.AssertSuccess(driver, output);
        Assert.Equal(2, Assert.Single(driver.GetRunResult().Results).GeneratedSources.Length);
        Assert.NotEqual(
            CompositionGeneratorHarness
                .LoadPlan(output, "CatalogApp.Container+Plans", "First")
                .Identity,
            CompositionGeneratorHarness
                .LoadPlan(output, "CatalogApp.Container+Plans", "Second")
                .Identity
        );
    }

    [Fact]
    public void Duplicate_factory_claims_do_not_emit_duplicate_source()
    {
        string source = Fixture(
                "rules.AddTypes(typeof(Catalog)).AsSelf().WithTransientLifetime().ExpectOne();"
            )
            .Replace(
                "public static partial CompositionPlan CreatePlan();",
                "[CompositionRules(nameof(CreatePlan))] private static void Other(CompositionRuleBuilder rules) { } public static partial CompositionPlan CreatePlan();",
                StringComparison.Ordinal
            );
        var (driver, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(source)
        );
        Assert.Equal(
            2,
            driver.GetRunResult().Diagnostics.Count(diagnostic => diagnostic.Id == "HLM0009")
        );
        Assert.Empty(Assert.Single(driver.GetRunResult().Results).GeneratedSources);
    }

    [Theory]
    [InlineData("Declare(null!);", true)]
    [InlineData("System.Action<CompositionRuleBuilder> action = Declare;", true)]
    [InlineData("string name = nameof(Declare);", false)]
    [InlineData("CreatePlan();", false)]
    public async Task Declaration_usage_analyzer_rejects_execution_and_capture_but_allows_nameof_and_factories(
        string usage,
        bool expected
    )
    {
        string source = Fixture(
                "rules.AddTypes(typeof(Catalog)).AsSelf().WithTransientLifetime().ExpectOne();"
            )
            .Replace(
                "public static partial CompositionPlan CreatePlan();",
                "public static void Use() { "
                    + usage
                    + " } public static partial CompositionPlan CreatePlan();",
                StringComparison.Ordinal
            );
        var (driver, output) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(source)
        );
        CompositionGeneratorHarness.AssertSuccess(driver, output);
        ImmutableArray<Diagnostic> diagnostics = await output
            .WithAnalyzers([new CompositionDeclarationUsageAnalyzer()])
            .GetAnalyzerDiagnosticsAsync(TestContext.Current.CancellationToken);
        Assert.Equal(expected ? 1 : 0, diagnostics.Count(diagnostic => diagnostic.Id == "HLM0009"));
    }

    [Fact]
    public void Unrelated_body_edits_preserve_generated_text_and_reuse_source_output()
    {
        string source =
            Fixture(
                "rules.AddClasses().AssignableTo<ICatalog>().AsImplementedInterfaces().WithTransientLifetime().ExpectMany();"
            ) + "\npublic static class Utilities { public static int Value() => 1; }";
        CSharpCompilation compilation = CompositionGeneratorHarness.Compilation(source);
        var (first, _) = CompositionGeneratorHarness.Run(compilation);
        CSharpCompilation changed = compilation.ReplaceSyntaxTree(
            compilation.SyntaxTrees.Single(),
            CSharpSyntaxTree.ParseText(
                source.Replace("Value() => 1", "Value() => 2", StringComparison.Ordinal),
                CompositionGeneratorHarness.ParseOptions,
                "Rules.cs",
                cancellationToken: TestContext.Current.CancellationToken
            )
        );
        var (second, output) = CompositionGeneratorHarness.Run(changed, first);
        CompositionGeneratorHarness.AssertSuccess(second, output);
        Assert.Equal(
            CompositionGeneratorHarness.Source(first),
            CompositionGeneratorHarness.Source(second)
        );
        var step = Assert.Single(
            Assert.Single(second.GetRunResult().Results).TrackedSteps["CompositionSource"]
        );
        Assert.All(
            step.Outputs,
            result =>
                Assert.True(
                    result.Reason
                        is IncrementalStepRunReason.Cached
                            or IncrementalStepRunReason.Unchanged
                )
        );
    }

    [Fact]
    public void An_interface_change_in_another_file_invalidates_inherited_discovery()
    {
        string rules = Fixture(
            "rules.AddClasses().AssignableTo<IHandler<CatalogItem>>().AsImplementedInterfaces().WithScopedLifetime().ExpectOne();",
            ""
        );
        const string candidates =
            "public interface IHandler<T> { } public class CatalogItem { } public class InventoryItem { } public abstract class BaseHandler<T> : IHandler<T> { } public class Handler : BaseHandler<CatalogItem> { public Handler() { } }";
        SyntaxTree candidateTree = CSharpSyntaxTree.ParseText(
            candidates,
            CompositionGeneratorHarness.ParseOptions,
            "Candidates.cs",
            cancellationToken: TestContext.Current.CancellationToken
        );
        CSharpCompilation original = CompositionGeneratorHarness
            .Compilation(rules)
            .AddSyntaxTrees(candidateTree);
        var (first, output) = CompositionGeneratorHarness.Run(original);
        CompositionGeneratorHarness.AssertSuccess(first, output);
        SyntaxTree changed = CSharpSyntaxTree.ParseText(
            candidates.Replace(
                "Handler : BaseHandler<CatalogItem>",
                "Handler : BaseHandler<InventoryItem>",
                StringComparison.Ordinal
            ),
            CompositionGeneratorHarness.ParseOptions,
            "Candidates.cs",
            cancellationToken: TestContext.Current.CancellationToken
        );
        var (second, _) = CompositionGeneratorHarness.Run(
            original.ReplaceSyntaxTree(candidateTree, changed),
            first
        );
        Assert.Contains(
            second.GetRunResult().Diagnostics,
            diagnostic => diagnostic.Id == "HLM0010"
        );
        Assert.Empty(Assert.Single(second.GetRunResult().Results).GeneratedSources);
    }

    [Fact]
    public void Every_explicit_open_interface_projection_must_have_a_closed_match()
    {
        string source = Fixture(
            "rules.AddTypes(typeof(Catalog)).As(typeof(ICatalog), typeof(IHandler<>)).WithTransientLifetime().ExpectOne();",
            Types + " public interface IHandler<T> { }"
        );
        var (driver, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(source)
        );
        Assert.Contains(
            driver.GetRunResult().Diagnostics,
            diagnostic => diagnostic.Id == "HLM0012"
        );
        Assert.Empty(Assert.Single(driver.GetRunResult().Results).GeneratedSources);
    }

    [Fact]
    public void Runtime_lifetime_values_and_helper_calls_are_not_evaluated()
    {
        string source = Fixture(
                "rules.AddTypes(typeof(Catalog)).AsSelf().WithLifetime(CurrentLifetime()).ExpectOne();"
            )
            .Replace(
                "public static partial CompositionPlan CreatePlan();",
                "private static ServiceLifetime CurrentLifetime() => throw new System.Exception(); public static partial CompositionPlan CreatePlan();",
                StringComparison.Ordinal
            );
        var (driver, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(source)
        );
        Assert.Contains(
            driver.GetRunResult().Diagnostics,
            diagnostic => diagnostic.Id == "HLM0011"
        );
        Assert.DoesNotContain(
            driver.GetRunResult().Diagnostics,
            diagnostic => diagnostic.Id == "CS8785"
        );
    }

    [Fact]
    public void Keyword_identifiers_and_group_constants_are_emitted_as_valid_CSharp()
    {
        string source = """
            using HostLoom.Composition;
            namespace @event;
            public static partial class @class
            {
                private const string GroupName = "catalog";
                [CompositionRules(nameof(@new))]
                private static void Declare(CompositionRuleBuilder rules)
                {
                    rules.Group(GroupName, group => group.AddTypes(typeof(@struct)).AsSelf().WithTransientLifetime().ExpectOne());
                }
                public static partial CompositionPlan @new();
            }
            public class @struct { public @struct() { } }
            """;
        var (driver, output) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(source)
        );
        CompositionGeneratorHarness.AssertSuccess(driver, output);
        Assert.Equal(
            "catalog",
            Assert
                .Single(
                    CompositionGeneratorHarness
                        .LoadPlan(output, "event.class", "new")
                        .Probe()
                        .Registrations
                )
                .Origin.Group
        );
    }

    [Fact]
    public void Absolute_checkout_paths_do_not_enter_generated_source()
    {
        string source = Fixture(
            "rules.AddTypes(typeof(Catalog)).AsSelf().WithTransientLifetime().ExpectOne();"
        );
        var (first, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(source, "/first/checkout/Rules.cs")
        );
        var (second, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(source, "/second/checkout/Rules.cs")
        );
        Assert.Equal(
            CompositionGeneratorHarness.Source(first),
            CompositionGeneratorHarness.Source(second)
        );
        Assert.DoesNotContain(
            "checkout",
            CompositionGeneratorHarness.Source(first),
            StringComparison.Ordinal
        );
    }
}
