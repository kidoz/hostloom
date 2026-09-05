using HostLoom.Composition;
using HostLoom.Composition.Generators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed partial class CompositionAdvancedGeneratorTests
{
    private const string Types = """
        public interface ICatalog { }
        public interface IInventory { }
        public class Catalog : ICatalog, IInventory { public Catalog() { } }
        public class Inventory : ICatalog { public Inventory() { } }
        public class Unrelated { }
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

    private static CompositionPlan Plan(string rules, string types = Types)
    {
        var (driver, output) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(Fixture(rules, types))
        );
        CompositionGeneratorHarness.AssertSuccess(driver, output);
        return CompositionGeneratorHarness.LoadPlan(output);
    }

    [Theory]
    [InlineData("WithAttribute<MarkerAttribute>()", 2)]
    [InlineData("WithoutAttribute<MarkerAttribute>()", 1)]
    public void Attribute_inheritance_filters_candidates_without_interface_attribute_inheritance(
        string filter,
        int count
    )
    {
        var plan = Plan(
            $"rules.AddClasses().AssignableTo<ICatalog>().{filter}.AsSelf().WithScopedLifetime().ExpectOne().ExpectExactly({count});",
            """
            [System.AttributeUsage(System.AttributeTargets.All, Inherited = true)]
            public class MarkerAttribute : System.Attribute { }
            [Marker] public interface ICatalog { }
            [Marker] public abstract class CatalogBase : ICatalog { }
            public class Catalog : CatalogBase { public Catalog() { } }
            public class Inventory : CatalogBase { public Inventory() { } }
            public class Shipment : ICatalog { public Shipment() { } }
            public class Unrelated { }
            """
        );
        Assert.Equal(count, plan.Probe().Registrations.Count);
        Assert.DoesNotContain(
            plan.Probe().RejectedCandidates,
            item => item.CandidateIdentity.Contains("Unrelated", StringComparison.Ordinal)
        );
        Assert.Contains(
            plan.Probe().RejectedCandidates,
            item => item.Reasons.Contains("Abstract class.")
        );
        Assert.All(
            plan.Probe().Registrations,
            item => Assert.Contains(filter, item.Origin.Selector, StringComparison.Ordinal)
        );
    }

    [Fact]
    public void Positive_marker_bounds_discovery_and_respects_noninherited_attributes()
    {
        var plan = Plan(
            "rules.AddClasses().WithAttribute<MarkerAttribute>().WithoutAttribute<ExcludeAttribute>().AsAllImplementedInterfaces().WithTransientLifetime().ExpectMany().ExpectExactly(1);",
            """
            [System.AttributeUsage(System.AttributeTargets.Class, Inherited = false)]
            public class MarkerAttribute : System.Attribute { }
            public class ExcludeAttribute : System.Attribute { }
            public interface ICatalog { }
            [Marker] public class Catalog : ICatalog { public Catalog() { } }
            public class Child : Catalog { public Child() { } }
            [Marker, Exclude] public class Inventory : ICatalog { public Inventory() { } }
            public class Unrelated { }
            """
        );
        Assert.Equal(
            "Catalog",
            Assert.Single(plan.Probe().Registrations).Descriptor.ImplementationType!.Name
        );
        Assert.Equal(
            "Inventory",
            Assert.Single(plan.Probe().RejectedCandidates).CandidateType!.Name
        );
    }

    [Fact]
    public void Rejected_inaccessible_types_have_stable_identity_and_ordered_reasons()
    {
        var plan = Plan(
            "rules.AddClasses().AssignableTo<ICatalog>().WithoutAttribute<ExcludeAttribute>().AsSelf().WithTransientLifetime().ExpectOne().AllowEmpty();",
            """
            public interface ICatalog { }
            public class ExcludeAttribute : System.Attribute { }
            public class Container
            {
                [Exclude] private abstract class Catalog : ICatalog { }
            }
            """
        );
        var rejection = Assert.Single(plan.Probe().RejectedCandidates);
        Assert.Null(rejection.CandidateType);
        Assert.Equal("global::Container.Catalog", rejection.CandidateIdentity);
        Assert.Equal(
            ["Excluded attribute: global::ExcludeAttribute", "Abstract class."],
            rejection.Reasons
        );
    }

    [Theory]
    [InlineData("Catalog.Services")]
    [InlineData("Catalog.Services.Converters")]
    public void Namespace_guard_allows_exact_namespace_and_children(string space)
    {
        var plan = Plan(
            "rules.AddClasses().AssignableTo<ICatalog>().RequireNamespace(\"Catalog.Services\").AsImplementedInterfaces().WithScopedLifetime().ExpectMany();",
            "public interface ICatalog { } namespace "
                + space
                + " { public class Converter : ICatalog { public Converter() { } } }"
        );
        Assert.Single(plan.Probe().Registrations);
    }

    [Theory]
    [InlineData(
        "rules.AddClasses().WithoutAttribute<System.ObsoleteAttribute>().AsSelf().WithScopedLifetime().ExpectOne();",
        "HLM0010"
    )]
    [InlineData(
        "rules.AddClasses().AssignableTo<ICatalog>().RequireNamespace(\"Catalog\").AsSelf().WithScopedLifetime().ExpectOne();",
        "HLM0010"
    )]
    [InlineData(
        "rules.AddClasses().AssignableTo<ICatalog>().RequireNamespace(\"\").AsSelf().WithScopedLifetime().ExpectOne();",
        "HLM0010"
    )]
    [InlineData(
        "rules.AddTypes(typeof(Catalog)).AsSelf().WithScopedLifetime().ExpectOne().ExpectExactly(-1);",
        "HLM0014"
    )]
    [InlineData(
        "rules.AddTypes(typeof(Catalog)).AsSelf().WithScopedLifetime().ExpectOne().ExpectExactly(2);",
        "HLM0014"
    )]
    [InlineData(
        "rules.AddTypes().AsSelf().WithScopedLifetime().ExpectOne().AllowEmpty().ExpectAtLeast(1);",
        "HLM0014"
    )]
    [InlineData(
        "rules.AddTypes(typeof(Catalog)).AsSelf().WithScopedLifetime().ExpectOne().Append().Skip();",
        "HLM0011"
    )]
    [InlineData(
        "rules.AddTypes(typeof(Catalog)).AsSelf().WithScopedLifetime().ExpectOne().Replace((CompositionReplacementBehavior)0);",
        "HLM0011"
    )]
    [InlineData(
        "rules.AddTypes(typeof(Catalog)).AsSelf().WithScopedLifetime().ExpectOne().Replace((CompositionReplacementBehavior)4);",
        "HLM0011"
    )]
    public void Invalid_advanced_rules_fail_at_authored_locations(string rules, string id)
    {
        var (driver, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(Fixture(rules))
        );
        Assert.Contains(
            driver.GetRunResult().Diagnostics,
            diagnostic =>
                diagnostic.Id == id && diagnostic.Location.SourceTree!.FilePath == "Rules.cs"
        );
        Assert.Empty(Assert.Single(driver.GetRunResult().Results).GeneratedSources);
    }

    [Fact]
    public void Counts_are_distinct_implementations_before_projection_and_do_not_override_cardinality()
    {
        var plan = Plan(
            "rules.AddTypes(typeof(Catalog), typeof(Catalog)).AsAllImplementedInterfaces().WithScopedLifetime().ExpectOne().ExpectExactly(1).ExpectAtLeast(1);"
        );
        Assert.Equal(2, plan.Probe().Registrations.Count);
        var (driver, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(
                Fixture(
                    "rules.AddTypes(typeof(Catalog), typeof(Inventory)).As<ICatalog>().WithScopedLifetime().ExpectOne().ExpectExactly(2).Skip();"
                )
            )
        );
        Assert.Contains(
            driver.GetRunResult().Diagnostics,
            diagnostic => diagnostic.Id == "HLM0013" && diagnostic.AdditionalLocations.Count == 1
        );
    }

    [Theory]
    [InlineData("Singleton", true)]
    [InlineData("Scoped", true)]
    [InlineData("Transient", false)]
    public void Self_aliases_follow_lifetime_identity_and_container_disposal(
        string lifetime,
        bool shared
    )
    {
        var plan = Plan(
            $"rules.AddTypes(typeof(Catalog)).AssignableToAny(typeof(ICatalog), typeof(IInventory)).AsSelfWithInterfaces().With{lifetime}Lifetime().ExpectOne();",
            """
            public interface ICatalog { }
            public interface IInventory { }
            public class Catalog : ICatalog, IInventory, System.IDisposable
            {
                public Catalog() { }
                public int Disposals { get; private set; }
                public void Dispose() { Disposals++; }
            }
            """
        );
        var registrations = plan.Probe().Registrations;
        Assert.Equal(3, registrations.Count);
        Assert.DoesNotContain(
            registrations,
            entry => entry.Descriptor.ServiceType == typeof(IDisposable)
        );
        var selfType = registrations
            .Single(entry => entry.Descriptor.ImplementationType is not null)
            .Descriptor.ServiceType;
        var interfaces = registrations
            .Where(entry => entry.Descriptor.ImplementationFactory is not null)
            .Select(entry => entry.Descriptor.ServiceType)
            .ToArray();
        var services = new ServiceCollection();
        plan.ApplyTo(services);
        var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );
        var scope = provider.CreateScope();
        object self = scope.ServiceProvider.GetRequiredService(selfType);
        object first = scope.ServiceProvider.GetRequiredService(interfaces[0]);
        object second = scope.ServiceProvider.GetRequiredService(interfaces[1]);
        Assert.Equal(shared, ReferenceEquals(self, first));
        Assert.Equal(shared, ReferenceEquals(first, second));
        using (var other = provider.CreateScope())
            Assert.Equal(
                lifetime == "Singleton",
                ReferenceEquals(self, other.ServiceProvider.GetRequiredService(selfType))
            );
        scope.Dispose();
        if (lifetime == "Singleton")
            Assert.Equal(0, selfType.GetProperty("Disposals")!.GetValue(self));
        provider.Dispose();
        Assert.Equal(shared ? 3 : 1, selfType.GetProperty("Disposals")!.GetValue(self));
        Assert.Equal(shared ? 3 : 2, selfType.GetProperty("Disposals")!.GetValue(first));
    }

    [Fact]
    public async Task Async_alias_disposal_is_owned_by_the_container_and_may_repeat()
    {
        var plan = Plan(
            "rules.AddTypes(typeof(Catalog)).AssignableTo<ICatalog>().AsSelfWithInterfaces().WithScopedLifetime().ExpectOne();",
            """
            public interface ICatalog { }
            public class Catalog : ICatalog, System.IAsyncDisposable
            {
                public Catalog() { }
                public int Disposals { get; private set; }
                public System.Threading.Tasks.ValueTask DisposeAsync() { Disposals++; return default; }
            }
            """
        );
        var services = new ServiceCollection();
        plan.ApplyTo(services);
        await using var provider = services.BuildServiceProvider();
        var scope = provider.CreateAsyncScope();
        var selfType = plan.Probe()
            .Registrations.Single(entry => entry.Descriptor.ImplementationType is not null)
            .Descriptor.ServiceType;
        var aliasType = plan.Probe()
            .Registrations.Single(entry => entry.Descriptor.ImplementationFactory is not null)
            .Descriptor.ServiceType;
        object self = scope.ServiceProvider.GetRequiredService(selfType);
        Assert.Same(self, scope.ServiceProvider.GetRequiredService(aliasType));
        await scope.DisposeAsync();
        Assert.Equal(2, selfType.GetProperty("Disposals")!.GetValue(self));
    }

    [Theory]
    [InlineData("Append", CompositionRegistrationStrategy.Append)]
    [InlineData("Skip", CompositionRegistrationStrategy.Skip)]
    [InlineData("Throw", CompositionRegistrationStrategy.Throw)]
    [InlineData("Replace", CompositionRegistrationStrategy.Replace)]
    public void Generated_strategies_are_applied_by_the_runtime(
        string policy,
        CompositionRegistrationStrategy strategy
    )
    {
        string args = policy == "Replace" ? "CompositionReplacementBehavior.All" : "";
        var plan = Plan(
            $"rules.AddTypes(typeof(Catalog)).As<ICatalog>().WithScopedLifetime().ExpectMany().{policy}({args});"
        );
        var entry = Assert.Single(plan.Probe().Registrations);
        Assert.Equal(strategy, entry.Strategy);
        IServiceCollection services = new ServiceCollection();
        services.Add(entry.Descriptor);
        if (policy is "Throw" or "Append")
            Assert.Throws<CompositionValidationException>(() => plan.ApplyTo(services));
        else
        {
            var report = plan.ApplyTo(services);
            Assert.Single(services);
            Assert.NotEmpty(report.Probe());
        }
    }

    [Fact]
    public void Open_generic_inherited_positional_registration_resolves_known_closed_service()
    {
        var plan = Plan(
            "rules.AddOpenGeneric(typeof(ICatalog<>), typeof(Catalog<>)).WithScopedLifetime().ExpectOne().ExpectExactly(1);",
            """
            public interface ICatalog<T> where T : class { }
            public abstract class CatalogBase<T> : ICatalog<T> where T : class { }
            public class Catalog<T> : CatalogBase<T> where T : class { public Catalog() { } }
            """
        );
        var entry = Assert.Single(plan.Probe().Registrations);
        Assert.True(entry.Descriptor.ImplementationType!.IsGenericTypeDefinition);
        var services = new ServiceCollection();
        plan.ApplyTo(services);
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        Type service = entry.Descriptor.ServiceType.MakeGenericType(typeof(string));
        Assert.Equal(
            entry.Descriptor.ImplementationType.MakeGenericType(typeof(string)),
            scope.ServiceProvider.GetRequiredService(service).GetType()
        );
    }

    [Theory]
    [InlineData(
        "public interface ICatalog<T> { } public class Catalog<T> : ICatalog<T> where T : class { public Catalog() { } }"
    )]
    [InlineData(
        "public interface ICatalog<T> { } public class Catalog<T> : ICatalog<T> where T : new() { public Catalog() { } }"
    )]
    [InlineData(
        "public interface ICatalog<T> { } public abstract class Catalog<T> : ICatalog<T> { }"
    )]
    [InlineData(
        "public interface ICatalog<T> { } public class Catalog<T> : ICatalog<T> { private Catalog() { } }"
    )]
    [InlineData(
        "public interface ICatalog<T> { } public class Catalog<T> { public Catalog() { } }"
    )]
    [InlineData(
        "public interface ICatalog<T> { } public class Catalog<T> : ICatalog<string> { public Catalog() { } }"
    )]
    [InlineData(
        "public interface ICatalog<T> { } public class Catalog<T> : ICatalog<System.Collections.Generic.List<T>> { public Catalog() { } }"
    )]
    public void Unsupported_open_generic_shapes_are_diagnosed(string types)
    {
        var (driver, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(
                Fixture(
                    "rules.AddOpenGeneric(typeof(ICatalog<>), typeof(Catalog<>)).WithScopedLifetime().ExpectOne();",
                    types
                )
            )
        );
        Assert.Contains(
            driver.GetRunResult().Diagnostics,
            diagnostic => diagnostic.Id == "HLM0015"
        );
    }

    [Theory]
    [InlineData("class")]
    [InlineData("struct")]
    [InlineData("unmanaged")]
    [InlineData("new()")]
    [InlineData("System.IDisposable")]
    [InlineData("System.IComparable<T>")]
    public void Equal_positional_constraints_are_supported(string constraint)
    {
        var plan = Plan(
            "rules.AddOpenGeneric(typeof(ICatalog<>), typeof(Catalog<>)).WithTransientLifetime().ExpectMany();",
            $"public interface ICatalog<T> where T : {constraint} {{ }} public class Catalog<T> : ICatalog<T> where T : {constraint} {{ public Catalog() {{ }} }}"
        );
        Assert.Single(plan.Probe().Registrations);
    }

    [Theory]
    [InlineData("Scoped")]
    [InlineData("Transient")]
    public void Singleton_capture_is_proven_through_known_transient_intermediates(
        string middleLifetime
    )
    {
        string rules = $"""
            rules.AddTypes(typeof(Catalog)).AsSelf().WithSingletonLifetime().ExpectOne();
            rules.AddTypes(typeof(Inventory)).AsSelf().With{middleLifetime}Lifetime().ExpectOne();
            rules.AddTypes(typeof(Session)).AsSelf().WithScopedLifetime().ExpectOne();
            """;
        var (driver, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(
                Fixture(
                    rules,
                    """
                    public class Catalog(Inventory inventory) { public Inventory Inventory { get; } = inventory; }
                    public class Inventory(Session session) { public Session Session { get; } = session; }
                    public class Session { public Session() { } }
                    """
                )
            )
        );
        Assert.Contains(
            driver.GetRunResult().Diagnostics,
            diagnostic => diagnostic.Id == "HLM0016" && diagnostic.AdditionalLocations.Count == 1
        );
    }

    [Fact]
    public void Unknown_edges_and_ambiguous_constructor_choices_do_not_claim_proven_capture()
    {
        var plan = Plan(
            """
            rules.AddTypes(typeof(Catalog)).AsSelf().WithSingletonLifetime().ExpectOne();
            rules.AddTypes(typeof(Session)).AsSelf().WithScopedLifetime().ExpectOne();
            """,
            """
            public class Catalog { public Catalog(Session session) { } public Catalog(External external, string value) { } }
            public class External { }
            public class Session { public Session() { } }
            """
        );
        Assert.Equal(2, plan.Probe().Registrations.Count);
    }

    [Fact]
    public void Project_relative_paths_are_normalized_without_checkout_root_leaks()
    {
        const string rules =
            "rules.AddTypes(typeof(Catalog)).AsSelf().WithScopedLifetime().ExpectOne();";
        string Generate(string root)
        {
            var compilation = CompositionGeneratorHarness.Compilation(
                Fixture(rules),
                root + "/Composition/./Rules.cs"
            );
            GeneratorDriver driver = CSharpGeneratorDriver.Create(
                [new CompositionGenerator().AsSourceGenerator()],
                parseOptions: CompositionGeneratorHarness.ParseOptions,
                optionsProvider: new BuildOptions(root)
            );
            var (result, output) = CompositionGeneratorHarness.Run(compilation, driver);
            CompositionGeneratorHarness.AssertSuccess(result, output);
            Assert.Equal(
                "Composition/Rules.cs",
                Assert
                    .Single(CompositionGeneratorHarness.LoadPlan(output).Probe().Registrations)
                    .Origin.FilePath
            );
            return CompositionGeneratorHarness.Source(result);
        }
        Assert.Equal(Generate("/checkout/one"), Generate("/another/root"));
        Assert.Equal(Generate("C:\\checkout"), Generate("/another/root"));
    }

    private sealed class BuildOptions(string root) : AnalyzerConfigOptionsProvider
    {
        public override AnalyzerConfigOptions GlobalOptions { get; } = new Values(root);

        public override AnalyzerConfigOptions GetOptions(SyntaxTree tree) => GlobalOptions;

        public override AnalyzerConfigOptions GetOptions(AdditionalText textFile) => GlobalOptions;

        private sealed class Values(string root) : AnalyzerConfigOptions
        {
            public override bool TryGetValue(string key, out string value)
            {
                value = root;
                return key == "build_property.MSBuildProjectDirectory";
            }
        }
    }
}
