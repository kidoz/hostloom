using HostLoom.Composition;
using Microsoft.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed partial class CompositionAdvancedGeneratorTests
{
    [Theory]
    [InlineData(
        "typeof(ICatalog<,>), typeof(Catalog<,>)",
        "public interface ICatalog<T,U> {} public class Catalog<T,U> : ICatalog<U,T> { public Catalog() {} }"
    )]
    [InlineData(
        "typeof(ICatalog<>), typeof(Catalog<,>)",
        "public interface ICatalog<T> {} public class Catalog<T,U> : ICatalog<T> { public Catalog() {} }"
    )]
    [InlineData(
        "typeof(ICatalog<int>), typeof(Catalog<>)",
        "public interface ICatalog<T> {} public class Catalog<T> : ICatalog<T> { public Catalog() {} }"
    )]
    [InlineData(
        "typeof(ICatalog<>), typeof(Catalog<int>)",
        "public interface ICatalog<T> {} public class Catalog<T> : ICatalog<T> { public Catalog() {} }"
    )]
    [InlineData(
        "typeof(ICatalog<>), typeof(Container.Catalog<>)",
        "public interface ICatalog<T> {} public class Container { public class Catalog<T> : ICatalog<T> { public Catalog() {} } }"
    )]
    [InlineData(
        "typeof(ICatalog<>), typeof(Catalog<>)",
        "public interface ICatalog<T> {} public class Catalog<T> : ICatalog<T> where T : System.IDisposable { public Catalog() {} }"
    )]
    [InlineData(
        "typeof(ICatalog<>), typeof(Catalog<>)",
        "public interface ICatalog<T> {} public class Catalog<[System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicMethods)] T> : ICatalog<T> { public Catalog() {} }"
    )]
    public void Invalid_generic_mapping_arity_constraints_and_trimming_metadata_are_errors(
        string pair,
        string types
    )
    {
        var (driver, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(
                Fixture($"rules.AddOpenGeneric({pair}).WithScopedLifetime().ExpectOne();", types)
            )
        );
        Assert.Contains(
            driver.GetRunResult().Diagnostics,
            diagnostic => diagnostic.Id == "HLM0015"
        );
        Assert.Empty(Assert.Single(driver.GetRunResult().Results).GeneratedSources);
    }

    [Fact]
    public void Dependent_positional_constraints_and_inherited_base_service_are_supported()
    {
        var plan = Plan(
            "rules.AddOpenGeneric(typeof(CatalogBase<,>), typeof(Catalog<,>)).WithTransientLifetime().ExpectMany();",
            """
            public abstract class CatalogBase<T,U> where T : U { }
            public class Catalog<T,U> : CatalogBase<T,U> where T : U { public Catalog() { } }
            """
        );
        Assert.Single(plan.Probe().Registrations);
    }

    [Fact]
    public void Namespace_guard_rejects_a_shared_prefix_and_reports_the_type_location()
    {
        var (driver, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(
                Fixture(
                    "rules.AddClasses().AssignableTo<ICatalog>().RequireNamespace(\"Catalog.Services\").AsSelf().WithScopedLifetime().ExpectOne();",
                    "public interface ICatalog {} namespace Catalog.ServicesExtra { public class Converter : ICatalog { public Converter() {} } }"
                )
            )
        );
        Assert.Contains(
            driver.GetRunResult().Diagnostics,
            diagnostic => diagnostic.Id == "HLM0010" && diagnostic.AdditionalLocations.Count == 1
        );
    }

    [Theory]
    [InlineData("AsImplementedInterfaces")]
    [InlineData("AsSelfWithInterfaces")]
    public void Attribute_only_rules_require_an_explicit_service_projection(string projection)
    {
        var (driver, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(
                Fixture(
                    $"rules.AddClasses().WithAttribute<MarkerAttribute>().{projection}().WithScopedLifetime().ExpectOne();",
                    "public class MarkerAttribute : System.Attribute {} [Marker] public class Catalog { public Catalog() {} }"
                )
            )
        );
        Assert.Contains(
            driver.GetRunResult().Diagnostics,
            diagnostic => diagnostic.Id == "HLM0012"
        );
    }

    [Fact]
    public void Multiple_markers_are_conjunctive_and_rejection_inventory_is_bounded_to_any_positive_marker()
    {
        var plan = Plan(
            "rules.AddClasses().WithAttribute<MarkerAttribute>().WithAttribute<ApprovedAttribute>().AsSelf().WithScopedLifetime().ExpectOne();",
            """
            public class MarkerAttribute : System.Attribute { }
            public class ApprovedAttribute : System.Attribute { }
            [Marker, Approved] public class Catalog { public Catalog() { } }
            [Marker] public class Inventory { public Inventory() { } }
            public class Unrelated { }
            """
        );
        Assert.Single(plan.Probe().Registrations);
        Assert.Equal(
            "Inventory",
            Assert.Single(plan.Probe().RejectedCandidates).CandidateType!.Name
        );
    }

    [Fact]
    public void Derived_marker_usage_controls_base_class_inheritance()
    {
        var plan = Plan(
            "rules.AddClasses().WithAttribute<MarkerAttribute>().AsSelf().WithTransientLifetime().ExpectOne();",
            """
            public class MarkerAttribute : System.Attribute { }
            [System.AttributeUsage(System.AttributeTargets.Class, Inherited = false)]
            public class DirectMarkerAttribute : MarkerAttribute { }
            [DirectMarker] public class Catalog { public Catalog() { } }
            public class Inventory : Catalog { public Inventory() { } }
            """
        );
        Assert.Equal(
            "Catalog",
            Assert.Single(plan.Probe().Registrations).Descriptor.ImplementationType!.Name
        );
    }

    [Theory]
    [InlineData(
        "System.Collections.Generic.IEnumerable<Session>",
        "rules.AddTypes(typeof(Session)).AsSelf().WithScopedLifetime().ExpectMany();",
        "public class Session { public Session() {} }"
    )]
    [InlineData(
        "ISession",
        "rules.AddTypes(typeof(Session)).AssignableTo<ISession>().AsSelfWithInterfaces().WithScopedLifetime().ExpectOne();",
        "public interface ISession {} public class Session : ISession { public Session() {} }"
    )]
    [InlineData(
        "ISession<string>",
        "rules.AddOpenGeneric(typeof(ISession<>), typeof(Session<>)).WithScopedLifetime().ExpectOne();",
        "public interface ISession<T> {} public class Session<T> : ISession<T> { public Session() {} }"
    )]
    public void Known_enumerable_alias_and_closed_open_generic_edges_prove_capture(
        string dependency,
        string registration,
        string types
    )
    {
        var (driver, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(
                Fixture(
                    "rules.AddTypes(typeof(Catalog)).AsSelf().WithSingletonLifetime().ExpectOne(); "
                        + registration,
                    $"public class Catalog({dependency} dependency) {{ public object Dependency {{ get; }} = dependency; }} "
                        + types
                )
            )
        );
        Assert.Contains(
            driver.GetRunResult().Diagnostics,
            diagnostic => diagnostic.Id == "HLM0016"
        );
    }

    [Fact]
    public void Closed_override_precedence_prevents_a_false_capture_from_open_registration()
    {
        var plan = Plan(
            """
            rules.AddTypes(typeof(Catalog)).AsSelf().WithSingletonLifetime().ExpectOne();
            rules.AddTypes(typeof(ClosedSession)).As<ISession<string>>().WithSingletonLifetime().ExpectOne();
            rules.AddOpenGeneric(typeof(ISession<>), typeof(Session<>)).WithScopedLifetime().ExpectOne();
            """,
            """
            public class Catalog(ISession<string> session) { public object Session { get; } = session; }
            public interface ISession<T> { }
            public class ClosedSession : ISession<string> { public ClosedSession() { } }
            public class Session<T> : ISession<T> { public Session() { } }
            """
        );
        Assert.Equal(3, plan.Probe().Registrations.Count);
    }

    [Fact]
    public void Keyed_dependencies_are_not_confused_with_unkeyed_scoped_registrations()
    {
        var plan = Plan(
            """
            rules.AddTypes(typeof(Catalog)).AsSelf().WithSingletonLifetime().ExpectOne();
            rules.AddTypes(typeof(Session)).AsSelf().WithScopedLifetime().ExpectOne();
            """,
            """
            public class Catalog([FromKeyedServices("catalog")] Session session) { public object Session { get; } = session; }
            public class Session { public Session() { } }
            """
        );
        Assert.Equal(2, plan.Probe().Registrations.Count);
    }

    [Fact]
    public void Expanding_generic_dependency_cycles_do_not_recurse_without_bound()
    {
        var plan = Plan(
            """
            rules.AddTypes(typeof(Catalog)).AsSelf().WithSingletonLifetime().ExpectOne();
            rules.AddOpenGeneric(typeof(ISession<>), typeof(Session<>)).WithTransientLifetime().ExpectOne();
            """,
            """
            public class Catalog(ISession<string> session) { public object Session { get; } = session; }
            public interface ISession<T> { }
            public class Session<T>(ISession<System.Collections.Generic.List<T>> next) : ISession<T> { public object Next { get; } = next; }
            """
        );
        Assert.Equal(2, plan.Probe().Registrations.Count);
    }

    [Fact]
    public void Duplicate_aliases_across_plans_retain_known_implementation_identity()
    {
        var plan = Plan(
            "rules.AddTypes(typeof(Catalog)).AssignableTo<ICatalog>().AsSelfWithInterfaces().WithScopedLifetime().ExpectMany();"
        );
        var alias = plan.Probe().Registrations.Single(entry => entry.AliasTargetType is not null);
        Assert.Equal("Catalog", alias.ImplementationType!.Name);
        var first = new CompositionPlan("first", [alias]);
        var secondAlias = new CompositionRegistration(
            new ServiceDescriptor(
                alias.Descriptor.ServiceType,
                _ => throw new InvalidOperationException("Factories must not run during planning."),
                ServiceLifetime.Scoped
            ),
            CompositionCardinality.Many,
            new CompositionOrigin("second"),
            aliasTargetType: alias.AliasTargetType
        );
        var second = new CompositionPlan("second", [secondAlias]);
        var services = new ServiceCollection();
        first.ApplyTo(services);
        var error = Assert.Throws<CompositionValidationException>(() => second.ApplyTo(services));
        Assert.Contains("Catalog", error.Message, StringComparison.Ordinal);
        Assert.Single(services);
        Assert.Throws<ArgumentException>(() =>
            new CompositionRegistration(
                ServiceDescriptor.Singleton<string>("catalog"),
                CompositionCardinality.One,
                new CompositionOrigin("invalid"),
                aliasTargetType: typeof(string)
            )
        );
    }

    [Fact]
    public void Explicit_enumerable_registration_precedes_builtin_enumerable_resolution()
    {
        var plan = Plan(
            """
            rules.AddTypes(typeof(Catalog)).AsSelf().WithSingletonLifetime().ExpectOne();
            rules.AddTypes(typeof(Sessions)).As<System.Collections.Generic.IEnumerable<Session>>().WithSingletonLifetime().ExpectOne();
            rules.AddTypes(typeof(Session)).AsSelf().WithScopedLifetime().ExpectOne();
            """,
            """
            public class Catalog(System.Collections.Generic.IEnumerable<Session> sessions) { public object Sessions { get; } = sessions; }
            public class Sessions : System.Collections.Generic.List<Session> { public Sessions() { } }
            public class Session { public Session() { } }
            """
        );
        Assert.Equal(3, plan.Probe().Registrations.Count);
    }

    [Fact]
    public void Open_singleton_fixed_constructor_edges_can_prove_scoped_capture()
    {
        var (driver, _) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(
                Fixture(
                    """
                    rules.AddOpenGeneric(typeof(ICatalog<>), typeof(Catalog<>)).WithSingletonLifetime().ExpectOne();
                    rules.AddTypes(typeof(Session)).AsSelf().WithScopedLifetime().ExpectOne();
                    """,
                    """
                    public interface ICatalog<T> { }
                    public class Catalog<T>(Session session) : ICatalog<T> { public object Session { get; } = session; }
                    public class Session { public Session() { } }
                    """
                )
            )
        );
        Assert.Contains(
            driver.GetRunResult().Diagnostics,
            diagnostic => diagnostic.Id == "HLM0016"
        );
    }

    [Fact]
    public void Advanced_output_has_reviewable_snapshot()
    {
        var (driver, output) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(
                Fixture(
                    """
                    rules.AddClasses().AssignableTo<ICatalog>().WithoutAttribute<ExcludedAttribute>().RequireNamespace("Catalog.Services")
                        .AsSelfWithInterfaces().WithScopedLifetime().ExpectMany().ExpectExactly(1).Replace(CompositionReplacementBehavior.ServiceType);
                    rules.AddOpenGeneric(typeof(IRepository<>), typeof(Repository<>)).WithTransientLifetime().ExpectOne();
                    """,
                    """
                    public interface ICatalog { }
                    public class ExcludedAttribute : System.Attribute { }
                    namespace Catalog.Services
                    {
                        public class Converter : ICatalog { public Converter() { } }
                        [Excluded] public class Inventory : ICatalog { public Inventory() { } }
                    }
                    public interface IRepository<T> { }
                    public class Repository<T> : IRepository<T> { public Repository() { } }
                    """
                )
            )
        );
        CompositionGeneratorHarness.AssertSuccess(driver, output);
        string generated = CompositionGeneratorHarness.Source(driver);
        string received = Path.Combine(
            Path.GetTempPath(),
            "HostLoom.Composition.Advanced.received.txt"
        );
        File.WriteAllText(received, generated);
        string expected = Path.Combine(
            AppContext.BaseDirectory,
            "Snapshots",
            "Composition",
            "Advanced.verified.txt"
        );
        Assert.True(File.Exists(expected), "Review generated source at " + received);
        Assert.Equal(File.ReadAllText(expected), generated);
    }
}
