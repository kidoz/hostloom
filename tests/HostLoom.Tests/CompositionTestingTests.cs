using HostLoom.Composition;
using HostLoom.Composition.Testing;
using HostLoom.Diagnostics;
using HostLoom.Examples.CompositionDiagnostics;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Tests;

public sealed class CompositionTestingTests
{
    private static readonly CompositionOrigin Origin = new(
        "DeclareCatalog",
        "catalog",
        "Rules.cs",
        8,
        "explicit catalog"
    );

    private static CompositionRegistration Entry<T>(CompositionOrigin? origin = null)
        where T : class, ICatalog =>
        new(ServiceDescriptor.Scoped<ICatalog, T>(), CompositionCardinality.Many, origin ?? Origin);

    [Fact]
    public void Multiset_equivalence_ignores_origins_and_order_but_sequence_assertions_do_not()
    {
        var first = new CompositionPlan("first", [Entry<Catalog>(), Entry<Inventory>()]);
        var second = new CompositionPlan(
            "second",
            [Entry<Inventory>(new CompositionOrigin("Other")), Entry<Catalog>()]
        );
        CompositionAssert.EquivalentRegistrations(first, second);
        Assert.Throws<CompositionAssertionException>(() =>
            CompositionAssert.RegistrationSequence(first, second)
        );
        Assert.Throws<CompositionAssertionException>(() =>
            CompositionAssert.Origins(second.Probe(), Origin, Origin)
        );
        CompositionAssert.Origins(first.Probe(), Origin, Origin);
    }

    [Fact]
    public void Multiset_comparison_preserves_duplicate_multiplicity()
    {
        var catalog = CompositionRegistrationShape.From(Entry<Catalog>());
        var inventory = CompositionRegistrationShape.From(Entry<Inventory>());
        CompositionAssert.EquivalentRegistrations(
            [catalog, catalog, inventory],
            [inventory, catalog, catalog]
        );
        Assert.Throws<CompositionAssertionException>(() =>
            CompositionAssert.EquivalentRegistrations([catalog, catalog], [catalog])
        );
        Assert.Throws<CompositionAssertionException>(() =>
            CompositionAssert.EquivalentRegistrations([catalog], [catalog, catalog])
        );
        Assert.Throws<CompositionAssertionException>(() =>
            CompositionAssert.EquivalentRegistrations([catalog, inventory], [catalog, catalog])
        );
    }

    [Fact]
    public void Independent_descriptors_and_alias_delegates_have_semantic_equality()
    {
        var left = CompositionRegistrationShape.From(Entry<Catalog>());
        var right = CompositionRegistrationShape.From(
            Entry<Catalog>(new CompositionOrigin("Other"))
        );
        Assert.Equal(left, right);
        CompositionRegistration Alias() =>
            new(
                new ServiceDescriptor(
                    typeof(ICatalog),
                    _ => throw new InvalidOperationException("Do not execute."),
                    ServiceLifetime.Scoped
                ),
                CompositionCardinality.Many,
                Origin,
                aliasTargetType: typeof(Catalog)
            );
        var first = CompositionRegistrationShape.From(Alias());
        var second = CompositionRegistrationShape.From(Alias());
        Assert.Equal(first, second);
        Assert.NotEqual(left, first);
        Assert.Equal(CompositionActivationKind.ForwardingAlias, first.Activation);
        Assert.Equal(typeof(Catalog), first.AliasTargetType);
    }

    [Theory]
    [InlineData(ServiceLifetime.Singleton)]
    [InlineData(ServiceLifetime.Transient)]
    public void Lifetime_is_part_of_semantics(ServiceLifetime lifetime)
    {
        var expected = CompositionRegistrationShape.From(Entry<Catalog>());
        var actual = CompositionRegistrationShape.FromDescriptor(
            new ServiceDescriptor(typeof(ICatalog), typeof(Catalog), lifetime)
        );
        Assert.Throws<CompositionAssertionException>(() =>
            CompositionAssert.EquivalentRegistrations([expected], [actual])
        );
    }

    [Fact]
    public void Opaque_factories_and_instances_require_explicit_semantic_contracts()
    {
        var factory = new ServiceDescriptor(
            typeof(ICatalog),
            _ => throw new InvalidOperationException("Never execute."),
            ServiceLifetime.Singleton
        );
        var instance = ServiceDescriptor.Singleton<ICatalog>(new Catalog());
        Assert.Throws<ArgumentException>(() =>
            CompositionRegistrationShape.FromDescriptor(factory)
        );
        Assert.Throws<ArgumentException>(() =>
            CompositionRegistrationShape.FromDescriptor(instance)
        );
        var first = CompositionRegistrationShape.FromDescriptor(
            factory,
            opaqueIdentity: "catalog configuration v1"
        );
        var second = CompositionRegistrationShape.FromDescriptor(
            new ServiceDescriptor(typeof(ICatalog), _ => new Catalog(), ServiceLifetime.Singleton),
            opaqueIdentity: "catalog configuration v1"
        );
        Assert.Equal(first, second);
        Assert.NotEqual(
            first,
            CompositionRegistrationShape.FromDescriptor(
                instance,
                opaqueIdentity: "catalog configuration v1"
            )
        );
        Assert.NotEqual(
            first,
            CompositionRegistrationShape.FromDescriptor(
                factory,
                opaqueIdentity: "catalog configuration v2"
            )
        );
        var plan = new CompositionPlan(
            "opaque",
            [new(factory, CompositionCardinality.One, Origin)]
        );
        Assert.Throws<ArgumentException>(() => CompositionRegistrationShape.Project(plan.Probe()));
        Assert.Equal(
            first,
            Assert.Single(
                CompositionRegistrationShape.Project(plan.Probe(), _ => "catalog configuration v1")
            )
        );
        Assert.Throws<CompositionAssertionException>(() =>
            CompositionAssert.MatchedTypes(plan.Probe(), typeof(Catalog))
        );
    }

    [Fact]
    public void Probe_assertions_cover_matches_projection_lifetime_cardinality_and_rejection_reasons()
    {
        var reasons = new[] { "Marker absent", "Abstract class" };
        var plan = new CompositionPlan(
            "catalog",
            [Entry<Catalog>(), Entry<Inventory>()],
            [new("CatalogBase", Origin, reasons)]
        );
        reasons[0] = "changed after construction";
        CompositionAssert.MatchedTypes(plan.Probe(), typeof(Inventory), typeof(Catalog));
        CompositionAssert.Service(
            plan.Probe(),
            typeof(ICatalog),
            ServiceLifetime.Scoped,
            CompositionCardinality.Many,
            typeof(Catalog),
            typeof(Inventory)
        );
        CompositionAssert.Rejection(
            plan.Probe(),
            "CatalogBase",
            Origin,
            "Marker absent",
            "Abstract class"
        );
        Assert.Throws<CompositionAssertionException>(() =>
            CompositionAssert.MatchedTypes(plan.Probe(), typeof(Catalog))
        );
        Assert.Throws<CompositionAssertionException>(() =>
            CompositionAssert.Service(
                plan.Probe(),
                typeof(ICatalog),
                ServiceLifetime.Singleton,
                CompositionCardinality.Many,
                typeof(Catalog),
                typeof(Inventory)
            )
        );
        Assert.Throws<CompositionAssertionException>(() =>
            CompositionAssert.Service(
                plan.Probe(),
                typeof(ICatalog),
                ServiceLifetime.Scoped,
                CompositionCardinality.One,
                typeof(Catalog),
                typeof(Inventory)
            )
        );
        Assert.Throws<CompositionAssertionException>(() =>
            CompositionAssert.Service(
                plan.Probe(),
                typeof(ICatalog),
                ServiceLifetime.Scoped,
                CompositionCardinality.Many,
                typeof(Catalog)
            )
        );
        Assert.Throws<CompositionAssertionException>(() =>
            CompositionAssert.Rejection(
                plan.Probe(),
                "CatalogBase",
                Origin,
                "Abstract class",
                "Marker absent"
            )
        );
        Assert.Throws<CompositionAssertionException>(() =>
            CompositionAssert.Rejection(plan.Probe(), "Missing", Origin, "Marker absent")
        );
        var projection = CompositionRegistrationShape.Project(plan.Probe());
        Assert.Throws<NotSupportedException>(() =>
            ((IList<CompositionRegistrationShape>)projection).Clear()
        );
    }

    [Fact]
    public void Shape_input_errors_are_not_silently_normalized()
    {
        Assert.Throws<ArgumentException>(() =>
            CompositionRegistrationShape.FromDescriptor(
                ServiceDescriptor.KeyedSingleton<ICatalog, Catalog>("catalog")
            )
        );
        Assert.Throws<ArgumentException>(() =>
            CompositionRegistrationShape.FromDescriptor(
                Entry<Catalog>().Descriptor,
                aliasTargetType: typeof(Catalog)
            )
        );
        Assert.Throws<ArgumentException>(() =>
            CompositionRegistrationShape.FromDescriptor(
                Entry<Catalog>().Descriptor,
                opaqueIdentity: "ignored"
            )
        );
        Assert.Throws<ArgumentNullException>(() =>
            CompositionAssert.RegistrationSequence(
                (IEnumerable<CompositionRegistrationShape>)null!,
                []
            )
        );
        Assert.Throws<ArgumentException>(() =>
            CompositionAssert.EquivalentRegistrations([null!], [])
        );
    }

    [Fact]
    public void Enumerable_ledger_adapter_records_one_choice_per_service_without_conflicts()
    {
        var plan = new CompositionPlan(
            "catalog",
            [Entry<Catalog>(), Entry<Inventory>()],
            [new("RejectedCatalog", Origin, ["Marker absent"])]
        );
        var services = new ServiceCollection();
        var ledger = new CompositionLedger();
        ApplicationCompositionLedger.Record(ledger, plan, plan.ApplyTo(services));
        var report = ledger.Snapshot();
        Assert.Empty(report.Conflicts);
        Assert.Equal(2, report.Decisions.Count);
        var choice = report.Decisions.Single(item => item.Choice != CompositionDecision.Skipped);
        Assert.Contains("Catalog / Scoped", choice.Choice, StringComparison.Ordinal);
        Assert.Contains("Inventory / Scoped", choice.Choice, StringComparison.Ordinal);
        Assert.Contains("DeclareCatalog", choice.Reason, StringComparison.Ordinal);
        Assert.Equal(
            "Marker absent",
            report.Decisions.Single(item => item.Choice == CompositionDecision.Skipped).Reason
        );
    }

    [Fact]
    public void Ledger_adapter_keeps_skips_replacements_and_removed_additions_visible()
    {
        var origin = new CompositionOrigin("ReplaceCatalog", "catalog");
        var plan = new CompositionPlan(
            "catalog",
            [
                Entry<Catalog>(),
                new(
                    Entry<Inventory>().Descriptor,
                    CompositionCardinality.Many,
                    origin,
                    CompositionRegistrationStrategy.Replace
                ),
            ]
        );
        var services = new ServiceCollection();
        var ledger = new CompositionLedger();
        var applied = plan.ApplyTo(services);
        ApplicationCompositionLedger.Record(ledger, plan, applied);
        var decision = Assert.Single(ledger.Snapshot().Decisions);
        Assert.DoesNotContain("Catalog / Scoped", decision.Choice, StringComparison.Ordinal);
        Assert.Contains("Inventory / Scoped", decision.Choice, StringComparison.Ordinal);
        Assert.Contains("Replaced:", decision.Reason, StringComparison.Ordinal);
        Assert.Contains("previous=", decision.Reason, StringComparison.Ordinal);
        var skipped = new CompositionPlan(
            "skipped",
            [
                new(
                    Entry<Catalog>().Descriptor,
                    CompositionCardinality.Many,
                    Origin,
                    CompositionRegistrationStrategy.Skip
                ),
            ]
        );
        var otherLedger = new CompositionLedger();
        ApplicationCompositionLedger.Record(otherLedger, skipped, skipped.ApplyTo(services));
        var skip = Assert.Single(otherLedger.Snapshot().Decisions);
        Assert.Equal("No retained additions", skip.Choice);
        Assert.Contains("Skipped:", skip.Reason, StringComparison.Ordinal);
        services.Clear();
        Assert.Equal(decision, Assert.Single(ledger.Snapshot().Decisions));
        Assert.Throws<ArgumentException>(() =>
            ApplicationCompositionLedger.Record(new CompositionLedger(), skipped, applied)
        );
    }

    public interface ICatalog;

    public sealed class Catalog : ICatalog
    {
        public Catalog() { }
    }

    public sealed class Inventory : ICatalog
    {
        public Inventory() { }
    }
}
