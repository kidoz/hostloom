using HostLoom.Composition;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Xunit;

namespace HostLoom.Tests;

public sealed class CompositionPlanTests
{
    private static readonly CompositionOrigin Origin = new(
        "DeclareCatalog",
        "catalog",
        "Composition.cs",
        12
    );

    [Fact]
    public void Planning_and_probing_copy_inputs_without_executing_factories()
    {
        ServiceDescriptor descriptor = ServiceDescriptor.Transient<ICatalog>(_ =>
            throw new InvalidOperationException("Executed")
        );
        var entries = new List<CompositionRegistration> { Entry(descriptor) };
        var reasons = new List<string> { "Marker absent" };
        var rejections = new List<CompositionCandidateRejection>
        {
            new(typeof(Inventory), Origin, reasons),
        };
        var plan = new CompositionPlan("catalog", entries, rejections);
        entries.Clear();
        reasons.Clear();
        rejections.Clear();

        CompositionPlanProbe probe = plan.Probe();
        Assert.Same(probe, plan.Probe());
        Assert.Same(descriptor, Assert.Single(probe.Registrations).Descriptor);
        Assert.Equal(
            "Marker absent",
            Assert.Single(Assert.Single(probe.RejectedCandidates).Reasons)
        );
        Assert.Throws<NotSupportedException>(() =>
            ((IList<CompositionRegistration>)probe.Registrations).Clear()
        );
        var services = new ServiceCollection();
        CompositionApplicationReport report = plan.ApplyTo(services);
        Assert.Equal(CompositionApplicationOutcome.Added, Assert.Single(report.Probe()).Outcome);
        Assert.Single(services);
        Assert.Throws<NotSupportedException>(() =>
            ((IList<CompositionApplicationDecision>)report.Probe()).Clear()
        );
    }

    [Fact]
    public void Many_preserves_order_and_scoped_resolution_and_disposal()
    {
        var plan = new CompositionPlan(
            "catalog",
            [
                Entry(ServiceDescriptor.Scoped<ICatalog, Catalog>(), CompositionCardinality.Many),
                Entry(ServiceDescriptor.Scoped<ICatalog, Inventory>(), CompositionCardinality.Many),
            ]
        );
        var services = new ServiceCollection();
        Assert.Same(services, services.AddHostLoomComposition(plan));
        using ServiceProvider provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );
        Catalog first;
        using (IServiceScope scope = provider.CreateScope())
        {
            ICatalog[] catalogs = scope.ServiceProvider.GetServices<ICatalog>().ToArray();
            Assert.Collection(
                catalogs,
                value => Assert.IsType<Catalog>(value),
                value => Assert.IsType<Inventory>(value)
            );
            first = Assert.IsType<Catalog>(catalogs[0]);
            Assert.Same(first, scope.ServiceProvider.GetServices<ICatalog>().First());
            Assert.IsType<Inventory>(scope.ServiceProvider.GetRequiredService<ICatalog>());
        }
        Assert.True(first.Disposed);
        using IServiceScope other = provider.CreateScope();
        Assert.NotSame(first, other.ServiceProvider.GetServices<ICatalog>().First());
    }

    [Fact]
    public void Late_conflict_leaves_collection_unchanged_and_does_not_consume_identity()
    {
        var existing = ServiceDescriptor.Transient<ICatalog, Catalog>();
        var services = new ServiceCollection { existing };
        var plan = new CompositionPlan(
            "catalog",
            [
                Entry(ServiceDescriptor.Transient<IInventory, Inventory>()),
                Entry(ServiceDescriptor.Transient<ICatalog, Inventory>()),
            ]
        );
        CompositionValidationException error = Assert.Throws<CompositionValidationException>(() =>
            plan.ApplyTo(services)
        );
        Assert.Equal(CompositionValidationPhase.Application, error.Phase);
        Assert.Same(existing, Assert.Single(services));
        Assert.Contains("collection index 0", error.Message, StringComparison.Ordinal);
        services.Clear();
        Assert.Equal(2, plan.ApplyTo(services).Probe().Count);
    }

    [Fact]
    public void Same_identity_is_rejected_even_for_fresh_plan_instances_but_other_collections_work()
    {
        var services = new ServiceCollection();
        CreatePlan().ApplyTo(services);
        Assert.Throws<CompositionValidationException>(() => CreatePlan().ApplyTo(services));
        Assert.Single(services);
        var other = new ServiceCollection();
        CreatePlan().ApplyTo(other);
        Assert.Single(other);
    }

    [Theory]
    [InlineData(CompositionRegistrationStrategy.Default)]
    [InlineData(CompositionRegistrationStrategy.Append)]
    [InlineData(CompositionRegistrationStrategy.Throw)]
    public void Existing_duplicate_is_rejected_without_mutation(
        CompositionRegistrationStrategy strategy
    )
    {
        var existing = ServiceDescriptor.Transient<ICatalog, Catalog>();
        var services = new ServiceCollection { existing };
        var plan = new CompositionPlan(
            "catalog",
            [
                Entry(
                    ServiceDescriptor.Transient<ICatalog, Catalog>(),
                    CompositionCardinality.Many,
                    strategy
                ),
            ]
        );
        Assert.Throws<CompositionValidationException>(() => plan.ApplyTo(services));
        Assert.Same(existing, Assert.Single(services));
    }

    [Fact]
    public void Explicit_append_cannot_bypass_one_cardinality()
    {
        var services = new ServiceCollection { ServiceDescriptor.Transient<ICatalog, Catalog>() };
        var plan = new CompositionPlan(
            "catalog",
            [
                Entry(
                    ServiceDescriptor.Transient<ICatalog, Inventory>(),
                    strategy: CompositionRegistrationStrategy.Append
                ),
            ]
        );
        Assert.Throws<CompositionValidationException>(() => plan.ApplyTo(services));
        Assert.Single(services);
    }

    [Fact]
    public void Skip_preserves_an_opaque_existing_factory_and_reports_the_skipped_intention()
    {
        var existing = ServiceDescriptor.Singleton<ICatalog>(_ =>
            throw new InvalidOperationException("Executed")
        );
        var services = new ServiceCollection { existing };
        var plan = new CompositionPlan(
            "catalog",
            [
                Entry(
                    ServiceDescriptor.Transient<ICatalog, Catalog>(),
                    strategy: CompositionRegistrationStrategy.Skip
                ),
            ]
        );
        var decision = Assert.Single(plan.ApplyTo(services).Probe());
        Assert.Equal(CompositionApplicationOutcome.Skipped, decision.Outcome);
        Assert.Same(existing, Assert.Single(services));
        Assert.Null(decision.PreviousOrigin);
    }

    [Fact]
    public void Skip_does_not_accept_an_existing_one_cardinality_violation()
    {
        var services = new ServiceCollection
        {
            ServiceDescriptor.Transient<ICatalog, Catalog>(),
            ServiceDescriptor.Transient<ICatalog, Inventory>(),
        };
        var plan = new CompositionPlan(
            "catalog",
            [
                Entry(
                    ServiceDescriptor.Transient<ICatalog, Catalog>(),
                    strategy: CompositionRegistrationStrategy.Skip
                ),
            ]
        );
        Assert.Throws<CompositionValidationException>(() => plan.ApplyTo(services));
        Assert.Equal(2, services.Count);
    }

    [Theory]
    [InlineData(CompositionReplacementBehavior.ServiceType, 1, 1)]
    [InlineData(CompositionReplacementBehavior.ImplementationType, 1, 2)]
    [InlineData(CompositionReplacementBehavior.All, 2, 1)]
    public void Replacement_predicates_are_unioned_and_keyed_entries_are_untouched(
        CompositionReplacementBehavior behavior,
        int replaced,
        int catalogs
    )
    {
        var keyed = ServiceDescriptor.KeyedTransient<ICatalog, Inventory>("local");
        var opaque = ServiceDescriptor.Transient<IInventory>(_ =>
            throw new InvalidOperationException("Executed")
        );
        var services = new ServiceCollection
        {
            ServiceDescriptor.Transient<ICatalog, Catalog>(),
            ServiceDescriptor.Transient<IInventory, Inventory>(),
            keyed,
            opaque,
        };
        var plan = new CompositionPlan(
            "catalog",
            [
                Entry(
                    ServiceDescriptor.Transient<ICatalog, Inventory>(),
                    CompositionCardinality.Many,
                    CompositionRegistrationStrategy.Replace,
                    behavior
                ),
            ]
        );
        CompositionApplicationReport report = plan.ApplyTo(services);
        Assert.Equal(
            replaced,
            report.Probe().Count(item => item.Outcome == CompositionApplicationOutcome.Replaced)
        );
        Assert.Equal(
            catalogs,
            services.Count(item => !item.IsKeyedService && item.ServiceType == typeof(ICatalog))
        );
        Assert.Contains(keyed, services);
        Assert.Contains(opaque, services);
        Assert.Equal(typeof(Inventory), services.Last().ImplementationType);
    }

    [Fact]
    public void Replacing_with_the_same_descriptor_moves_it_after_retained_descriptors()
    {
        var catalog = ServiceDescriptor.Transient<ICatalog, Catalog>();
        var inventory = ServiceDescriptor.Transient<IInventory, Inventory>();
        var services = new ServiceCollection { catalog, inventory };
        var plan = new CompositionPlan(
            "catalog",
            [Entry(catalog, strategy: CompositionRegistrationStrategy.Replace)]
        );
        plan.ApplyTo(services);
        Assert.Collection(
            services,
            descriptor => Assert.Same(inventory, descriptor),
            descriptor => Assert.Same(catalog, descriptor)
        );
    }

    [Fact]
    public void Prior_plan_origins_are_retained_in_conflicts_and_replacement_reports()
    {
        var services = new ServiceCollection();
        CreatePlan().ApplyTo(services);
        var incoming = new CompositionOrigin("DeclareInventory", filePath: "Inventory.cs", line: 4);
        var conflicting = new CompositionPlan(
            "other",
            [
                new CompositionRegistration(
                    ServiceDescriptor.Transient<ICatalog, Inventory>(),
                    CompositionCardinality.One,
                    incoming
                ),
            ]
        );
        CompositionValidationException error = Assert.Throws<CompositionValidationException>(() =>
            conflicting.ApplyTo(services)
        );
        Assert.Equal(Origin, error.ExistingOrigin);
        Assert.Equal(incoming, error.Origin);
        var replacement = new CompositionPlan(
            "replacement",
            [
                new CompositionRegistration(
                    ServiceDescriptor.Transient<ICatalog, Inventory>(),
                    CompositionCardinality.One,
                    incoming,
                    CompositionRegistrationStrategy.Replace
                ),
            ]
        );
        var removed = Assert.Single(
            replacement.ApplyTo(services).Probe(),
            item => item.Outcome == CompositionApplicationOutcome.Replaced
        );
        Assert.Equal(Origin, removed.PreviousOrigin);
    }

    [Fact]
    public void Implementation_replacement_cannot_remove_a_previous_plans_only_service()
    {
        var services = new ServiceCollection();
        CreatePlan().ApplyTo(services);
        ServiceDescriptor original = services[0];
        var plan = new CompositionPlan(
            "inventory",
            [
                Entry(
                    ServiceDescriptor.Transient<IInventory, Catalog>(),
                    strategy: CompositionRegistrationStrategy.Replace,
                    replacement: CompositionReplacementBehavior.ImplementationType
                ),
            ]
        );
        Assert.Throws<CompositionValidationException>(() => plan.ApplyTo(services));
        Assert.Same(original, Assert.Single(services));
    }

    [Fact]
    public void Skipped_external_descriptor_retains_cardinality_without_inventing_its_origin()
    {
        var services = new ServiceCollection { ServiceDescriptor.Transient<ICatalog, Catalog>() };
        var skip = new CompositionPlan(
            "skipped",
            [
                Entry(
                    ServiceDescriptor.Transient<ICatalog, Inventory>(),
                    strategy: CompositionRegistrationStrategy.Skip
                ),
            ]
        );
        Assert.Null(Assert.Single(skip.ApplyTo(services).Probe()).PreviousOrigin);
        var many = new CompositionPlan(
            "many",
            [Entry(ServiceDescriptor.Transient<ICatalog, Inventory>(), CompositionCardinality.Many)]
        );
        Assert.Throws<CompositionValidationException>(() => many.ApplyTo(services));
        Assert.Single(services);
    }

    [Fact]
    public void Replacing_known_one_with_many_is_rejected()
    {
        var services = new ServiceCollection();
        CreatePlan().ApplyTo(services);
        var plan = new CompositionPlan(
            "inventory",
            [
                Entry(
                    ServiceDescriptor.Transient<ICatalog, Inventory>(),
                    CompositionCardinality.Many,
                    CompositionRegistrationStrategy.Replace
                ),
            ]
        );
        Assert.Throws<CompositionValidationException>(() => plan.ApplyTo(services));
        Assert.Equal(typeof(Catalog), Assert.Single(services).ImplementationType);
    }

    [Fact]
    public void Unrelated_keyed_service_does_not_satisfy_or_conflict_with_one()
    {
        var services = new ServiceCollection
        {
            ServiceDescriptor.KeyedTransient<ICatalog, Catalog>("local"),
        };
        CreatePlan().ApplyTo(services);
        Assert.Equal(2, services.Count);
    }

    [Fact]
    public void Plan_internal_ambiguity_is_rejected_before_any_collection_exists()
    {
        CompositionValidationException error = Assert.Throws<CompositionValidationException>(() =>
            new CompositionPlan(
                "catalog",
                [
                    Entry(ServiceDescriptor.Transient<ICatalog, Catalog>()),
                    Entry(ServiceDescriptor.Transient<ICatalog, Inventory>()),
                ]
            )
        );
        Assert.Equal(CompositionValidationPhase.PlanConstruction, error.Phase);
        Assert.Contains(nameof(Catalog), error.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(Inventory), error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Plan_internal_mixed_lifetimes_and_cardinalities_are_rejected()
    {
        Assert.Throws<CompositionValidationException>(() =>
            new CompositionPlan(
                "catalog",
                [
                    Entry(
                        ServiceDescriptor.Transient<ICatalog, Catalog>(),
                        CompositionCardinality.Many
                    ),
                    Entry(
                        ServiceDescriptor.Scoped<ICatalog, Inventory>(),
                        CompositionCardinality.Many
                    ),
                ]
            )
        );
        Assert.Throws<CompositionValidationException>(() =>
            new CompositionPlan(
                "catalog",
                [
                    Entry(ServiceDescriptor.Transient<ICatalog, Catalog>()),
                    Entry(
                        ServiceDescriptor.Transient<ICatalog, Inventory>(),
                        CompositionCardinality.Many
                    ),
                ]
            )
        );
    }

    [Fact]
    public void Factory_and_instance_duplicates_use_identity_without_inspecting_the_implementation()
    {
        Func<IServiceProvider, ICatalog> factory = _ =>
            throw new InvalidOperationException("Executed");
        Assert.Throws<CompositionValidationException>(() =>
            new CompositionPlan(
                "catalog",
                [
                    Entry(ServiceDescriptor.Transient(factory), CompositionCardinality.Many),
                    Entry(ServiceDescriptor.Transient(factory), CompositionCardinality.Many),
                ]
            )
        );
        using var catalog = new Catalog();
        Assert.Throws<CompositionValidationException>(() =>
            new CompositionPlan(
                "catalog",
                [
                    Entry(
                        ServiceDescriptor.Singleton<ICatalog>(catalog),
                        CompositionCardinality.Many
                    ),
                    Entry(
                        ServiceDescriptor.Singleton<ICatalog>(catalog),
                        CompositionCardinality.Many
                    ),
                ]
            )
        );
    }

    [Fact]
    public void Distinct_opaque_factories_can_be_appended_without_execution()
    {
        var plan = new CompositionPlan(
            "catalog",
            [
                Entry(
                    ServiceDescriptor.Transient<ICatalog>(_ =>
                        throw new InvalidOperationException("First")
                    ),
                    CompositionCardinality.Many
                ),
                Entry(
                    ServiceDescriptor.Transient<ICatalog>(_ =>
                        throw new InvalidOperationException("Second")
                    ),
                    CompositionCardinality.Many
                ),
            ]
        );
        var services = new ServiceCollection();
        plan.ApplyTo(services);
        Assert.Equal(2, services.Count);
    }

    [Fact]
    public void Missing_dependencies_are_left_to_final_provider_validation()
    {
        var plan = new CompositionPlan(
            "catalog",
            [Entry(ServiceDescriptor.Transient<ICatalog, CatalogWithDependency>())]
        );
        var services = new ServiceCollection();
        plan.ApplyTo(services);
        Assert.Throws<AggregateException>(() =>
            services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true })
        );
    }

    [Fact]
    public void Invalid_inputs_are_rejected_at_the_public_boundary()
    {
        Assert.Throws<ArgumentException>(() => new CompositionPlan(" ", []));
        Assert.Throws<ArgumentNullException>(() => new CompositionPlan("catalog", null!));
        Assert.Throws<ArgumentException>(() => new CompositionPlan("catalog", [null!]));
        Assert.Throws<ArgumentException>(() =>
            new CompositionCandidateRejection(typeof(Catalog), Origin, [])
        );
        Assert.Throws<ArgumentException>(() =>
            Entry(ServiceDescriptor.KeyedTransient<ICatalog, Catalog>("local"))
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            Entry(ServiceDescriptor.Transient<ICatalog, Catalog>(), (CompositionCardinality)42)
        );
        Assert.Throws<ArgumentOutOfRangeException>(() => new CompositionOrigin("catalog", line: 0));
    }

    private static CompositionPlan CreatePlan() =>
        new("catalog", [Entry(ServiceDescriptor.Transient<ICatalog, Catalog>())]);

    private static CompositionRegistration Entry(
        ServiceDescriptor descriptor,
        CompositionCardinality cardinality = CompositionCardinality.One,
        CompositionRegistrationStrategy strategy = CompositionRegistrationStrategy.Default,
        CompositionReplacementBehavior replacement = CompositionReplacementBehavior.ServiceType
    ) => new(descriptor, cardinality, Origin, strategy, replacement);

    public interface ICatalog;

    public interface IInventory;

    public sealed class Catalog : ICatalog, IInventory, IDisposable
    {
        public bool Disposed { get; private set; }

        public void Dispose() => Disposed = true;
    }

    public sealed class Inventory : ICatalog, IInventory;

    public sealed class CatalogWithDependency(IInventory inventory) : ICatalog
    {
        public IInventory Inventory { get; } = inventory;
    }
}
