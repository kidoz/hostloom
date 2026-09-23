using HostLoom.Composition;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed partial class CompositionAdvancedGeneratorTests
{
    private const string PolicyTypes = """
        public interface ICatalog { }
        public interface IInventory { }
        public class Catalog : ICatalog, IInventory { public Catalog() { } }
        public class Inventory : ICatalog { public Inventory() { } }
        public class ExternalAttribute : System.Attribute { }
        [External] public class Archive : ICatalog, IInventory { public Archive() { } }
        """;

    // Two implementations of one service, and one implementation of two services.
    private const string ManyImplementations =
        "rules.AddClasses().AssignableTo<ICatalog>().WithoutAttribute<ExternalAttribute>().AsImplementedInterfaces().WithScopedLifetime().ExpectMany()";
    private const string ManyServices =
        "rules.AddTypes(typeof(Catalog)).As(typeof(ICatalog), typeof(IInventory)).WithScopedLifetime().ExpectOne()";

    [Theory]
    [InlineData(ManyImplementations, "Throw()")]
    [InlineData(ManyImplementations, "Skip()")]
    [InlineData(ManyImplementations, "Replace(CompositionReplacementBehavior.ServiceType)")]
    [InlineData(ManyImplementations, "Replace(CompositionReplacementBehavior.ImplementationType)")]
    [InlineData(ManyImplementations, "Replace(CompositionReplacementBehavior.All)")]
    [InlineData(ManyServices, "Throw()")]
    [InlineData(ManyServices, "Skip()")]
    [InlineData(ManyServices, "Replace(CompositionReplacementBehavior.ServiceType)")]
    [InlineData(ManyServices, "Replace(CompositionReplacementBehavior.ImplementationType)")]
    [InlineData(ManyServices, "Replace(CompositionReplacementBehavior.All)")]
    public void Rule_policies_register_every_entry_of_the_rule_into_an_empty_collection(
        string rule,
        string policy
    )
    {
        var plan = Plan($"{rule}.{policy};", PolicyTypes);
        var entries = plan.Probe().Registrations;
        Assert.Equal(2, entries.Count);
        var services = new ServiceCollection();

        var report = plan.ApplyTo(services).Probe();

        ServiceDescriptor[] descriptors = entries
            .Select(static entry => entry.Descriptor)
            .ToArray();
        Assert.Equal(descriptors, services.ToArray());
        Assert.Equal(descriptors, report.Select(static decision => decision.Descriptor).ToArray());
        Assert.All(
            report,
            decision =>
            {
                Assert.Equal(CompositionApplicationOutcome.Added, decision.Outcome);
                Assert.Same(entries[0].Origin, decision.Origin);
            }
        );
        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
        );
        using var scope = provider.CreateScope();
        foreach (var entry in entries)
            Assert.Contains(
                scope.ServiceProvider.GetServices(entry.Descriptor.ServiceType),
                service => service!.GetType() == entry.ImplementationType
            );
    }

    [Theory]
    [InlineData(ManyImplementations, "Throw()", "ICatalog:Archive IInventory:Catalog", null, null)]
    [InlineData(
        ManyImplementations,
        "Skip()",
        "ICatalog:Archive IInventory:Catalog",
        "ICatalog:Archive IInventory:Catalog",
        "Skipped ICatalog:Catalog; Skipped ICatalog:Inventory"
    )]
    [InlineData(
        ManyImplementations,
        "Replace(CompositionReplacementBehavior.ServiceType)",
        "ICatalog:Archive IInventory:Catalog",
        "IInventory:Catalog ICatalog:Catalog ICatalog:Inventory",
        "Replaced ICatalog:Archive; Added ICatalog:Catalog; Added ICatalog:Inventory"
    )]
    [InlineData(
        ManyImplementations,
        "Replace(CompositionReplacementBehavior.ImplementationType)",
        "ICatalog:Archive IInventory:Catalog",
        "ICatalog:Archive ICatalog:Catalog ICatalog:Inventory",
        "Replaced IInventory:Catalog; Added ICatalog:Catalog; Added ICatalog:Inventory"
    )]
    [InlineData(
        ManyImplementations,
        "Replace(CompositionReplacementBehavior.All)",
        "ICatalog:Archive IInventory:Catalog",
        "ICatalog:Catalog ICatalog:Inventory",
        "Replaced ICatalog:Archive; Replaced IInventory:Catalog; Added ICatalog:Catalog; Added ICatalog:Inventory"
    )]
    [InlineData(ManyServices, "Throw()", "IInventory:Archive", null, null)]
    [InlineData(
        ManyServices,
        "Skip()",
        "IInventory:Archive",
        "IInventory:Archive ICatalog:Catalog",
        "Added ICatalog:Catalog; Skipped IInventory:Catalog"
    )]
    [InlineData(
        ManyServices,
        "Replace(CompositionReplacementBehavior.ServiceType)",
        "ICatalog:Archive IInventory:Archive",
        "ICatalog:Catalog IInventory:Catalog",
        "Replaced ICatalog:Archive; Added ICatalog:Catalog; Replaced IInventory:Archive; Added IInventory:Catalog"
    )]
    [InlineData(
        ManyServices,
        "Replace(CompositionReplacementBehavior.ImplementationType)",
        "ICatalog:Catalog",
        "ICatalog:Catalog IInventory:Catalog",
        "Replaced ICatalog:Catalog; Added ICatalog:Catalog; Added IInventory:Catalog"
    )]
    [InlineData(
        ManyServices,
        "Replace(CompositionReplacementBehavior.All)",
        "ICatalog:Catalog IInventory:Archive",
        "ICatalog:Catalog IInventory:Catalog",
        "Replaced ICatalog:Catalog; Added ICatalog:Catalog; Replaced IInventory:Archive; Added IInventory:Catalog"
    )]
    public void Rule_policies_collide_only_with_registrations_that_existed_before_the_rule(
        string rule,
        string policy,
        string existing,
        string? expectedServices,
        string? expectedDecisions
    )
    {
        var plan = Plan($"{rule}.{policy};", PolicyTypes);
        var entries = plan.Probe().Registrations;
        var assembly = entries[0].Descriptor.ServiceType.Assembly;
        ServiceDescriptor[] prior = existing
            .Split(' ')
            .Select(pair => pair.Split(':'))
            .Select(pair => new ServiceDescriptor(
                assembly.GetType(pair[0], throwOnError: true)!,
                assembly.GetType(pair[1], throwOnError: true)!,
                ServiceLifetime.Scoped
            ))
            .ToArray();
        IServiceCollection services = new ServiceCollection();
        foreach (var descriptor in prior)
            services.Add(descriptor);

        if (expectedServices is null)
        {
            var error = Assert.Throws<CompositionValidationException>(() => plan.ApplyTo(services));
            Assert.Equal(CompositionValidationPhase.Application, error.Phase);
            Assert.Contains(
                "collection index 0: 'Archive', Scoped, origin 'external descriptor'",
                error.Message,
                StringComparison.Ordinal
            );
            Assert.Same(entries[0].Origin, error.Origin);
            Assert.Null(error.ExistingOrigin);
            Assert.Equal(prior, services.ToArray());
            return;
        }

        var report = plan.ApplyTo(services).Probe();

        Assert.Equal(expectedServices, string.Join(" ", services.Select(Describe)));
        Assert.Equal(
            expectedDecisions,
            string.Join(
                "; ",
                report.Select(decision => $"{decision.Outcome} {Describe(decision.Descriptor)}")
            )
        );
        foreach (var decision in report)
        {
            Assert.Same(entries[0].Origin, decision.Origin);
            Assert.Null(decision.PreviousOrigin);
            if (decision.Outcome == CompositionApplicationOutcome.Replaced)
            {
                Assert.Contains(decision.Descriptor, prior);
                Assert.DoesNotContain(decision.Descriptor, services);
                continue;
            }
            Assert.Contains(
                entries,
                entry => ReferenceEquals(entry.Descriptor, decision.Descriptor)
            );
            if (decision.Outcome == CompositionApplicationOutcome.Skipped)
                Assert.Equal(
                    $"Kept existing service at collection index {Array.FindIndex(prior, descriptor => descriptor.ServiceType == decision.Descriptor.ServiceType)}.",
                    decision.Reason
                );
        }
    }

    [Theory]
    [InlineData("Throw()", null)]
    [InlineData("Skip()", "Added ICatalog:Inventory; Skipped ICatalog:Catalog")]
    [InlineData(
        "Replace(CompositionReplacementBehavior.ServiceType)",
        "Added ICatalog:Inventory; Replaced ICatalog:Inventory; Added ICatalog:Catalog"
    )]
    public void Rule_policies_still_collide_with_earlier_rules_of_the_same_plan(
        string policy,
        string? expectedDecisions
    )
    {
        var plan = Plan(
            $"""
            rules.AddTypes(typeof(Inventory)).As<ICatalog>().WithScopedLifetime().ExpectMany();
            rules.AddTypes(typeof(Catalog)).As<ICatalog>().WithScopedLifetime().ExpectMany().{policy};
            """,
            PolicyTypes
        );
        var entries = plan.Probe().Registrations;
        Assert.Equal(2, entries.Count);
        Assert.NotEqual(entries[0].Origin, entries[1].Origin);
        var services = new ServiceCollection();

        if (expectedDecisions is null)
        {
            var error = Assert.Throws<CompositionValidationException>(() => plan.ApplyTo(services));
            Assert.Contains(
                "collection index 0: 'Inventory'",
                error.Message,
                StringComparison.Ordinal
            );
            Assert.Same(entries[1].Origin, error.Origin);
            Assert.Same(entries[0].Origin, error.ExistingOrigin);
            Assert.Empty(services);
            return;
        }

        var report = plan.ApplyTo(services).Probe();

        Assert.Equal(
            expectedDecisions,
            string.Join(
                "; ",
                report.Select(decision => $"{decision.Outcome} {Describe(decision.Descriptor)}")
            )
        );
        var collision = report[1];
        Assert.Same(entries[1].Origin, collision.Origin);
        Assert.Same(entries[0].Origin, collision.PreviousOrigin);
        Assert.Equal(
            policy == "Skip()" ? entries[0].Descriptor : entries[1].Descriptor,
            Assert.Single(services)
        );
    }

    private static string Describe(ServiceDescriptor descriptor) =>
        $"{descriptor.ServiceType.Name}:{descriptor.ImplementationType!.Name}";
}
