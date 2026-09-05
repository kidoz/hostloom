using HostLoom.Composition;
using HostLoom.Composition.Testing;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed class CompositionTestingGeneratorTests
{
    [Theory]
    [InlineData("AsImplementedInterfaces")]
    [InlineData("AsSelfWithInterfaces")]
    public void Explicit_and_discovered_inherited_types_have_equal_ordered_semantics(
        string projection
    )
    {
        string source = $$"""
            using HostLoom.Composition;
            public static partial class CatalogComposition
            {
                [CompositionRules(nameof(CreatePlan))]
                private static void Discover(CompositionRuleBuilder rules)
                {
                    rules.AddClasses().AssignableTo(typeof(ICatalog<>)).{{projection}}().WithScopedLifetime().ExpectOne();
                }
                [CompositionRules(nameof(CreateExplicit))]
                private static void Explicit(CompositionRuleBuilder rules)
                {
                    rules.AddTypes(typeof(Inventory), typeof(Catalog)).AssignableTo(typeof(ICatalog<>)).{{projection}}().WithScopedLifetime().ExpectOne();
                }
                public static partial CompositionPlan CreatePlan();
                public static partial CompositionPlan CreateExplicit();
            }
            public interface ICatalog<T> { }
            public abstract class CatalogBase<T> : ICatalog<T> { }
            public class Catalog : CatalogBase<string> { public Catalog() { } }
            public class Inventory : CatalogBase<int> { public Inventory() { } }
            """;
        var (driver, output) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(source)
        );
        CompositionGeneratorHarness.AssertSuccess(driver, output);
        CompositionPlan discovered = CompositionGeneratorHarness.LoadPlan(output);
        var assembly = discovered.Probe().Registrations[0].ImplementationType!.Assembly;
        var explicitPlan = Assert.IsType<CompositionPlan>(
            assembly.GetType("CatalogComposition")!.GetMethod("CreateExplicit")!.Invoke(null, null)
        );
        CompositionAssert.EquivalentRegistrations(explicitPlan, discovered);
        CompositionAssert.RegistrationSequence(explicitPlan, discovered);
        Assert.NotEmpty(discovered.Probe().RejectedCandidates);
        Assert.Empty(explicitPlan.Probe().RejectedCandidates);
        Assert.Throws<CompositionAssertionException>(() =>
            CompositionAssert.Origins(
                discovered.Probe(),
                explicitPlan.Probe().Registrations.Select(static entry => entry.Origin).ToArray()
            )
        );
    }
}
