using HostLoom.Composition;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed partial class CompositionAdvancedGeneratorTests
{
    [Theory]
    [InlineData(false, false, "ImplementationType")]
    [InlineData(true, false, "ImplementationType")]
    [InlineData(false, true, "ImplementationType")]
    [InlineData(true, true, "ImplementationType")]
    [InlineData(false, false, "All")]
    [InlineData(true, false, "All")]
    [InlineData(false, true, "All")]
    [InlineData(true, true, "All")]
    public void Capture_analysis_matches_descriptors_after_implementation_replacement(
        bool capturingLast,
        bool enumerable,
        string replacement
    )
    {
        const string safe =
            "rules.AddTypes(typeof(SafeSession)).As<ISession>().WithTransientLifetime().ExpectMany();";
        const string capturing =
            "rules.AddTypes(typeof(CapturingSession)).As<ISession>().WithTransientLifetime().ExpectMany();";
        string rules =
            (capturingLast ? safe + capturing : capturing + safe)
            + $"rules.AddTypes(typeof({(capturingLast ? "CapturingSession" : "SafeSession")})).As<IReplacement>().WithTransientLifetime().ExpectMany().Replace(CompositionReplacementBehavior.{replacement});"
            + "rules.AddTypes(typeof(Scope)).AsSelf().WithScopedLifetime().ExpectOne();";
        string types = $$"""
            public class Catalog({{(
                enumerable ? "System.Collections.Generic.IEnumerable<ISession>" : "ISession"
            )}} sessions) { public object Sessions { get; } = sessions; }
            public interface ISession {}
            public interface IReplacement {}
            public class SafeSession : ISession, IReplacement { public SafeSession() {} }
            public class CapturingSession(Scope scope) : ISession, IReplacement { public object Scope { get; } = scope; }
            public class Scope { public Scope() {} }
            """;
        AssertCaptureMatchesRuntime(rules, types, expectedCapture: !capturingLast);
    }

    [Theory]
    [InlineData("ServiceType")]
    [InlineData("All")]
    public void Capture_analysis_ignores_a_singleton_removed_by_service_replacement(
        string replacement
    ) =>
        AssertCaptureMatchesRuntime(
            $"""
            rules.AddTypes(typeof(ReplacementCatalog)).As<Catalog>().WithSingletonLifetime().ExpectMany().Replace(CompositionReplacementBehavior.{replacement});
            rules.AddTypes(typeof(Scope)).AsSelf().WithScopedLifetime().ExpectOne();
            """,
            """
            public class Catalog { public Catalog(Scope scope) {} }
            public class ReplacementCatalog : Catalog { public ReplacementCatalog() : base(null!) {} }
            public class Scope { public Scope() {} }
            """,
            expectedCapture: false,
            rootCardinality: "Many"
        );

    private static void AssertCaptureMatchesRuntime(
        string rules,
        string types,
        bool expectedCapture,
        string rootCardinality = "One"
    )
    {
        string root =
            $"rules.AddTypes(typeof(Catalog)).AsSelf().WithSingletonLifetime().Expect{rootCardinality}();";
        var (driver, output) = CompositionGeneratorHarness.Run(
            CompositionGeneratorHarness.Compilation(Fixture(root + rules, types))
        );
        if (expectedCapture)
            Assert.Single(driver.GetRunResult().Diagnostics, item => item.Id == "HLM0016");
        else
            CompositionGeneratorHarness.AssertSuccess(driver, output);

        // Compile with a transient root to obtain the same generated descriptors even when
        // HLM0016 prevents emitting the singleton plan. Restore its lifetime before validation.
        // Keep replacement lifetimes identical so raw-plan conflict checks remain applicable.
        var plan = Plan(
            (root + rules).Replace(
                "WithSingletonLifetime()",
                "WithTransientLifetime()",
                StringComparison.Ordinal
            ),
            types
        );
        var services = new ServiceCollection();
        plan.ApplyTo(services);
        for (var index = 0; index < services.Count; index++)
        {
            var entry = services[index];
            if (entry.ServiceType.Name == "Catalog")
                services[index] = new ServiceDescriptor(
                    entry.ServiceType,
                    entry.ImplementationType!,
                    ServiceLifetime.Singleton
                );
        }
        var options = new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true };
        if (expectedCapture)
            Assert.Throws<AggregateException>(() => services.BuildServiceProvider(options));
        else
        {
            using var provider = services.BuildServiceProvider(options);
            var catalog = services.Single(entry => entry.ServiceType.Name == "Catalog");
            Assert.NotNull(provider.GetRequiredService(catalog.ServiceType));
        }
    }
}
