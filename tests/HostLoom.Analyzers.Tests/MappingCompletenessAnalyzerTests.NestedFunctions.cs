using System.Globalization;
using HostLoom.Analyzers.Infrastructure;
using Microsoft.CodeAnalysis;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed partial class MappingCompletenessAnalyzerTests
{
    [Theory]
    [InlineData("value => value.ToUpperInvariant()", false)]
    [InlineData("value => { return value.ToUpperInvariant(); }", false)]
    [InlineData("delegate(string value) { return value.ToUpperInvariant(); }", false)]
    [InlineData("value => value.ToUpperInvariant()", true)]
    public async Task Nested_lambda_returns_do_not_change_the_map_body_shape(
        string selector,
        bool statementForm
    )
    {
        string creation = $$"""
            new Destination
            {
                Name = string.Concat(System.Linq.Enumerable.Select(new[] { source.Name }, {{selector}})),
                City = source.City,
                Mask = source.Mask
            }
            """;
        string body = statementForm
            ? $"{{ var destination = {creation}; return destination; }}"
            : $"=> {creation};";
        Diagnostic[] diagnostics = await AnalyzeAsync(
            $$"""
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source) {{body}}
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Theory]
    [InlineData("Destination Unused() => new Destination { Mask = source.Mask };")]
    [InlineData("Func<Destination> unused = () => new Destination { Mask = source.Mask };")]
    [InlineData(
        "Func<Destination> unused = delegate { return new Destination { Mask = source.Mask }; };"
    )]
    public async Task A_nested_function_cannot_supply_members_of_the_returned_destination(
        string nestedFunction
    )
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            $$"""
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source)
                {
                    {{nestedFunction}}
                    return new Destination { Name = source.Name, City = source.City };
                }
            }
            """
        );

        AssertMissingMembers(diagnostics, "Mask");
    }

    [Fact]
    public async Task A_nested_destination_local_does_not_make_the_returned_local_ambiguous()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source)
                {
                    Destination Unused()
                    {
                        var other = new Destination();
                        return other;
                    }

                    var destination = new Destination();
                    destination.Name = source.Name;
                    destination.City = source.City;
                    destination.Mask = source.Mask;
                    return destination;
                }
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Theory]
    [InlineData("void Fill() { destination.Mask = source.Mask; }", "Fill();")]
    [InlineData("Action fill = () => destination.Mask = source.Mask;", "fill();")]
    [InlineData("void Fill() { destination.Mask = source.Mask; }", "")]
    public async Task A_destination_captured_by_a_nested_function_is_not_verifiable(
        string nestedFunction,
        string invocation
    )
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            $$"""
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source)
                {
                    var destination = new Destination { Name = source.Name, City = source.City };
                    {{nestedFunction}}
                    {{invocation}}
                    return destination;
                }
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(HostLoomDiagnosticDescriptors.MappingNotVerifiableDiagnosticId, diagnostic.Id);
    }

    private static void AssertMissingMembers(Diagnostic[] diagnostics, string members)
    {
        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(
            HostLoomDiagnosticDescriptors.UnassignedDestinationMemberDiagnosticId,
            diagnostic.Id
        );
        Assert.Contains(
            $"never assigns {members};",
            diagnostic.GetMessage(CultureInfo.InvariantCulture),
            StringComparison.Ordinal
        );
    }
}
