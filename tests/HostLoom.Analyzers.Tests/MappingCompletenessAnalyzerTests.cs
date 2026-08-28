using System.Globalization;
using HostLoom.Analyzers.Infrastructure;
using HostLoom.Analyzers.Usage;
using Microsoft.CodeAnalysis;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed class MappingCompletenessAnalyzerTests
{
    private const string Contracts = """
        using System;
        using System.Collections.Generic;
        using HostLoom.Mapping;

        public sealed class Source
        {
            public string Name { get; set; } = "";
            public string City { get; set; } = "";
            public string Mask { get; set; } = "";
        }

        public sealed class Destination
        {
            public string Name { get; set; } = "";
            public string City { get; set; } = "";
            public string Mask { get; set; } = "";
        }
        """;

    // -- Verified: shape A, the destination returned directly -----------------------------------

    [Fact]
    public async Task An_object_initializer_assigning_every_member_is_accepted()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source) =>
                    new Destination { Name = source.Name, City = source.City, Mask = source.Mask };
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task An_object_initializer_missing_a_member_names_it()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source) =>
                    new Destination { Name = source.Name, City = source.City };
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(
            HostLoomDiagnosticDescriptors.UnassignedDestinationMemberDiagnosticId,
            diagnostic.Id
        );
        Assert.Contains(
            "Mask",
            diagnostic.GetMessage(CultureInfo.InvariantCulture),
            StringComparison.Ordinal
        );
    }

    // -- Verified: shape B, construct then assign ----------------------------------------------

    [Fact]
    public async Task A_statement_body_assigning_every_member_is_accepted()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source)
                {
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

    [Fact]
    public async Task A_statement_body_missing_a_member_names_it()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source)
                {
                    var destination = new Destination();
                    destination.Name = source.Name;
                    destination.City = source.City;
                    return destination;
                }
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(
            HostLoomDiagnosticDescriptors.UnassignedDestinationMemberDiagnosticId,
            diagnostic.Id
        );
        Assert.Contains(
            "Mask",
            diagnostic.GetMessage(CultureInfo.InvariantCulture),
            StringComparison.Ordinal
        );
    }

    [Fact]
    public async Task A_member_assigned_on_only_one_branch_counts_as_assigned()
    {
        // The target is a forgotten member. Evidence the author considered one is the signal, so
        // a conditional assignment is deliberate and not reported.
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source)
                {
                    var destination = new Destination();
                    destination.Name = source.Name;
                    destination.City = source.City;
                    if (source.Mask.Length > 0)
                    {
                        destination.Mask = source.Mask;
                    }

                    return destination;
                }
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task Reading_a_member_of_the_local_is_not_an_assignment()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source)
                {
                    var destination = new Destination();
                    destination.Name = source.Name;
                    destination.City = destination.Name;
                    return destination;
                }
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Contains(
            "Mask",
            diagnostic.GetMessage(CultureInfo.InvariantCulture),
            StringComparison.Ordinal
        );
    }

    // -- Not verifiable -------------------------------------------------------------------------

    [Fact]
    public async Task A_local_passed_to_a_method_drops_to_not_verifiable()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source)
                {
                    var destination = new Destination();
                    destination.Name = source.Name;
                    Fill(destination);
                    return destination;
                }

                private static void Fill(Destination destination) { }
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(HostLoomDiagnosticDescriptors.MappingNotVerifiableDiagnosticId, diagnostic.Id);
    }

    [Fact]
    public async Task Two_destination_locals_drop_to_not_verifiable()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source)
                {
                    var first = new Destination();
                    var second = new Destination();
                    first.Name = source.Name;
                    return source.Name.Length > 0 ? first : second;
                }
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(HostLoomDiagnosticDescriptors.MappingNotVerifiableDiagnosticId, diagnostic.Id);
    }

    [Fact]
    public async Task A_destination_from_elsewhere_drops_to_not_verifiable()
    {
        // Nothing was constructed here, so there is no assignment set to compare against.
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source) => Cached;

                private static Destination Cached { get; } = new Destination();
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(HostLoomDiagnosticDescriptors.MappingNotVerifiableDiagnosticId, diagnostic.Id);
    }

    // -- Not applicable ---------------------------------------------------------------------------

    [Fact]
    public async Task A_destination_with_no_settable_members_is_not_applicable()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Immutable
            {
                public Immutable(string name) => Name = name;

                public string Name { get; }
            }

            public sealed class Mapper : IMapper<Source, Immutable>
            {
                public Immutable Map(Source source) => new Immutable(source.Name);
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task A_constructor_argument_counts_as_assigning_its_member()
    {
        // A record's positional members have init setters, so they are settable and would
        // otherwise be reported despite being supplied at construction.
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed record Positional(string Name, string City);

            public sealed class Mapper : IMapper<Source, Positional>
            {
                public Positional Map(Source source) => new Positional(source.Name, source.City);
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task A_sequence_destination_is_not_applicable()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, List<string>>
            {
                public List<string> Map(Source source) => new List<string> { source.Name };
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    // -- The named opt-out ------------------------------------------------------------------------

    [Fact]
    public async Task A_named_unmapped_member_is_excused()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            [UnmappedMembers(nameof(Destination.Mask))]
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source) =>
                    new Destination { Name = source.Name, City = source.City };
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task An_opt_out_excuses_only_the_members_it_names()
    {
        // The point of naming them: a member added to the contract later is still reported,
        // where a blanket "incomplete on purpose" marker would have excused it too.
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            [UnmappedMembers(nameof(Destination.Mask))]
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source) => new Destination { Name = source.Name };
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Contains(
            "City",
            diagnostic.GetMessage(CultureInfo.InvariantCulture),
            StringComparison.Ordinal
        );
        Assert.DoesNotContain(
            "Mask",
            diagnostic.GetMessage(CultureInfo.InvariantCulture),
            StringComparison.Ordinal
        );
    }

    // -- Scope ------------------------------------------------------------------------------------

    [Fact]
    public async Task A_method_named_Map_outside_a_mapper_is_ignored()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class NotAMapper
            {
                public Destination Map(Source source) => new Destination { Name = source.Name };
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    private static Task<Diagnostic[]> AnalyzeAsync(string mapper) =>
        AnalyzerTestHarness.AnalyzeAsync(
            Contracts + Environment.NewLine + mapper,
            new MappingCompletenessAnalyzer()
        );
}
