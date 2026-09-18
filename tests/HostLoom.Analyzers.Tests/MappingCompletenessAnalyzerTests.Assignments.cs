using System.Globalization;
using HostLoom.Analyzers.Infrastructure;
using Microsoft.CodeAnalysis;
using Xunit;

namespace HostLoom.Analyzers.Tests;

/// <summary>
/// Assignment shapes beyond the plain <c>local.Member = value</c>: compound and coalescing
/// assignment, deconstruction, increments, nested member writes, fields, accessor visibility,
/// and the union across several returns.
/// </summary>
public sealed partial class MappingCompletenessAnalyzerTests
{
    // -- Assignment operators --------------------------------------------------------------------

    [Theory]
    [InlineData("destination.Mask += source.Mask;")]
    [InlineData("destination.Mask ??= source.Mask;")]
    public async Task A_compound_or_coalescing_assignment_counts_as_assigned(string statement)
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            $$"""
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source)
                {
                    var destination = new Destination { Name = source.Name, City = source.City };
                    {{statement}}
                    return destination;
                }
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task A_deconstruction_assignment_is_not_yet_counted_as_an_assignment()
    {
        // Pinned as observed, not as intended: the targets of a deconstruction sit under a tuple
        // rather than directly under an assignment, so the rule misses them and reports the map
        // as incomplete although every member is written. A false positive, not a data loss.
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source)
                {
                    var destination = new Destination { Name = source.Name };
                    (destination.City, destination.Mask) = (source.City, source.Mask);
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
            "City, Mask",
            diagnostic.GetMessage(CultureInfo.InvariantCulture),
            StringComparison.Ordinal
        );
    }

    [Fact]
    public async Task A_chained_assignment_counts_every_target()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source)
                {
                    var destination = new Destination { Name = source.Name };
                    destination.City = destination.Mask = source.City;
                    return destination;
                }
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task Writing_into_a_nested_member_does_not_assign_the_containing_member()
    {
        // destination.Address.City = ... reads Address and writes City on whatever Address holds.
        // Address itself was never assigned, which is exactly the forgotten member the rule is for.
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Place { public string City { get; set; } = ""; }
            public sealed class Nested
            {
                public string Name { get; set; } = "";
                public Place Address { get; set; } = new();
            }

            public sealed class Mapper : IMapper<Source, Nested>
            {
                public Nested Map(Source source)
                {
                    var destination = new Nested { Name = source.Name };
                    destination.Address.City = source.City;
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
            "Address",
            diagnostic.GetMessage(CultureInfo.InvariantCulture),
            StringComparison.Ordinal
        );
    }

    [Fact]
    public async Task A_nested_object_initializer_does_not_assign_the_containing_member()
    {
        // `Address = { City = ... }` is a member initializer: it mutates the Address the
        // destination already holds rather than assigning Address.
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Place { public string City { get; set; } = ""; }
            public sealed class Nested
            {
                public string Name { get; set; } = "";
                public Place Address { get; set; } = new();
            }

            public sealed class Mapper : IMapper<Source, Nested>
            {
                public Nested Map(Source source) =>
                    new Nested { Name = source.Name, Address = { City = source.City } };
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(
            HostLoomDiagnosticDescriptors.UnassignedDestinationMemberDiagnosticId,
            diagnostic.Id
        );
        Assert.Contains(
            "Address",
            diagnostic.GetMessage(CultureInfo.InvariantCulture),
            StringComparison.Ordinal
        );
    }

    // -- Statement contexts -----------------------------------------------------------------------

    [Fact]
    public async Task Assignments_inside_try_using_and_loop_blocks_count()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source)
                {
                    var destination = new Destination();
                    try
                    {
                        destination.Name = source.Name;
                    }
                    finally
                    {
                        destination.City = source.City;
                    }
                    foreach (var _ in new[] { 1 })
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
    public async Task A_local_declared_without_a_new_destination_is_not_verifiable()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source)
                {
                    Destination destination;
                    destination = new Destination { Name = source.Name, City = source.City, Mask = source.Mask };
                    return destination;
                }
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(HostLoomDiagnosticDescriptors.MappingNotVerifiableDiagnosticId, diagnostic.Id);
    }

    [Fact]
    public async Task A_local_passed_by_reference_escapes()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source)
                {
                    var destination = new Destination { Name = source.Name, City = source.City, Mask = source.Mask };
                    Fill(ref destination);
                    return destination;
                }

                private static void Fill(ref Destination destination) { }
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(HostLoomDiagnosticDescriptors.MappingNotVerifiableDiagnosticId, diagnostic.Id);
    }

    // -- Return shapes ----------------------------------------------------------------------------

    [Fact]
    public async Task Several_constructed_returns_are_checked_as_their_union()
    {
        // Documented limit: the rule targets forgotten members, not per-path completeness. Each
        // branch here omits a member the other assigns, and the union covers all three.
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source)
                {
                    if (source.Name.Length == 0)
                    {
                        return new Destination { Name = source.Name, City = source.City };
                    }

                    return new Destination { Name = source.Name, Mask = source.Mask };
                }
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task Several_constructed_returns_still_report_a_member_no_path_assigns()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source)
                {
                    if (source.Name.Length == 0)
                    {
                        return new Destination { Name = source.Name, City = source.City };
                    }

                    return new Destination { Name = source.Name };
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

    [Theory]
    [InlineData(
        "source.Name.Length == 0 ? new Destination { Name = \"\", City = \"\", Mask = \"\" } : new Destination { Name = \"\", City = \"\", Mask = \"\" }"
    )]
    [InlineData(
        "source.Name switch { \"\" => new Destination { Name = \"\", City = \"\", Mask = \"\" }, _ => new Destination { Name = \"\", City = \"\", Mask = \"\" } }"
    )]
    public async Task A_conditional_or_switch_expression_over_creations_is_not_verifiable(
        string expression
    )
    {
        // Only a creation returned directly is shape A. Wrapping two complete creations in a
        // conditional drops to "not verifiable" rather than being silently accepted.
        Diagnostic[] diagnostics = await AnalyzeAsync(
            $$"""
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source) => {{expression}};
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(HostLoomDiagnosticDescriptors.MappingNotVerifiableDiagnosticId, diagnostic.Id);
    }

    [Fact]
    public async Task A_with_expression_is_a_destination_from_elsewhere()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed record Snapshot
            {
                public string Name { get; init; } = "";
                public string City { get; init; } = "";
            }

            public sealed class Mapper : IMapper<Snapshot, Snapshot>
            {
                public Snapshot Map(Snapshot source) => source with { Name = source.Name.Trim() };
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(HostLoomDiagnosticDescriptors.MappingNotVerifiableDiagnosticId, diagnostic.Id);
    }

    [Fact]
    public async Task A_target_typed_new_is_a_constructed_return()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Mapper : IMapper<Source, Destination>
            {
                public Destination Map(Source source) =>
                    new() { Name = source.Name, City = source.City };
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

    // -- Which members are required ---------------------------------------------------------------

    [Fact]
    public async Task An_init_only_property_is_required_and_satisfied_by_an_initializer()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Frozen
            {
                public string Name { get; init; } = "";
                public string City { get; init; } = "";
            }

            public sealed class Complete : IMapper<Source, Frozen>
            {
                public Frozen Map(Source source) => new() { Name = source.Name, City = source.City };
            }

            public sealed class Partial : IMapper<Source, Frozen>
            {
                public Frozen Map(Source source) => new() { Name = source.Name };
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(
            HostLoomDiagnosticDescriptors.UnassignedDestinationMemberDiagnosticId,
            diagnostic.Id
        );
        Assert.Contains(
            "City",
            diagnostic.GetMessage(CultureInfo.InvariantCulture),
            StringComparison.Ordinal
        );
    }

    [Fact]
    public async Task A_public_field_is_required_and_a_readonly_or_const_one_is_not()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Bag
            {
                public string Name = "";
                public readonly string Fixed = "";
                public const string Kind = "bag";
                public static string Shared = "";
            }

            public sealed class Mapper : IMapper<Source, Bag>
            {
                public Bag Map(Source source) => new();
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        string message = diagnostic.GetMessage(CultureInfo.InvariantCulture);
        Assert.Contains("Name", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Fixed", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Kind", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Shared", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Only_a_publicly_settable_property_is_required()
    {
        // Private, protected, and internal setters are outside the public contract the rule
        // guards; a static or indexer member is never a mapped value.
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Guarded
            {
                public string Name { get; set; } = "";
                public string Hidden { get; private set; } = "";
                public string Family { get; protected set; } = "";
                public string Local { get; internal set; } = "";
                public string ReadOnly { get; } = "";
                public static string Shared { get; set; } = "";
                public string this[int index] { get => ""; set { } }
            }

            public sealed class Mapper : IMapper<Source, Guarded>
            {
                public Guarded Map(Source source) => new();
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        string message = diagnostic.GetMessage(CultureInfo.InvariantCulture);
        Assert.Contains("Name", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Hidden", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Family", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Local", message, StringComparison.Ordinal);
        Assert.DoesNotContain("ReadOnly", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Shared", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_struct_destination_is_checked_like_a_class()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public struct Point
            {
                public int X { get; set; }
                public int Y { get; set; }
            }

            public sealed class Mapper : IMapper<Source, Point>
            {
                public Point Map(Source source) => new() { X = source.Name.Length };
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(
            HostLoomDiagnosticDescriptors.UnassignedDestinationMemberDiagnosticId,
            diagnostic.Id
        );
        Assert.Contains(
            "Y",
            diagnostic.GetMessage(CultureInfo.InvariantCulture),
            StringComparison.Ordinal
        );
    }

    [Fact]
    public async Task A_constructor_parameter_matches_its_member_case_insensitively()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Built
            {
                public Built(string name, string city) { Name = name; City = city; }
                public string Name { get; set; }
                public string City { get; set; }
                public string Mask { get; set; } = "";
            }

            public sealed class Mapper : IMapper<Source, Built>
            {
                public Built Map(Source source) => new(source.Name, source.City);
            }
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        string message = diagnostic.GetMessage(CultureInfo.InvariantCulture);
        Assert.Contains("Mask", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Name", message, StringComparison.Ordinal);
        Assert.DoesNotContain("City", message, StringComparison.Ordinal);
    }

    // -- Opt-out and map discovery ----------------------------------------------------------------

    [Fact]
    public async Task An_opt_out_naming_a_member_that_does_not_exist_excuses_nothing()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            [UnmappedMembers("Masq")]
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

    [Fact]
    public async Task A_map_inherited_from_a_base_class_is_checked_where_it_is_written()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public abstract class MapperBase : IMapper<Source, Destination>
            {
                public Destination Map(Source source) =>
                    new Destination { Name = source.Name, City = source.City };
            }

            public sealed class Mapper : MapperBase;
            """
        );

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal(
            HostLoomDiagnosticDescriptors.UnassignedDestinationMemberDiagnosticId,
            diagnostic.Id
        );
    }

    [Fact]
    public async Task A_class_implementing_two_pairs_is_checked_once_per_map()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public sealed class Other
            {
                public string Name { get; set; } = "";
                public string Extra { get; set; } = "";
            }

            public sealed class Mapper : IMapper<Source, Destination>, IMapper<Source, Other>
            {
                Destination IMapper<Source, Destination>.Map(Source source) =>
                    new Destination { Name = source.Name, City = source.City };

                Other IMapper<Source, Other>.Map(Source source) => new Other { Name = source.Name };
            }
            """
        );

        Assert.Equal(2, diagnostics.Length);
        Assert.All(
            diagnostics,
            diagnostic =>
                Assert.Equal(
                    HostLoomDiagnosticDescriptors.UnassignedDestinationMemberDiagnosticId,
                    diagnostic.Id
                )
        );
        Assert.Contains(
            diagnostics,
            diagnostic =>
                diagnostic
                    .GetMessage(CultureInfo.InvariantCulture)
                    .Contains("Mask", StringComparison.Ordinal)
        );
        Assert.Contains(
            diagnostics,
            diagnostic =>
                diagnostic
                    .GetMessage(CultureInfo.InvariantCulture)
                    .Contains("Extra", StringComparison.Ordinal)
        );
    }
}
