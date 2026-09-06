using Microsoft.CodeAnalysis;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed partial class MappingCompletenessAnalyzerTests
{
    [Theory]
    [InlineData("new ProductModel(source.Name, source.City)", false)]
    [InlineData("new ProductModel(source.Name, source.City) { Mask = source.Mask }", true)]
    public async Task Derived_record_constructor_arguments_assign_inherited_members(
        string creation,
        bool complete
    )
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            $$"""
            public record NamedModel(string Name);
            public record ProductModel(string Name, string City) : NamedModel(Name)
            {
                public string Mask { get; init; }
            }

            public sealed class Mapper : IMapper<Source, ProductModel>
            {
                public ProductModel Map(Source source) => {{creation}};
            }
            """
        );

        if (complete)
        {
            Assert.Empty(diagnostics);
        }
        else
        {
            AssertMissingMembers(diagnostics, "Mask");
        }
    }

    [Theory]
    [InlineData("public string Name;")]
    [InlineData("public string Name { get; set; }")]
    public async Task Derived_class_constructor_arguments_assign_inherited_state(string member)
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            $$"""
            public class NamedModel
            {
                protected NamedModel(string name) { Name = name; }
                {{member}}
            }
            public sealed class ProductModel(string name) : NamedModel(name);
            public sealed class Mapper : IMapper<Source, ProductModel>
            {
                public ProductModel Map(Source source) => new ProductModel(source.Name);
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Theory]
    [InlineData("=> new ProductModel { Name = source.Name };", true)]
    [InlineData(
        "{ var model = new ProductModel(); model.Name = source.Name; return model; }",
        true
    )]
    [InlineData("=> new ProductModel();", false)]
    public async Task An_override_chain_is_one_required_property(string body, bool complete)
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            $$"""
            public abstract class NamedModel { public abstract string Name { get; set; } }
            public class VendorModel : NamedModel { public override string Name { get; set; } }
            public sealed class ProductModel : VendorModel { public override string Name { get; set; } }
            public sealed class Mapper : IMapper<Source, ProductModel>
            {
                public ProductModel Map(Source source) {{body}}
            }
            """
        );

        if (complete)
        {
            Assert.Empty(diagnostics);
        }
        else
        {
            AssertMissingMembers(diagnostics, "Name");
        }
    }

    [Theory]
    [InlineData("=> new ProductModel { Name = source.Name };")]
    [InlineData("{ var model = new ProductModel(); model.Name = source.Name; return model; }")]
    public async Task An_override_assignment_satisfies_a_base_destination_contract(string body)
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            $$"""
            public abstract class NamedModel { public abstract string Name { get; set; } }
            public sealed class ProductModel : NamedModel { public override string Name { get; set; } }
            public sealed class Mapper : IMapper<Source, NamedModel>
            {
                public NamedModel Map(Source source) {{body}}
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Theory]
    [InlineData("public interface IProductModel { string Name { get; set; } }", false)]
    [InlineData(
        "public interface INamedModel { string Name { get; set; } } public interface IProductModel : INamedModel { }",
        false
    )]
    [InlineData(
        "public interface INamedModel { string Name { get; set; } } public interface ILeft : INamedModel { } public interface IRight : INamedModel { } public interface IProductModel : ILeft, IRight { }",
        false
    )]
    [InlineData("public interface IProductModel { string Name { get; set; } }", true)]
    [InlineData(
        "public interface INamedModel { string Name { get; set; } } public interface IProductModel : INamedModel { }",
        true
    )]
    [InlineData(
        "public interface INamedModel { string Name { get; set; } } public interface ILeft : INamedModel { } public interface IRight : INamedModel { } public interface IProductModel : ILeft, IRight { }",
        true
    )]
    public async Task Interface_members_are_checked_across_the_inheritance_graph(
        string contract,
        bool complete
    )
    {
        string initializer = complete ? "{ Name = source.Name }" : "";
        Diagnostic[] diagnostics = await AnalyzeAsync(
            $$"""
            {{contract}}
            public sealed class ProductModel : IProductModel { public string Name { get; set; } }
            public sealed class Mapper : IMapper<Source, IProductModel>
            {
                public IProductModel Map(Source source) => new ProductModel() {{initializer}};
            }
            """
        );

        if (complete)
        {
            Assert.Empty(diagnostics);
        }
        else
        {
            AssertMissingMembers(diagnostics, "Name");
        }
    }

    [Theory]
    [InlineData("IProductModel")]
    [InlineData("ProductModel")]
    public async Task An_interface_destination_can_be_built_through_either_local_type(
        string localType
    )
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            $$"""
            public interface IProductModel { string Name { get; set; } }
            public sealed class ProductModel : IProductModel { public string Name { get; set; } }
            public sealed class Mapper : IMapper<Source, IProductModel>
            {
                public IProductModel Map(Source source)
                {
                    {{localType}} model = new ProductModel();
                    model.Name = source.Name;
                    return model;
                }
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task A_record_constructor_can_supply_an_interface_member()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public interface INamedModel { string Name { get; init; } }
            public interface IProductModel : INamedModel { }
            public sealed record ProductModel(string Name) : IProductModel;
            public sealed class Mapper : IMapper<Source, IProductModel>
            {
                public IProductModel Map(Source source) => new ProductModel(source.Name);
            }
            """
        );

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task A_same_named_property_cannot_satisfy_a_separate_explicit_interface_member()
    {
        Diagnostic[] diagnostics = await AnalyzeAsync(
            """
            public interface IProductModel { string Name { get; set; } }
            public sealed class ProductModel : IProductModel
            {
                public string Name { get; set; }
                string IProductModel.Name { get; set; }
            }
            public sealed class Mapper : IMapper<Source, IProductModel>
            {
                public IProductModel Map(Source source) => new ProductModel { Name = source.Name };
            }
            """
        );

        AssertMissingMembers(diagnostics, "Name");
    }
}
