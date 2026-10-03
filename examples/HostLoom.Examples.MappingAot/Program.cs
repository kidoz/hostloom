using HostLoom.Mapping;
using HostLoom.Mapping.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

// A Native AOT publish of this program must produce no trim or AOT warnings. The program checks
// its own results and exits with 1 on a mismatch, so running the native binary is the test: pair
// inference from the map class interfaces, resolution through the scoped dispatcher and the closed
// contracts, and the update map all work without reflection-based map dispatch.
var services = new ServiceCollection();
services.AddHostLoomMapping(mapping =>
    mapping.Add<ProductMapper>().AddUpdate<ProductUpdateMapper>()
);
var container = services.BuildServiceProvider(
    new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
);
await using var containerLifetime = container.ConfigureAwait(false);

using var scope = container.CreateScope();
var provider = scope.ServiceProvider;

var entity = new ProductEntity
{
    Id = 7,
    Name = "notebook",
    Region = "eu",
    Categories = { "books", "music" },
};

var problems = new List<string>();
var dispatcher = provider.GetRequiredService<IMapper>();
var mapped = dispatcher.From(entity).To<ProductModel>();
Expect(Matches(mapped), $"the scoped dispatcher maps through From().To<>(), got {mapped}");

var creation = provider.GetRequiredService<IMapper<ProductEntity, ProductModel>>();
var many = creation.MapMany([entity, entity]);
Expect(many.Count == 2 && many.All(Matches), $"MapMany maps every element, got {many.Count}");
Expect(
    creation.MapOrNull(null) is null
        && creation.MapOrNull(entity) is { } optional
        && Matches(optional),
    "MapOrNull keeps the null policy at the call site"
);

var identifier = entity.Id;
provider
    .GetRequiredService<IUpdateMapper<ProductUpdate, ProductEntity>>()
    .MapInto(new ProductUpdate("notebook pro", "us", ["books"]), entity);
Expect(
    entity.Id == identifier
        && entity.Name == "notebook pro"
        && entity.Region == "us"
        && entity.Categories is ["books"],
    $"the update map writes into the instance and keeps its identity, got {entity.Name} in {entity.Region}"
);

Console.WriteLine(
    problems.Count == 0
        ? "mapping verified: dispatcher, MapMany, MapOrNull, and update map all match"
        : "mapping mismatches: " + string.Join("; ", problems)
);
return problems.Count == 0 ? 0 : 1;

void Expect(bool condition, string what)
{
    if (!condition)
    {
        problems.Add(what);
    }
}

// Record equality on ProductModel compares the category list by reference, so the checks compare
// members and the sequence.
static bool Matches(ProductModel model) =>
    model.Id == 7
    && model.Name == "notebook"
    && model.Region == "eu"
    && model.Categories is ["books", "music"];

internal sealed class ProductEntity
{
    public required int Id { get; init; }

    public required string Name { get; set; }

    public required string Region { get; set; }

    public List<string> Categories { get; } = [];
}

internal sealed record ProductModel(
    int Id,
    string Name,
    string Region,
    IReadOnlyList<string> Categories
);

internal sealed record ProductUpdate(string Name, string Region, IReadOnlyList<string> Categories);

internal sealed class ProductMapper : IMapper<ProductEntity, ProductModel>
{
    public ProductModel Map(ProductEntity source) =>
        new(source.Id, source.Name, source.Region, [.. source.Categories]);
}

internal sealed class ProductUpdateMapper : IUpdateMapper<ProductUpdate, ProductEntity>
{
    public void MapInto(ProductUpdate source, ProductEntity destination)
    {
        destination.Name = source.Name;
        destination.Region = source.Region;
        destination.Categories.Clear();
        destination.Categories.AddRange(source.Categories);
    }
}
