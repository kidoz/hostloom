using HostLoom.Composition;
using Microsoft.Extensions.DependencyInjection;

var origin = new CompositionOrigin("DeclareCatalog", "catalog");
var plan = new CompositionPlan(
    "CompositionAot.Catalog",
    [
        new(
            ServiceDescriptor.Scoped<CatalogSession, CatalogSession>(),
            CompositionCardinality.One,
            origin
        ),
        new(
            ServiceDescriptor.Scoped<ICatalogSession>(provider =>
                provider.GetRequiredService<CatalogSession>()
            ),
            CompositionCardinality.One,
            origin
        ),
        new(
            new ServiceDescriptor(
                typeof(IRepository<>),
                typeof(Repository<>),
                ServiceLifetime.Scoped
            ),
            CompositionCardinality.One,
            origin
        ),
    ]
);

if (plan.Probe().Registrations.Count != 3)
{
    throw new InvalidOperationException("The plan inventory is incomplete.");
}
var services = new ServiceCollection();
CompositionApplicationReport applied = plan.ApplyTo(services);
if (applied.Probe().Count != 3)
{
    throw new InvalidOperationException("The application report is incomplete.");
}
CompositionPlan generated = GeneratedCatalogComposition.CreatePlan();
if (generated.Probe().Registrations.Count != 3)
{
    throw new InvalidOperationException("The generated plan inventory is incomplete.");
}
generated.ApplyTo(services);
using ServiceProvider provider = services.BuildServiceProvider(
    new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
);
CatalogSession first;
using (IServiceScope scope = provider.CreateScope())
{
    first = scope.ServiceProvider.GetRequiredService<CatalogSession>();
    var alias = scope.ServiceProvider.GetRequiredService<ICatalogSession>();
    var repository = scope.ServiceProvider.GetRequiredService<IRepository<CatalogItem>>();
    var catalogConverter = scope.ServiceProvider.GetRequiredService<
        ICatalogConverter<CatalogItem>
    >();
    var inventoryConverter = scope.ServiceProvider.GetRequiredService<
        ICatalogConverter<InventoryItem>
    >();
    if (
        !ReferenceEquals(first, catalogConverter.Session)
        || !ReferenceEquals(first, inventoryConverter.Session)
    )
    {
        throw new InvalidOperationException(
            "Generated inherited registrations lost scope identity."
        );
    }
    scope.ServiceProvider.GetRequiredService<GeneratedCatalogProbe>();
    if (!ReferenceEquals(first, alias) || !ReferenceEquals(first, repository.Session))
    {
        throw new InvalidOperationException("Scope identity was lost.");
    }
}
if (!first.Disposed)
{
    throw new InvalidOperationException("The container did not dispose its scoped instance.");
}
using (IServiceScope scope = provider.CreateScope())
{
    if (ReferenceEquals(first, scope.ServiceProvider.GetRequiredService<ICatalogSession>()))
    {
        throw new InvalidOperationException("Separate scopes shared an instance.");
    }
}
Console.WriteLine(
    "Composition AOT passed: explicit and generated plans, inherited generic matching, application reports, scoped alias, open generic resolution, disposal."
);

internal interface ICatalogSession;

internal sealed class CatalogSession : ICatalogSession, IDisposable
{
    public CatalogSession() { }

    public bool Disposed { get; private set; }

    public void Dispose() => Disposed = true;
}

internal interface IRepository<T>
    where T : class
{
    ICatalogSession Session { get; }
}

internal sealed class Repository<T>(ICatalogSession session) : IRepository<T>
    where T : class
{
    public ICatalogSession Session { get; } = session;
}

internal sealed class CatalogItem;
