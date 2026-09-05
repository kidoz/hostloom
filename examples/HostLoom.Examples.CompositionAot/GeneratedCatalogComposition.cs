using HostLoom.Composition;

internal static partial class GeneratedCatalogComposition
{
    [CompositionRules(nameof(CreatePlan))]
    private static void Declare(CompositionRuleBuilder rules)
    {
        rules.Group(
            "catalog",
            group =>
            {
                group
                    .AddClasses()
                    .AssignableTo(typeof(ICatalogConverter<>))
                    .WithAttribute<CatalogRegistrationAttribute>()
                    .AsSelfWithInterfaces()
                    .WithScopedLifetime()
                    .ExpectOne()
                    .ExpectExactly(2);
                group
                    .AddTypes(typeof(GeneratedCatalogProbe))
                    .AsSelf()
                    .WithSingletonLifetime()
                    .ExpectOne();
                group
                    .AddOpenGeneric(typeof(IGeneratedRepository<>), typeof(GeneratedRepository<>))
                    .WithScopedLifetime()
                    .ExpectOne();
                group
                    .AddTypes(typeof(GeneratedAsyncProbe))
                    .AssignableTo<IAsyncCatalogProbe>()
                    .AsSelfWithInterfaces()
                    .WithScopedLifetime()
                    .ExpectOne();
            }
        );
    }

    public static partial CompositionPlan CreatePlan();
}

internal interface ICatalogConverter<T>
{
    ICatalogSession Session { get; }
}

[CatalogRegistration]
internal abstract class CatalogConverterBase<T>(ICatalogSession session)
    : ICatalogConverter<T>,
        IDisposable
{
    public ICatalogSession Session { get; } = session;
    public int Disposals { get; private set; }

    public void Dispose() => Disposals++;
}

internal sealed class CatalogConverter(ICatalogSession session)
    : CatalogConverterBase<CatalogItem>(session);

internal sealed class InventoryConverter(ICatalogSession session)
    : CatalogConverterBase<InventoryItem>(session);

internal sealed class InventoryItem;

internal sealed class GeneratedCatalogProbe
{
    public GeneratedCatalogProbe() { }
}

[AttributeUsage(AttributeTargets.Class, Inherited = true)]
internal sealed class CatalogRegistrationAttribute : Attribute;

internal interface IGeneratedRepository<T>
    where T : class
{
    ICatalogSession Session { get; }
}

internal sealed class GeneratedRepository<T>(ICatalogSession session) : IGeneratedRepository<T>
    where T : class
{
    public ICatalogSession Session { get; } = session;
}

internal interface IAsyncCatalogProbe;

internal sealed class GeneratedAsyncProbe : IAsyncCatalogProbe, IAsyncDisposable
{
    public GeneratedAsyncProbe() { }

    public int Disposals { get; private set; }

    public ValueTask DisposeAsync()
    {
        Disposals++;
        return ValueTask.CompletedTask;
    }
}
