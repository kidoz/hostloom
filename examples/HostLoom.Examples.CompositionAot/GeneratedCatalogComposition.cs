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
                    .AsImplementedInterfaces()
                    .WithScopedLifetime()
                    .ExpectOne();
                group
                    .AddTypes(typeof(GeneratedCatalogProbe))
                    .AsSelf()
                    .WithSingletonLifetime()
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

internal abstract class CatalogConverterBase<T>(ICatalogSession session) : ICatalogConverter<T>
{
    public ICatalogSession Session { get; } = session;
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
