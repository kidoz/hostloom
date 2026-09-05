using HostLoom.Composition;
using HostLoom.Diagnostics;
using HostLoom.Examples.CompositionDiagnostics;
using Microsoft.Extensions.DependencyInjection;

var origin = new CompositionOrigin("DeclareCatalog/rule1", "catalog");
var plan = new CompositionPlan(
    "CatalogApplication.CreatePlan",
    [
        new(ServiceDescriptor.Scoped<ICatalog, Catalog>(), CompositionCardinality.Many, origin),
        new(ServiceDescriptor.Scoped<ICatalog, Inventory>(), CompositionCardinality.Many, origin),
    ]
);
var services = new ServiceCollection();
CompositionApplicationReport applied = plan.ApplyTo(services);
CompositionLedger ledger = services.CompositionLedger();
ApplicationCompositionLedger.Record(ledger, plan, applied);
CompositionReport report = ledger.Snapshot();
if (report.Conflicts.Count != 0 || report.Decisions.Count != 1)
    throw new InvalidOperationException(
        "Enumerable registrations must produce one ledger choice without a conflict."
    );
Console.WriteLine(report.Decisions[0].Choice);

internal interface ICatalog;

internal sealed class Catalog : ICatalog
{
    public Catalog() { }
}

internal sealed class Inventory : ICatalog
{
    public Inventory() { }
}
