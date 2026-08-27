using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace HostLoom.Diagnostics;

/// <summary>
/// Records what the composition root decided, from inside the <c>AddX</c> methods that decide it.
/// </summary>
public static class CompositionLedgerServiceCollectionExtensions
{
    /// <summary>
    /// Returns the ledger for this collection, adding it on first use. Get-or-add rather than
    /// opt-in so a library can record unconditionally and cheaply: collection is then independent
    /// of whether — or when — the application asks for a report.
    /// </summary>
    public static CompositionLedger CompositionLedger(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        for (var i = 0; i < services.Count; i++)
        {
            if (
                services[i].ServiceType == typeof(CompositionLedger)
                && services[i].ImplementationInstance is CompositionLedger existing
            )
            {
                return existing;
            }
        }

        var ledger = new CompositionLedger();
        services.AddSingleton(ledger);
        return ledger;
    }

    /// <summary>Records what a component resolved to, chainable inside a registration method.</summary>
    public static IServiceCollection RecordComposition(
        this IServiceCollection services,
        string component,
        string choice,
        string? reason = null,
        [CallerMemberName] string? origin = null
    )
    {
        // The origin is forwarded rather than defaulted again: recapturing it inside Record would
        // name this extension method instead of the registration that called it.
        services.CompositionLedger().Record(component, choice, reason, origin);
        return services;
    }

    /// <summary>Records a component deliberately left out, chainable inside a registration method.</summary>
    public static IServiceCollection RecordSkippedComposition(
        this IServiceCollection services,
        string component,
        string reason,
        [CallerMemberName] string? origin = null
    )
    {
        services.CompositionLedger().RecordSkipped(component, reason, origin);
        return services;
    }

    /// <summary>
    /// Reports the ledger once when the host starts, under the
    /// <see cref="CompositionDiagnostics.LogCategory"/> category. Safe to call more than once and
    /// at any point in the composition root: the report is taken at startup, so it includes
    /// decisions recorded both before and after this call.
    /// </summary>
    public static IServiceCollection AddCompositionDiagnostics(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.CompositionLedger();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, CompositionReporter>()
        );
        return services;
    }
}
