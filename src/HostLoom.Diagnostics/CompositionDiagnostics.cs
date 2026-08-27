using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace HostLoom.Diagnostics;

/// <summary>
/// Reports the composition ledger through the logging stack. The generic host runs this
/// automatically at startup; call it directly when composing a provider without a host.
/// </summary>
public static class CompositionDiagnostics
{
    /// <summary>
    /// The logger category every composition event is written under, so the whole report can be
    /// raised, lowered, or silenced through standard <c>Logging</c> configuration without an
    /// options knob of its own.
    /// </summary>
    public const string LogCategory = "HostLoom.Diagnostics.Composition";

    /// <summary>
    /// Reports the ledger registered in <paramref name="services"/>. Does nothing when no ledger
    /// was ever populated or no logger factory exists, and swallows any failure raised while
    /// reporting — composition diagnostics are an aid, and must never be the reason a host fails
    /// to start. The <see cref="Report(ILogger, CompositionReport)"/> overload stays transparent,
    /// so a caller supplying its own logger still sees what went wrong.
    /// </summary>
    public static void Report(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        try
        {
            if (services.GetService<CompositionLedger>() is not { } ledger)
            {
                return;
            }

            if (services.GetService<ILoggerFactory>()?.CreateLogger(LogCategory) is not { } logger)
            {
                return;
            }

            Report(logger, ledger.Snapshot());
        }
        catch (Exception)
        {
            // Swallowed by contract, as the bootstrap logger swallows its own write failures: a
            // provider that throws while describing the composition must not take the host down
            // with it, and there is no second logger to report the failure to.
        }
    }

    /// <summary>
    /// Writes one <c>Information</c> manifest line for the whole composition, one <c>Debug</c> line
    /// per decision with its reason and origin, and one <c>Warning</c> per conflicting component.
    /// The manifest sits at <c>Information</c> on purpose: a composition question is asked when
    /// production misbehaves, which is exactly when a <c>Debug</c> line has already been filtered
    /// out.
    /// </summary>
    public static void Report(ILogger logger, CompositionReport report)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(report);
        if (report.Decisions.Count == 0)
        {
            // Nothing recorded. An empty manifest reads as "nothing was configured", which is a
            // different and wrong claim.
            return;
        }

        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("HostLoom composition: {Composition}", report.Describe());
        }

        if (logger.IsEnabled(LogLevel.Debug))
        {
            foreach (var decision in report.Decisions)
            {
                logger.LogDebug(
                    "HostLoom composition {Component} -> {Choice} recorded by {Origin}: {Reason}",
                    decision.Component,
                    decision.Choice,
                    decision.Origin ?? "(unknown)",
                    decision.Reason ?? "(no reason recorded)"
                );
            }
        }

        if (!logger.IsEnabled(LogLevel.Warning))
        {
            return;
        }

        foreach (var conflict in report.Conflicts)
        {
            logger.LogWarning(
                "HostLoom composition component '{Component}' was recorded with conflicting choices: "
                    + "{Choices}. Only one of them is in effect.",
                conflict.Component,
                string.Join(", ", conflict.Choices)
            );
        }
    }
}
