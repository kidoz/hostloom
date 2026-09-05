using HostLoom.Composition;
using HostLoom.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Examples.CompositionDiagnostics;

// Application-owned example: neither composition package depends on the optional ledger.
internal static class ApplicationCompositionLedger
{
    internal static void Record(
        CompositionLedger ledger,
        CompositionPlan plan,
        CompositionApplicationReport applied
    )
    {
        ArgumentNullException.ThrowIfNull(ledger);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(applied);
        if (!string.Equals(plan.Identity, applied.Identity, StringComparison.Ordinal))
            throw new ArgumentException("The report must describe this plan.", nameof(applied));
        var actions = applied.Probe();
        // Replay removals of entries added earlier in this application. This is an application
        // report, not an inventory of external descriptors retained by Skip or later mutations.
        var retained = new HashSet<ServiceDescriptor>(ReferenceEqualityComparer.Instance);
        foreach (var action in actions)
        {
            if (action.Outcome == CompositionApplicationOutcome.Added)
                retained.Add(action.Descriptor);
            else if (action.Outcome == CompositionApplicationOutcome.Replaced)
                retained.Remove(action.Descriptor);
        }
        foreach (
            var group in actions.GroupBy(static action =>
                (action.Origin.Group, action.Descriptor.ServiceType)
            )
        )
        {
            string component = Key(
                applied.Identity,
                group.Key.Group,
                "service",
                TypeName(group.Key.ServiceType)
            );
            string[] added = group
                .Where(action =>
                    action.Outcome == CompositionApplicationOutcome.Added
                    && retained.Contains(action.Descriptor)
                )
                .Select(action => Describe(action.Descriptor, plan))
                .ToArray();
            string choice =
                added.Length == 0
                    ? "No retained additions"
                    : "Retained additions: " + string.Join("; ", added);
            string reason = string.Join(
                " | ",
                group.Select(action =>
                    $"{action.Outcome}: {Describe(action.Descriptor, plan)}; {action.Reason}; rule={action.Origin.Rule}; previous={action.PreviousOrigin?.ToString() ?? "external/none"}"
                )
            );
            ledger.Record(component, choice, reason, origin: applied.Identity);
        }
        foreach (var rejection in plan.Probe().RejectedCandidates)
            ledger.RecordSkipped(
                Key(
                    plan.Identity,
                    rejection.Origin.Group,
                    "candidate",
                    rejection.Origin.Rule,
                    rejection.CandidateIdentity
                ),
                string.Join("; ", rejection.Reasons),
                origin: rejection.Origin.ToString()
            );
    }

    private static string Describe(ServiceDescriptor descriptor, CompositionPlan plan)
    {
        var known = plan.Probe()
            .Registrations.FirstOrDefault(entry => ReferenceEquals(entry.Descriptor, descriptor));
        string activation =
            known?.AliasTargetType is { } alias ? "alias -> " + TypeName(alias)
            : descriptor.ImplementationType is { } type ? TypeName(type)
            : descriptor.ImplementationFactory is not null ? "opaque factory"
            : "opaque instance";
        return activation + " / " + descriptor.Lifetime;
    }

    private static string TypeName(Type type) => type.FullName ?? type.Name;

    // Length prefixes keep user-owned group/rule/type names from creating component collisions.
    private static string Key(params string?[] parts) =>
        string.Concat(
            parts.Select(static part =>
                part is null
                    ? "-1:"
                    : part.Length.ToString(System.Globalization.CultureInfo.InvariantCulture)
                        + ":"
                        + part
            )
        );
}
