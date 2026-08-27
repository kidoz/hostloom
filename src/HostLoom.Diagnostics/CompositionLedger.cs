using System.Runtime.CompilerServices;

namespace HostLoom.Diagnostics;

/// <summary>
/// Collects composition decisions while the service collection is built, and is registered into
/// that same collection as a singleton instance, so the decisions survive into the built container
/// and can be reported when a real <see cref="Microsoft.Extensions.Logging.ILogger"/> and bound
/// options finally exist. Recording is passive: a library feeds the ledger unconditionally, and
/// nothing is written anywhere until the application opts in with
/// <see cref="CompositionLedgerServiceCollectionExtensions.AddCompositionDiagnostics"/>. That split
/// keeps collection order-independent — a registration made before the opt-in is still recorded.
/// </summary>
/// <remarks>
/// The lock guards the hand-off from registration to the thread that starts the host, not
/// concurrent registration: <c>IServiceCollection</c> is not thread-safe, so composing one
/// from several threads is already unsupported before this type is involved. Registration is
/// single-threaded; only the snapshot crosses a thread boundary. Nothing here is on a request path.
/// </remarks>
public sealed class CompositionLedger
{
    private readonly List<CompositionDecision> _decisions = [];
    private readonly Lock _gate = new();

    /// <summary>Records what a component resolved to.</summary>
    /// <param name="component">What was decided, for example <c>Transport</c>.</param>
    /// <param name="choice">What it resolved to, for example <c>Kafka</c>.</param>
    /// <param name="reason">Why — most useful when it names the configuration key that decided it.</param>
    /// <param name="origin">Defaults to the calling registration method; pass it explicitly when forwarding.</param>
    public void Record(
        string component,
        string choice,
        string? reason = null,
        [CallerMemberName] string? origin = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(component);
        ArgumentException.ThrowIfNullOrWhiteSpace(choice);
        if (string.Equals(choice, CompositionDecision.Skipped, StringComparison.Ordinal))
        {
            // Routing skips through here would let one in without a reason, which is the whole
            // thing RecordSkipped exists to prevent.
            throw new ArgumentException(
                $"Record a skipped component with {nameof(RecordSkipped)}, which requires the reason.",
                nameof(choice)
            );
        }

        Add(new CompositionDecision(component, choice, reason, origin));
    }

    /// <summary>
    /// Records a component that was deliberately left out. The reason is required, because a skip
    /// without one leaves the reader exactly where they started.
    /// </summary>
    public void RecordSkipped(
        string component,
        string reason,
        [CallerMemberName] string? origin = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(component);
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        Add(new CompositionDecision(component, CompositionDecision.Skipped, reason, origin));
    }

    /// <summary>Takes an immutable view of the ledger and computes its conflicting components.</summary>
    public CompositionReport Snapshot()
    {
        CompositionDecision[] decisions;
        lock (_gate)
        {
            decisions = [.. _decisions];
        }

        var conflicts = decisions
            .GroupBy(decision => decision.Component, StringComparer.Ordinal)
            .Select(group => new CompositionConflict(
                group.Key,
                [.. group.Select(decision => decision.Choice).Distinct(StringComparer.Ordinal)]
            ))
            // The same component recorded twice with the same choice is a harmless duplicate call,
            // not a disagreement. Only differing choices mean one of them is not in effect.
            .Where(conflict => conflict.Choices.Count > 1)
            .ToArray();

        return new CompositionReport(decisions, conflicts);
    }

    private void Add(CompositionDecision decision)
    {
        lock (_gate)
        {
            _decisions.Add(decision);
        }
    }
}
