using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Composition;

/// <summary>An immutable, ordered registration plan that can be inspected without a provider.</summary>
/// <remarks>
/// Application is single-threaded, just like IServiceCollection composition. Validation failures
/// leave the collection unchanged. Exceptions thrown by a custom collection during mutation are
/// outside that guarantee. Factories and constructors are never invoked by this type.
/// </remarks>
public sealed class CompositionPlan
{
    private static readonly ConditionalWeakTable<IServiceCollection, ApplicationState> States =
        new();
    private readonly CompositionPlanProbe _probe;

    /// <summary>Creates a plan from explicit descriptors and defensively copies its inputs.</summary>
    /// <param name="identity">A stable assembly/type/factory identity, shared by fresh instances of the same plan.</param>
    /// <param name="registrations">Registrations in application order.</param>
    /// <param name="rejectedCandidates">Optional rejected candidates with declaration provenance.</param>
    public CompositionPlan(
        string identity,
        IEnumerable<CompositionRegistration> registrations,
        IEnumerable<CompositionCandidateRejection>? rejectedCandidates = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(identity);
        ArgumentNullException.ThrowIfNull(registrations);
        CompositionRegistration[] entries = registrations.ToArray();
        CompositionCandidateRejection[] rejections = rejectedCandidates?.ToArray() ?? [];
        if (entries.Any(static entry => entry is null))
        {
            throw new ArgumentException(
                "Registrations cannot contain null.",
                nameof(registrations)
            );
        }
        if (rejections.Any(static rejection => rejection is null))
        {
            throw new ArgumentException(
                "Rejections cannot contain null.",
                nameof(rejectedCandidates)
            );
        }

        _probe = new CompositionPlanProbe(identity, entries, rejections);
        var contracts = new Dictionary<Type, CompositionRegistration>();
        foreach (CompositionRegistration entry in entries)
        {
            AddContract(contracts, entry, CompositionValidationPhase.PlanConstruction);
        }
        ValidateSet(
            entries.Select(static entry => new TrackedDescriptor(entry.Descriptor, entry)).ToList(),
            contracts,
            CompositionValidationPhase.PlanConstruction
        );
    }

    /// <summary>The stable identity used to reject repeated application to the same collection.</summary>
    public string Identity => _probe.Identity;

    /// <summary>Returns an immutable intention snapshot without executing registrations.</summary>
    public CompositionPlanProbe Probe() => _probe;

    /// <summary>Validates against the current collection, applies once and returns passive effects.</summary>
    public CompositionApplicationReport ApplyTo(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        ApplicationState state = States.GetValue(services, static _ => new ApplicationState());
        if (state.Identities.Contains(Identity))
        {
            throw Error(
                CompositionValidationPhase.Application,
                "This identity was already applied to this collection."
            );
        }

        var staged = new List<TrackedDescriptor>(services.Count + _probe.Registrations.Count);
        var contracts = new Dictionary<Type, CompositionRegistration>();
        for (var index = 0; index < services.Count; index++)
        {
            ServiceDescriptor descriptor = services[index];
            state.Registrations.TryGetValue(descriptor, out CompositionRegistration? known);
            staged.Add(new TrackedDescriptor(descriptor, known, index));
            if (state.Contracts.TryGetValue(descriptor, out CompositionRegistration? contract))
            {
                AddContract(contracts, contract, CompositionValidationPhase.Application);
            }
        }

        var decisions = new List<CompositionApplicationDecision>();
        foreach (CompositionRegistration entry in _probe.Registrations)
        {
            AddContract(contracts, entry, CompositionValidationPhase.Application);
            Stage(entry, staged, decisions);
        }
        ValidateSet(staged, contracts, CompositionValidationPhase.Application);
        var report = new CompositionApplicationReport(Identity, decisions);

        // Validation is now complete. Retained descriptors keep both their identity and order.
        // Remove only descriptors selected for replacement; no Clear/re-add of unrelated entries.
        var retained = new HashSet<int>(
            staged
                .Where(static item => item.ExistingIndex.HasValue)
                .Select(static item => item.ExistingIndex!.Value)
        );
        for (var i = services.Count - 1; i >= 0; i--)
        {
            if (!retained.Contains(i))
            {
                services.RemoveAt(i);
            }
        }
        foreach (
            TrackedDescriptor item in staged.Where(static item => !item.ExistingIndex.HasValue)
        )
        {
            services.Add(item.Descriptor);
        }

        state.Identities.Add(Identity);
        state.Registrations.Clear();
        state.Contracts.Clear();
        foreach (TrackedDescriptor item in staged)
        {
            if (item.Registration is not null)
            {
                state.Registrations[item.Descriptor] = item.Registration;
            }
            if (
                !item.Descriptor.IsKeyedService
                && contracts.TryGetValue(
                    item.Descriptor.ServiceType,
                    out CompositionRegistration? contract
                )
            )
            {
                state.Contracts[item.Descriptor] = contract;
            }
        }
        return report;
    }

    private void Stage(
        CompositionRegistration entry,
        List<TrackedDescriptor> staged,
        List<CompositionApplicationDecision> decisions
    )
    {
        CompositionRegistrationStrategy strategy =
            entry.Strategy == CompositionRegistrationStrategy.Default
                ? entry.Cardinality == CompositionCardinality.One
                    ? CompositionRegistrationStrategy.Throw
                    : CompositionRegistrationStrategy.Append
                : entry.Strategy;
        int collision = staged.FindIndex(item => SameService(item.Descriptor, entry.Descriptor));
        if (strategy == CompositionRegistrationStrategy.Skip && collision >= 0)
        {
            decisions.Add(
                new CompositionApplicationDecision(
                    entry.Descriptor,
                    entry.Origin,
                    CompositionApplicationOutcome.Skipped,
                    $"Kept existing service at collection index {collision}.",
                    staged[collision].Registration?.Origin
                )
            );
            return;
        }
        if (strategy == CompositionRegistrationStrategy.Throw && collision >= 0)
        {
            throw Conflict(
                entry,
                staged[collision],
                collision,
                "The service is already registered."
            );
        }
        if (strategy == CompositionRegistrationStrategy.Replace)
        {
            foreach (
                TrackedDescriptor item in staged.Where(item =>
                    ShouldReplace(entry, item.Descriptor)
                )
            )
            {
                decisions.Add(
                    new CompositionApplicationDecision(
                        item.Descriptor,
                        entry.Origin,
                        CompositionApplicationOutcome.Replaced,
                        $"Replaced by rule '{entry.Origin.Rule}'.",
                        item.Registration?.Origin
                    )
                );
            }
            staged.RemoveAll(item => ShouldReplace(entry, item.Descriptor));
        }
        staged.Add(new TrackedDescriptor(entry.Descriptor, entry));
        decisions.Add(
            new CompositionApplicationDecision(
                entry.Descriptor,
                entry.Origin,
                CompositionApplicationOutcome.Added,
                $"Added by rule '{entry.Origin.Rule}'."
            )
        );
    }

    private void AddContract(
        Dictionary<Type, CompositionRegistration> contracts,
        CompositionRegistration incoming,
        CompositionValidationPhase phase
    )
    {
        Type service = incoming.Descriptor.ServiceType;
        if (
            contracts.TryGetValue(service, out CompositionRegistration? existing)
            && existing.Cardinality != incoming.Cardinality
        )
        {
            throw Error(
                phase,
                $"Service '{service}' has incompatible cardinalities from "
                    + $"'{existing.Origin}' and '{incoming.Origin}'.",
                incoming.Origin,
                existing.Origin
            );
        }
        contracts[service] = incoming;
    }

    private void ValidateSet(
        List<TrackedDescriptor> descriptors,
        Dictionary<Type, CompositionRegistration> contracts,
        CompositionValidationPhase phase
    )
    {
        foreach ((Type service, CompositionRegistration contract) in contracts)
        {
            var matching = descriptors
                .Select(static (item, index) => (Item: item, Index: index))
                .Where(pair =>
                    !pair.Item.Descriptor.IsKeyedService
                    && pair.Item.Descriptor.ServiceType == service
                )
                .ToArray();
            if (contract.Cardinality == CompositionCardinality.One && matching.Length != 1)
            {
                CompositionOrigin? other = matching
                    .Select(static pair => pair.Item.Registration?.Origin)
                    .FirstOrDefault(origin => origin is not null && origin != contract.Origin);
                string locations = string.Join(
                    ", ",
                    matching.Select(pair => Describe(pair.Item, pair.Index))
                );
                throw Error(
                    phase,
                    $"Service '{service}' declared One by '{contract.Origin}' "
                        + $"would have {matching.Length} registrations. {locations}",
                    contract.Origin,
                    other
                );
            }
            for (var i = 0; i < matching.Length; i++)
            {
                for (var j = 0; j < i; j++)
                {
                    ServiceDescriptor current = matching[i].Item.Descriptor;
                    ServiceDescriptor previous = matching[j].Item.Descriptor;
                    if (
                        current.Lifetime != previous.Lifetime
                        || SameActivation(matching[i].Item, matching[j].Item)
                    )
                    {
                        throw Error(
                            phase,
                            $"Service '{service}' contains a duplicate activation or conflicting "
                                + $"lifetimes: {Describe(matching[j].Item, matching[j].Index)}; "
                                + Describe(matching[i].Item, matching[i].Index),
                            matching[i].Item.Registration?.Origin ?? contract.Origin,
                            matching[j].Item.Registration?.Origin
                        );
                    }
                }
            }
        }
    }

    private CompositionValidationException Conflict(
        CompositionRegistration incoming,
        TrackedDescriptor existing,
        int index,
        string reason
    ) =>
        Error(
            CompositionValidationPhase.Application,
            $"{reason} Incoming service '{incoming.Descriptor.ServiceType}', implementation "
                + $"'{incoming.ImplementationType?.ToString() ?? "opaque factory/instance"}', "
                + $"{incoming.Descriptor.Lifetime}, from '{incoming.Origin}'; "
                + Describe(existing, index),
            incoming.Origin,
            existing.Registration?.Origin
        );

    private CompositionValidationException Error(
        CompositionValidationPhase phase,
        string message,
        CompositionOrigin? origin = null,
        CompositionOrigin? existingOrigin = null
    ) => new(Identity, phase, message, origin, existingOrigin);

    private static string Describe(TrackedDescriptor item, int index) =>
        $"collection index {index}: '{(item.Registration?.ImplementationType ?? item.Descriptor.ImplementationType)?.ToString() ?? "opaque factory/instance"}', "
        + $"{item.Descriptor.Lifetime}, origin '{item.Registration?.Origin.ToString() ?? "external descriptor"}'";

    private static bool SameService(ServiceDescriptor left, ServiceDescriptor right) =>
        !left.IsKeyedService && !right.IsKeyedService && left.ServiceType == right.ServiceType;

    private static bool SameActivation(TrackedDescriptor left, TrackedDescriptor right)
    {
        Type? leftType =
            left.Registration?.ImplementationType ?? left.Descriptor.ImplementationType;
        Type? rightType =
            right.Registration?.ImplementationType ?? right.Descriptor.ImplementationType;
        return leftType is not null && leftType == rightType
            || left.Descriptor.ImplementationFactory is not null
                && ReferenceEquals(
                    left.Descriptor.ImplementationFactory,
                    right.Descriptor.ImplementationFactory
                )
            || left.Descriptor.ImplementationInstance is not null
                && ReferenceEquals(
                    left.Descriptor.ImplementationInstance,
                    right.Descriptor.ImplementationInstance
                );
    }

    private static bool ShouldReplace(
        CompositionRegistration incoming,
        ServiceDescriptor existing
    ) =>
        !existing.IsKeyedService
        && (
            (incoming.ReplacementBehavior & CompositionReplacementBehavior.ServiceType) != 0
                && existing.ServiceType == incoming.Descriptor.ServiceType
            || (incoming.ReplacementBehavior & CompositionReplacementBehavior.ImplementationType)
                != 0
                && incoming.Descriptor.ImplementationType is not null
                && existing.ImplementationType == incoming.Descriptor.ImplementationType
        );

    private sealed record TrackedDescriptor(
        ServiceDescriptor Descriptor,
        CompositionRegistration? Registration,
        int? ExistingIndex = null
    );

    private sealed class ApplicationState
    {
        public HashSet<string> Identities { get; } = new(StringComparer.Ordinal);
        public Dictionary<ServiceDescriptor, CompositionRegistration> Registrations { get; } =
            new(ReferenceEqualityComparer.Instance);
        public Dictionary<ServiceDescriptor, CompositionRegistration> Contracts { get; } =
            new(ReferenceEqualityComparer.Instance);
    }
}
