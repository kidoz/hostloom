using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Composition.Testing;

/// <summary>Test-framework-independent assertions over passive composition data.</summary>
public static class CompositionAssert
{
    /// <summary>Compares normalized registration multisets, preserving duplicate multiplicity.</summary>
    public static void EquivalentRegistrations(
        IEnumerable<CompositionRegistrationShape> expected,
        IEnumerable<CompositionRegistrationShape> actual
    )
    {
        var left = Copy(expected);
        var right = Copy(actual);
        var remaining = left.GroupBy(static entry => entry)
            .ToDictionary(static group => group.Key, static group => group.Count());
        foreach (var entry in right)
        {
            if (!remaining.TryGetValue(entry, out int count) || count == 0)
                throw new CompositionAssertionException(
                    $"Unexpected registration (or excess duplicate): {entry}."
                );
            remaining[entry] = count - 1;
        }
        foreach (var pair in remaining)
            if (pair.Value != 0)
                throw new CompositionAssertionException(
                    $"Missing {pair.Value} registration(s): {pair.Key}."
                );
    }

    /// <summary>Compares two generated/type-backed plans without comparing their provenance.</summary>
    public static void EquivalentRegistrations(CompositionPlan expected, CompositionPlan actual)
    {
        ArgumentNullException.ThrowIfNull(expected);
        ArgumentNullException.ThrowIfNull(actual);
        EquivalentRegistrations(
            CompositionRegistrationShape.Project(expected.Probe()),
            CompositionRegistrationShape.Project(actual.Probe())
        );
    }

    /// <summary>Compares the normalized registration sequence, including multiplicity and order.</summary>
    public static void RegistrationSequence(
        IEnumerable<CompositionRegistrationShape> expected,
        IEnumerable<CompositionRegistrationShape> actual
    )
    {
        var left = Copy(expected);
        var right = Copy(actual);
        if (left.Length != right.Length)
            throw new CompositionAssertionException(
                $"Expected {left.Length} registrations but found {right.Length}."
            );
        for (var i = 0; i < left.Length; i++)
            if (left[i] != right[i])
                throw new CompositionAssertionException(
                    $"Registration {i} differs. Expected: {left[i]}. Actual: {right[i]}."
                );
    }

    /// <summary>Compares generated/type-backed plans in registration order.</summary>
    public static void RegistrationSequence(CompositionPlan expected, CompositionPlan actual)
    {
        ArgumentNullException.ThrowIfNull(expected);
        ArgumentNullException.ThrowIfNull(actual);
        RegistrationSequence(
            CompositionRegistrationShape.Project(expected.Probe()),
            CompositionRegistrationShape.Project(actual.Probe())
        );
    }

    /// <summary>Asserts the distinct matched implementation set, independently of service projection.</summary>
    public static void MatchedTypes(CompositionPlanProbe probe, params Type[] expected)
    {
        ArgumentNullException.ThrowIfNull(probe);
        ArgumentNullException.ThrowIfNull(expected);
        if (expected.Any(static type => type is null))
            throw new ArgumentException("Expected types cannot contain null.", nameof(expected));
        if (probe.Registrations.Any(static entry => entry.ImplementationType is null))
            throw new CompositionAssertionException(
                "Matched types are unknown for opaque factories or instances."
            );
        var actual = probe
            .Registrations.Select(static entry => entry.ImplementationType!)
            .ToHashSet();
        if (!actual.SetEquals(expected))
            throw new CompositionAssertionException(
                $"Matched types differ. Expected: {string.Join(", ", expected.Select(static type => type.ToString()))}. Actual: {string.Join(", ", actual)}."
            );
    }

    /// <summary>Asserts the complete implementation multiset, lifetime and cardinality for one service.</summary>
    public static void Service(
        CompositionPlanProbe probe,
        Type serviceType,
        ServiceLifetime lifetime,
        CompositionCardinality cardinality,
        params Type[] implementations
    )
    {
        ArgumentNullException.ThrowIfNull(probe);
        ArgumentNullException.ThrowIfNull(serviceType);
        ArgumentNullException.ThrowIfNull(implementations);
        if (implementations.Any(static type => type is null))
            throw new ArgumentException(
                "Expected implementations cannot contain null.",
                nameof(implementations)
            );
        var entries = probe
            .Registrations.Where(entry => entry.Descriptor.ServiceType == serviceType)
            .ToArray();
        if (
            entries.Any(entry =>
                entry.Descriptor.Lifetime != lifetime
                || entry.Cardinality != cardinality
                || entry.ImplementationType is null
            )
        )
            throw new CompositionAssertionException(
                $"Service '{serviceType}' has a different lifetime/cardinality or an unknown implementation."
            );
        var remaining = implementations.ToList();
        foreach (var entry in entries)
            if (!remaining.Remove(entry.ImplementationType!))
                throw new CompositionAssertionException(
                    $"Unexpected implementation of '{serviceType}': {entry.ImplementationType}."
                );
        if (remaining.Count != 0)
            throw new CompositionAssertionException(
                $"Missing {remaining.Count} implementation(s) of '{serviceType}'."
            );
    }

    /// <summary>Asserts registration origins in order, separately from semantic equality.</summary>
    public static void Origins(CompositionPlanProbe probe, params CompositionOrigin[] expected)
    {
        ArgumentNullException.ThrowIfNull(probe);
        ArgumentNullException.ThrowIfNull(expected);
        if (!probe.Registrations.Select(static entry => entry.Origin).SequenceEqual(expected))
            throw new CompositionAssertionException("Registration origin sequence differs.");
    }

    /// <summary>Requires exactly one candidate rejection at this origin with these ordered reasons.</summary>
    public static void Rejection(
        CompositionPlanProbe probe,
        string candidateIdentity,
        CompositionOrigin origin,
        params string[] reasons
    )
    {
        ArgumentNullException.ThrowIfNull(probe);
        ArgumentException.ThrowIfNullOrWhiteSpace(candidateIdentity);
        ArgumentNullException.ThrowIfNull(origin);
        ArgumentNullException.ThrowIfNull(reasons);
        var entries = probe
            .RejectedCandidates.Where(entry =>
                entry.CandidateIdentity == candidateIdentity && entry.Origin == origin
            )
            .ToArray();
        if (
            entries.Length != 1
            || !entries[0].Reasons.SequenceEqual(reasons, StringComparer.Ordinal)
        )
            throw new CompositionAssertionException(
                $"Expected one rejection of '{candidateIdentity}' at '{origin}' with the supplied ordered reasons."
            );
    }

    private static CompositionRegistrationShape[] Copy(
        IEnumerable<CompositionRegistrationShape> entries
    )
    {
        ArgumentNullException.ThrowIfNull(entries);
        var copy = entries.ToArray();
        if (copy.Any(static entry => entry is null))
            throw new ArgumentException("Registrations cannot contain null.", nameof(entries));
        return copy;
    }
}
