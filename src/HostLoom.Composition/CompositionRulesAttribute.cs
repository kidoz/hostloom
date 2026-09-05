namespace HostLoom.Composition;

/// <summary>Marks a declaration-only method whose rules generate the named partial plan factory.</summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class CompositionRulesAttribute : Attribute
{
    /// <summary>Names a parameterless static partial CompositionPlan method in the same type.</summary>
    public CompositionRulesAttribute(string factoryName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(factoryName);
        FactoryName = factoryName;
    }

    /// <summary>The name of the generated plan factory.</summary>
    public string FactoryName { get; }
}
