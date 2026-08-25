namespace HostLoom.Pipelines;

public interface IProbeSite
{
    /// <summary>
    /// Describes this site to a structural probe without executing it. The default reports the
    /// implementing type's name, so a filter only overrides this when it has more to say.
    /// </summary>
    void Probe(IProbeContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        context.CreateScope(GetType().Name);
    }
}
