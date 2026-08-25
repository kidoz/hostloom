namespace HostLoom.Examples.Pipelines;

/// <summary>
/// The example's stand-in for configuration: a filter's EnabledWhen predicate reads it on every
/// run, so flipping a flag changes the next composed pipe without re-registration. A production
/// service would read IOptionsMonitor-backed configuration the same way.
/// </summary>
internal sealed class FeatureFlags
{
    public bool SentenceCountEnabled { get; set; }
}
