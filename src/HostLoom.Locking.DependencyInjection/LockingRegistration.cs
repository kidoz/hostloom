namespace HostLoom.Locking.DependencyInjection;

/// <summary>
/// Registration-time state shared by every <see cref="LockingBuilder"/> over one service
/// collection. It records the provider chosen so a second <c>Use*</c> can name the first, which
/// probing service descriptors cannot do.
/// </summary>
internal sealed class LockingRegistration
{
    /// <summary>The name given to <see cref="LockingBuilder.UseProvider{TProvider}(string)"/>, or <see langword="null"/>.</summary>
    public string? ProviderName { get; set; }

    /// <summary>Whether <see cref="LockingBuilder.AddHealthChecks"/> already registered the readiness check.</summary>
    public bool HealthChecksAdded { get; set; }
}
