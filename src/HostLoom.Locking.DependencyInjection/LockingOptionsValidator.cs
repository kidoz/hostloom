using Microsoft.Extensions.Options;

namespace HostLoom.Locking.DependencyInjection;

/// <summary>
/// Runs <see cref="LockingOptions.Validate"/> at startup and adds the one rule only the container
/// knows: an enabled lock needs a provider.
/// </summary>
internal sealed class LockingOptionsValidator(LockingRegistration registration)
    : IValidateOptions<LockingOptions>
{
    public ValidateOptionsResult Validate(string? name, LockingOptions options)
    {
        List<string> problems = [.. options.Validate()];
        if (options.Enabled && registration.ProviderName is null)
        {
            problems.Add(
                "Locking:Enabled is true but no lock provider was chosen. Call UseInMemory() or "
                    + "UseProvider<TProvider>(name) on the builder returned by AddHostLoomLocking, "
                    + "or set Locking:Enabled to false for single-instance mode."
            );
        }

        return problems.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(problems);
    }
}
