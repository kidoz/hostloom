using Microsoft.Extensions.Options;

namespace HostLoom.Caching.DependencyInjection;

/// <summary>
/// Runs <see cref="CachingOptions.Validate"/> at startup and adds the composition rules only a
/// container knows: a distributed store needs a serializer, and something must be chosen.
/// </summary>
internal sealed class CachingOptionsValidator(CachingRegistration registration)
    : IValidateOptions<CachingOptions>
{
    public ValidateOptionsResult Validate(string? name, CachingOptions options)
    {
        var problems = new List<string>(options.Validate());
        if (registration.StoreName is null)
        {
            problems.Add(
                "No cache store was chosen. Call UseInMemory() for an in-process cache or "
                    + "UseStore<TStore>(name) for a distributed one on the CachingBuilder."
            );
        }
        else if (registration.StoreName != CachingBuilder.InMemoryStoreName)
        {
            if (registration.SerializerName is null)
            {
                problems.Add(
                    $"Store '{registration.StoreName}' needs a serializer. Call "
                        + "UseSystemTextJson(JsonSerializerOptions) with a TypeInfoResolver, "
                        + "UseSerializer<TSerializer>(), or the reflection opt-in "
                        + "UseReflectionSerialization() on the CachingBuilder."
                );
            }
        }
        else if (!options.L1.Enabled)
        {
            problems.Add(
                "Caching:L1:Enabled is false and UseInMemory() composes no distributed tier, so nothing would be cached."
            );
        }

        return problems.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(problems);
    }
}
