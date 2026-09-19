using Microsoft.Extensions.Options;

namespace HostLoom.Leadership.DependencyInjection;

/// <summary>Runs <see cref="LeadershipOptions.Validate"/> for every role's named options when the host starts.</summary>
internal sealed class LeadershipOptionsValidator : IValidateOptions<LeadershipOptions>
{
    public ValidateOptionsResult Validate(string? name, LeadershipOptions options)
    {
        var problems = options.Validate();
        return problems.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(
                problems.Select(problem =>
                    name is null or "" ? problem : $"Role '{name}': {problem}"
                )
            );
    }
}
