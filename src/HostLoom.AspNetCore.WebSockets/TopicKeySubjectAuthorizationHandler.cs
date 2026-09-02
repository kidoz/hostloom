using Microsoft.AspNetCore.Authorization;

namespace HostLoom.AspNetCore.WebSockets;

internal sealed class TopicKeySubjectRequirement : IAuthorizationRequirement;

internal sealed class TopicKeySubjectAuthorizationHandler(GatewayConfiguration configuration)
    : AuthorizationHandler<TopicKeySubjectRequirement, WebSocketTopicResource>
{
    protected override Task HandleRequirementAsync(
        AuthorizationHandlerContext context,
        TopicKeySubjectRequirement requirement,
        WebSocketTopicResource resource
    )
    {
        var subject = context.User.FindFirst(configuration.Options.SubjectClaimType)?.Value;
        if (
            !string.IsNullOrWhiteSpace(resource.Key)
            && !string.IsNullOrWhiteSpace(subject)
            && string.Equals(subject, resource.Key, StringComparison.Ordinal)
        )
        {
            context.Succeed(requirement);
        }

        return Task.CompletedTask;
    }
}
