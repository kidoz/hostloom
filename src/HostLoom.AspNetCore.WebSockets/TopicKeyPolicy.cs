using System.Security.Claims;

namespace HostLoom.AspNetCore.WebSockets;

/// <summary>Built-in authorization policies for client-selected topic keys.</summary>
public static class TopicKeyPolicy
{
    /// <summary>
    /// Requires a nonempty subscription key equal to the configured subject claim
    /// <see cref="HostLoomWebSocketOptions.SubjectClaimType"/>.
    /// </summary>
    public const string SubjectOnly = "HostLoom.WebSockets.TopicKey.SubjectOnly";

    internal static string? ResolveSubject(ClaimsPrincipal user, string claimType)
    {
        if (user.Identity is not ClaimsIdentity { IsAuthenticated: true } identity)
            return null;

        var subject = identity.FindFirst(claimType)?.Value;
        return string.IsNullOrWhiteSpace(subject) ? null : subject;
    }
}
