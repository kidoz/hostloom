using System.Globalization;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Http;

namespace HostLoom.AspNetCore.WebSockets;

/// <summary>
/// Resolves expiry from the authentication ticket first and then from an <c>exp</c> claim.
/// </summary>
public sealed class DefaultWebSocketSessionLifetimeResolver : IWebSocketSessionLifetimeResolver
{
    /// <inheritdoc />
    public ValueTask<DateTimeOffset?> ResolveExpirationAsync(
        HttpContext context,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(context);
        cancellationToken.ThrowIfCancellationRequested();

        var ticketExpiration = context
            .Features.Get<IAuthenticateResultFeature>()
            ?.AuthenticateResult?.Properties?.ExpiresUtc;
        if (ticketExpiration is not null)
        {
            return ValueTask.FromResult(ticketExpiration);
        }

        DateTimeOffset? claimExpiration = null;
        foreach (var claim in context.User.FindAll("exp"))
        {
            if (
                !long.TryParse(
                    claim.Value,
                    NumberStyles.Integer,
                    CultureInfo.InvariantCulture,
                    out var seconds
                )
            )
            {
                continue;
            }

            DateTimeOffset parsed;
            try
            {
                parsed = DateTimeOffset.FromUnixTimeSeconds(seconds);
            }
            catch (ArgumentOutOfRangeException)
            {
                continue;
            }

            if (claimExpiration is null || parsed < claimExpiration)
            {
                claimExpiration = parsed;
            }
        }

        return ValueTask.FromResult(claimExpiration);
    }
}
