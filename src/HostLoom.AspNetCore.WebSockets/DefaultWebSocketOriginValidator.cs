using System.Globalization;
using Microsoft.AspNetCore.Http;

namespace HostLoom.AspNetCore.WebSockets;

internal sealed class DefaultWebSocketOriginValidator : IWebSocketOriginValidator
{
    private readonly HostLoomWebSocketOptions _options;
    private readonly HashSet<NormalizedOrigin> _allowedOrigins;

    public DefaultWebSocketOriginValidator(GatewayConfiguration configuration)
    {
        _options = configuration.Options;
        _allowedOrigins = _options
            .AllowedOrigins.Select(static origin =>
                NormalizedOrigin.TryParse(origin, out var normalized)
                    ? normalized
                    : throw new InvalidOperationException(
                        $"The configured WebSocket origin '{origin}' is invalid."
                    )
            )
            .ToHashSet();
    }

    public ValueTask<bool> IsAllowedAsync(HttpContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        cancellationToken.ThrowIfCancellationRequested();

        if (_options.OriginMode is WebSocketOriginMode.Disabled)
        {
            return ValueTask.FromResult(true);
        }

        var values = context.Request.Headers.Origin;
        if (values.Count == 0)
        {
            return ValueTask.FromResult(_options.AllowMissingOrigin);
        }

        if (values.Count != 1 || !NormalizedOrigin.TryParse(values[0]!, out var suppliedOrigin))
        {
            return ValueTask.FromResult(false);
        }

        var allowed = _options.OriginMode switch
        {
            WebSocketOriginMode.SameOrigin => NormalizedOrigin.TryParseRequest(
                context.Request,
                out var requestOrigin
            )
                && suppliedOrigin == requestOrigin,
            WebSocketOriginMode.AllowList => _allowedOrigins.Contains(suppliedOrigin),
            _ => false,
        };
        return ValueTask.FromResult(allowed);
    }

    internal readonly record struct NormalizedOrigin(string Scheme, string Host, int Port)
    {
        public static bool TryParse(string value, out NormalizedOrigin origin)
        {
            if (
                !Uri.TryCreate(value, UriKind.Absolute, out var uri)
                || !string.IsNullOrEmpty(uri.UserInfo)
                || !string.IsNullOrEmpty(uri.Query)
                || !string.IsNullOrEmpty(uri.Fragment)
                || uri.AbsolutePath is not "/"
                || !TryNormalizeScheme(uri.Scheme, out var scheme)
            )
            {
                origin = default;
                return false;
            }

            origin = new NormalizedOrigin(scheme, uri.IdnHost.ToUpperInvariant(), uri.Port);
            return true;
        }

        public static bool TryParseRequest(HttpRequest request, out NormalizedOrigin origin)
        {
            if (!request.Host.HasValue)
            {
                origin = default;
                return false;
            }

            return TryParse(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"{request.Scheme}://{request.Host.ToUriComponent()}"
                ),
                out origin
            );
        }

        private static bool TryNormalizeScheme(string value, out string scheme)
        {
            if (
                value.Equals("http", StringComparison.OrdinalIgnoreCase)
                || value.Equals("ws", StringComparison.OrdinalIgnoreCase)
            )
            {
                scheme = "http";
                return true;
            }

            if (
                value.Equals("https", StringComparison.OrdinalIgnoreCase)
                || value.Equals("wss", StringComparison.OrdinalIgnoreCase)
            )
            {
                scheme = "https";
                return true;
            }

            scheme = string.Empty;
            return false;
        }
    }
}
