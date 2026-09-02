using System.Runtime.CompilerServices;
using System.Security.Claims;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace HostLoom.AspNetCore.WebSockets;

internal sealed class WebSocketRequestRouter(
    GatewayConfiguration configuration,
    IServiceScopeFactory scopeFactory,
    ILogger<WebSocketRequestRouter> logger
)
{
    public async ValueTask<HubFrame> RouteAsync(
        HubFrame frame,
        ClaimsPrincipal user,
        CancellationToken cancellationToken
    )
    {
        if (
            string.IsNullOrWhiteSpace(frame.Operation)
            || !configuration.TryGetRequest(frame.Operation, out var route)
        )
        {
            return Fault(
                frame.StreamId,
                HubFaultCodes.OperationNotFound,
                "The requested operation is not registered."
            );
        }

        var scope = scopeFactory.CreateAsyncScope();
        try
        {
            var resource = new WebSocketOperationResource(route.Name, route.Destination);
            if (
                !await IsAuthorizedAsync(
                        scope.ServiceProvider,
                        user,
                        resource,
                        route.AuthorizationPolicy
                    )
                    .ConfigureAwait(false)
            )
            {
                return Fault(
                    frame.StreamId,
                    HubFaultCodes.Forbidden,
                    "The caller is not authorized for this operation."
                );
            }

            if (frame.Payload is not { } payload)
            {
                return Fault(
                    frame.StreamId,
                    HubFaultCodes.InvalidPayload,
                    "A request payload is required."
                );
            }

            var timeout = configuration.Options.DefaultRequestTimeout;
            if (frame.TimeoutMilliseconds is { } milliseconds)
            {
                if (
                    milliseconds <= 0
                    || milliseconds > configuration.Options.MaximumRequestTimeout.TotalMilliseconds
                )
                {
                    return Fault(
                        frame.StreamId,
                        HubFaultCodes.InvalidFrame,
                        "The request timeout is outside the allowed range."
                    );
                }

                timeout = TimeSpan.FromMilliseconds(milliseconds);
            }

            try
            {
                var invoker = (IWebSocketRequestInvoker)
                    scope.ServiceProvider.GetRequiredService(route.InvokerType);
                var response = await invoker
                    .InvokeAsync(payload, route.Destination, timeout, cancellationToken)
                    .ConfigureAwait(false);
                return new HubFrame
                {
                    Kind = HubFrameKind.Response,
                    StreamId = frame.StreamId,
                    Payload = response,
                };
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return Fault(frame.StreamId, HubFaultCodes.Canceled, "The request was canceled.");
            }
            catch (RequestTimeoutException)
            {
                return Fault(
                    frame.StreamId,
                    HubFaultCodes.RequestTimeout,
                    "The downstream request timed out."
                );
            }
            catch (Exception exception)
                when (exception is MalformedEnvelopeException or InvalidDataException)
            {
                return Fault(
                    frame.StreamId,
                    HubFaultCodes.InvalidPayload,
                    "The request payload could not be decoded."
                );
            }
            catch (RemoteRequestException exception)
            {
                var message = configuration.Options.IncludeRemoteFaultMessages
                    ? exception.Message
                    : "The downstream request failed.";
                return Fault(frame.StreamId, HubFaultCodes.RequestFailed, message);
            }
            catch (Exception exception)
            {
                logger.LogError(
                    exception,
                    "WebSocket operation {Operation} failed before a response was produced.",
                    route.Name
                );
                return Fault(
                    frame.StreamId,
                    HubFaultCodes.RequestFailed,
                    "The request could not be completed."
                );
            }
        }
        finally
        {
            await scope.DisposeAsync().ConfigureAwait(false);
        }
    }

    public async ValueTask<bool> AuthorizeTopicAsync(
        TopicRoute topic,
        string? key,
        ClaimsPrincipal user
    )
    {
        var scope = scopeFactory.CreateAsyncScope();
        try
        {
            return await IsAuthorizedAsync(
                    scope.ServiceProvider,
                    user,
                    new WebSocketTopicResource(topic.Name, key),
                    topic.AuthorizationPolicy
                )
                .ConfigureAwait(false);
        }
        finally
        {
            await scope.DisposeAsync().ConfigureAwait(false);
        }
    }

    public async IAsyncEnumerable<SerializedWebSocketSnapshot> GetTopicSnapshotAsync(
        TopicRoute topic,
        WebSocketTopicSnapshotContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        if (topic.SnapshotInvokerType is null)
        {
            yield break;
        }

        var scope = scopeFactory.CreateAsyncScope();
        try
        {
            var invoker = (IWebSocketTopicSnapshotInvoker)
                scope.ServiceProvider.GetRequiredService(topic.SnapshotInvokerType);
            await foreach (
                var item in invoker
                    .GetSnapshotAsync(context, topic.KeySelector, cancellationToken)
                    .WithCancellation(cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                yield return item;
            }
        }
        finally
        {
            await scope.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async ValueTask<bool> IsAuthorizedAsync(
        IServiceProvider services,
        ClaimsPrincipal user,
        object resource,
        string? policy
    )
    {
        if (policy is null)
        {
            return true;
        }

        var authorization = services.GetRequiredService<IAuthorizationService>();
        var result = await authorization
            .AuthorizeAsync(user, resource, policy)
            .ConfigureAwait(false);
        return result.Succeeded;
    }

    internal static HubFrame Fault(ulong streamId, string code, string message) =>
        new()
        {
            Kind = HubFrameKind.Fault,
            StreamId = streamId,
            Code = code,
            Message = message,
        };
}
