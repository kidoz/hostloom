using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace HostLoom.AspNetCore.WebSockets;

public static class ServiceCollectionExtensions
{
    public static HostLoomWebSocketBuilder AddWebSocketGateway(
        this HostLoomBuilder hostLoom,
        Action<HostLoomWebSocketOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(hostLoom);
        var existing = hostLoom.Services.FirstOrDefault(static descriptor =>
            descriptor.ServiceType == typeof(GatewayConfiguration)
        );
        if (existing?.ImplementationInstance is GatewayConfiguration configuration)
        {
            if (configure is not null)
            {
                throw new InvalidOperationException(
                    "The WebSocket gateway options were already configured."
                );
            }

            return new HostLoomWebSocketBuilder(hostLoom, configuration);
        }

        var options = new HostLoomWebSocketOptions();
        configure?.Invoke(options);
        options.Validate();
        configuration = new GatewayConfiguration(options);

        hostLoom.Services.AddSingleton(configuration);
        hostLoom.Services.AddAuthorization();
        hostLoom.Services.AddLogging();
        hostLoom.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IWebSocketHubProtocol, JsonWebSocketHubProtocol>()
        );
        hostLoom.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IWebSocketHubProtocol, MessagePackWebSocketHubProtocol>()
        );
        hostLoom.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IWebSocketHubProtocol, ProtobufWebSocketHubProtocol>()
        );
        hostLoom.Services.TryAddSingleton<WebSocketSessionRegistry>();
        hostLoom.Services.TryAddSingleton<WebSocketRequestRouter>();
        hostLoom.Services.TryAddSingleton<WebSocketSessionFactory>();
        return new HostLoomWebSocketBuilder(hostLoom, configuration);
    }
}
