using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Transport.RabbitMq;

public static class HostLoomBuilderExtensions
{
    public static HostLoomBuilder UseRabbitMq(
        this HostLoomBuilder builder,
        Action<RabbitMqOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddOptions<RabbitMqOptions>();
        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        return builder.UseTransport<RabbitMqRequestBroker>();
    }
}
