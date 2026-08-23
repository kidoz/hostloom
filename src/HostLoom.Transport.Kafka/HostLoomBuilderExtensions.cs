using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Transport.Kafka;

public static class HostLoomBuilderExtensions
{
    public static HostLoomBuilder UseKafka(this HostLoomBuilder builder, Action<KafkaOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddOptions<KafkaOptions>();
        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        return builder.UseTransport<KafkaRequestBroker>();
    }
}
