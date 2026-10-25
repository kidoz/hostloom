using HostLoom.Transport.Kafka;
using Xunit;

namespace HostLoom.Tests;

public sealed class KafkaConsumerGroupNameTests
{
    [Fact]
    public void Dotted_components_and_roles_cannot_alias()
    {
        string[] groups =
        [
            KafkaConsumerGroupNames.Request("orders", "audit.shipping"),
            KafkaConsumerGroupNames.Request("orders.audit", "shipping"),
            KafkaConsumerGroupNames.Subscription("orders", "audit", "shipping"),
            KafkaConsumerGroupNames.Subscription("orders", "audit.shipping", "inventory"),
            KafkaConsumerGroupNames.Subscription("orders", "audit", "shipping.inventory"),
            KafkaConsumerGroupNames.Subscription("orders.audit", "shipping", "inventory"),
        ];
        Assert.Equal(groups.Length, groups.Distinct(StringComparer.Ordinal).Count());
        Assert.Equal(
            KafkaConsumerGroupNames.Request("orders", "audit"),
            KafkaConsumerGroupNames.Request("orders", "audit")
        );
        Assert.All(groups, group => Assert.InRange(group.Length, 1, 255));
    }

    [Fact]
    public void Legacy_mode_retains_existing_offset_identities()
    {
        Assert.Equal(
            "orders.audit",
            KafkaConsumerGroupNames.Request("orders", "audit", KafkaConsumerGroupNaming.Legacy)
        );
        Assert.Equal(
            "orders.audit.shipping",
            KafkaConsumerGroupNames.Subscription(
                "orders",
                "audit",
                "shipping",
                KafkaConsumerGroupNaming.Legacy
            )
        );
    }
}
