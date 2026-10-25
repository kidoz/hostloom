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
    public void Physical_names_have_one_stable_role_qualified_format()
    {
        Assert.Equal(
            "hostloom.v1.request.5ad983281a5303f7009e89f2378bd4f0ad6d72055dadfe69ac9838381ae0bc89",
            KafkaConsumerGroupNames.Request("orders", "audit")
        );
        Assert.Equal(
            "hostloom.v1.event.f050bf84c44971106e29d57a0a729ccfd37d61e1c93323a3259effed9d6e011e",
            KafkaConsumerGroupNames.Subscription("orders", "audit", "shipping")
        );
        Assert.Equal(
            "hostloom.v1.reply.b28abf4c05eec63e4736571ae82a5e3d0a56468b7719cd3c03dcd90ab281fb64",
            KafkaConsumerGroupNames.Reply("orders", "inventory")
        );
        Assert.NotEqual(
            KafkaConsumerGroupNames.Request("orders", "inventory"),
            KafkaConsumerGroupNames.Reply("orders", "inventory")
        );
    }

    [Fact]
    public void Address_normalization_preserves_ordinal_group_and_subscription_components()
    {
        Assert.Equal(
            KafkaConsumerGroupNames.Request("orders", "audit"),
            KafkaConsumerGroupNames.Request("orders", " audit ")
        );
        Assert.Equal(
            KafkaConsumerGroupNames.Subscription("orders", "audit", "shipping"),
            KafkaConsumerGroupNames.Subscription("orders", " audit ", "shipping")
        );
        Assert.NotEqual(
            KafkaConsumerGroupNames.Request("orders", "audit"),
            KafkaConsumerGroupNames.Request("Orders", "audit")
        );
        Assert.NotEqual(
            KafkaConsumerGroupNames.Subscription("orders", "audit", "shipping"),
            KafkaConsumerGroupNames.Subscription("orders", "audit", " shipping ")
        );
    }

    [Fact]
    public void Long_unicode_components_keep_physical_names_bounded()
    {
        var component = string.Concat(Enumerable.Repeat("catalog-\u00e9-\u4e66", 1000));
        var request = KafkaConsumerGroupNames.Request(component, component);
        var subscription = KafkaConsumerGroupNames.Subscription(component, component, component);
        Assert.InRange(request.Length, 1, 255);
        Assert.InRange(subscription.Length, 1, 255);
        Assert.NotEqual(request, subscription);
        Assert.NotEqual(request, KafkaConsumerGroupNames.Request(component + ".eu", component));
    }
}
