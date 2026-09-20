using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// The inbox key must be one-to-one with (topic, subscription, message id). Both names may contain
/// the separator, so they are length-prefixed; a bare join would let two subscriptions share a key
/// and one delivery silence the other.
/// </summary>
public sealed class InboxKeyTests
{
    private static readonly Guid Id = Guid.Parse("11111111-2222-3333-4444-555555555555");

    [Fact]
    public void The_key_length_prefixes_the_topic_and_subscription()
    {
        Assert.Equal(
            "6:orders:5:audit:11111111222233334444555555555555",
            InboxFilter.KeyFor("orders", "audit", Id)
        );
    }

    [Fact]
    public void Names_containing_the_separator_do_not_collide()
    {
        // With a plain "{topic}:{subscription}:{id}" both pairs would read "catalog:eu:updates:…".
        var first = InboxFilter.KeyFor("catalog:eu", "updates", Id);
        var second = InboxFilter.KeyFor("catalog", "eu:updates", Id);

        Assert.NotEqual(first, second);
        Assert.Equal("10:catalog:eu:7:updates:11111111222233334444555555555555", first);
        Assert.Equal("7:catalog:10:eu:updates:11111111222233334444555555555555", second);
    }

    [Fact]
    public void Numeric_looking_names_cannot_forge_a_length_prefix()
    {
        // "1:a" as a topic versus a topic "a" with the subscription "1:a:…" style inputs.
        var first = InboxFilter.KeyFor("1:a", "b", Id);
        var second = InboxFilter.KeyFor("1", "a:1:b", Id);

        Assert.NotEqual(first, second);
    }

    [Fact]
    public void Different_message_ids_and_subscriptions_produce_different_keys()
    {
        var keys = new HashSet<string>(StringComparer.Ordinal)
        {
            InboxFilter.KeyFor("orders", "audit", Id),
            InboxFilter.KeyFor("orders", "shipping", Id),
            InboxFilter.KeyFor("orders", "audit", Guid.NewGuid()),
            InboxFilter.KeyFor("invoices", "audit", Id),
        };

        Assert.Equal(4, keys.Count);
    }
}
