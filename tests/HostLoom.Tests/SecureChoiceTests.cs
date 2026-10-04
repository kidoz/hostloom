using HostLoom.Generators;
using HostLoom.Generators.Testing;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// <see cref="SecureChoice"/> selects one element of an indexed collection with equal weight
/// over [0, Count): a singleton returns its item, a scripted index picks the scripted element,
/// and null or empty collections are rejected.
/// </summary>
public sealed class SecureChoiceTests
{
    [Fact]
    public void A_singleton_list_returns_its_item()
    {
        Assert.Equal("orders", SecureChoice.Choose(["orders"]));
    }

    [Fact]
    public void An_empty_list_is_rejected()
    {
        var exception = Assert.Throws<ArgumentException>(() =>
            SecureChoice.Choose(Array.Empty<string>())
        );
        Assert.Contains("at least one element", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_null_list_is_rejected()
    {
        Assert.Throws<ArgumentNullException>(() => SecureChoice.Choose<string>(null!));
    }

    [Fact]
    public void A_scripted_index_picks_the_scripted_element()
    {
        var items = new[] { "orders", "invoices", "inventory", "catalog", "shipments" };

        var chosen = SecureChoice.Choose(items, new ScriptedRandomSource(2));

        Assert.Equal("inventory", chosen);
    }

    [Fact]
    public void Every_chosen_value_is_a_member_of_the_list()
    {
        var items = new[] { "orders", "invoices", "inventory", "catalog", "shipments" };

        for (var index = 0; index < 128; index++)
        {
            Assert.Contains(SecureChoice.Choose(items), items);
        }
    }
}
