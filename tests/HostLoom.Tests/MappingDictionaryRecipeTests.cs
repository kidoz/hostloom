using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Pins the properties the mapping documentation claims for the dictionary recipe it offers in
/// place of a dictionary extension: independent result, explicit comparer, and a throw on a key
/// that collides under that comparer.
/// </summary>
public sealed class MappingDictionaryRecipeTests
{
    [Fact]
    public void The_copy_owns_a_new_dictionary_with_the_comparer_named_at_the_call_site()
    {
        var source = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Region"] = "eu",
        };

        var copy = new Dictionary<string, string>(source, StringComparer.Ordinal);
        source["Region"] = "us";

        Assert.Equal("eu", copy["Region"]);
        Assert.False(copy.ContainsKey("region"));
        Assert.Same(StringComparer.Ordinal, copy.Comparer);
    }

    [Fact]
    public void A_key_that_collides_under_the_destination_comparer_throws()
    {
        var source = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["Region"] = "eu",
            ["region"] = "us",
        };

        Assert.Throws<ArgumentException>(() =>
            new Dictionary<string, string>(source, StringComparer.OrdinalIgnoreCase)
        );
        Assert.Throws<ArgumentException>(() =>
            source.ToDictionary(
                pair => pair.Key,
                pair => pair.Value,
                StringComparer.OrdinalIgnoreCase
            )
        );
    }
}
