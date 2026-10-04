using HostLoom.Generators;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// The <see cref="GuidStringIdGenerator"/> presets emit BCL GUID strings in their pinned
/// formats — dashed (<c>"D"</c>, 36 characters, 4 hyphens) and compact (<c>"N"</c>, 32
/// characters, no hyphens) — and both are <see cref="IStringIdGenerator"/> implementations.
/// </summary>
public sealed class GuidStringIdGeneratorTests
{
    [Fact]
    public void The_dashed_format_is_36_characters_with_4_hyphens_and_parses()
    {
        var id = GuidStringIdGenerator.Dashed.CreateStringId();

        Assert.Equal(36, id.Length);
        Assert.Equal(4, id.Count(c => c == '-'));
        Assert.True(Guid.TryParse(id, out _));
    }

    [Fact]
    public void The_compact_format_is_32_hyphenless_characters_and_parses_as_n()
    {
        var id = GuidStringIdGenerator.Compact.CreateStringId();

        Assert.Equal(32, id.Length);
        Assert.DoesNotContain('-', id);
        Assert.True(Guid.TryParseExact(id, "N", out _));
    }

    [Fact]
    public void Both_presets_implement_the_string_id_generator_seam()
    {
        Assert.IsAssignableFrom<IStringIdGenerator>(GuidStringIdGenerator.Dashed);
        Assert.IsAssignableFrom<IStringIdGenerator>(GuidStringIdGenerator.Compact);
    }
}
