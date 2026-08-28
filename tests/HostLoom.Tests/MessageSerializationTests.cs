using System.Text.Json;
using Xunit;

namespace HostLoom.Tests;

public sealed class MessageSerializationTests
{
    [Fact]
    public void Invalid_json_is_reported_as_a_malformed_envelope()
    {
        var serializer = new SystemTextJsonMessageSerializer();

        var exception = Assert.Throws<MalformedEnvelopeException>(() =>
            ((IMessageSerializer)serializer).Deserialize<Greeting>("not-json"u8)
        );

        Assert.IsType<JsonException>(exception.InnerException);
    }

    private sealed record Greeting(string Text);
}
