using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// The deliberate asymmetry for a valid envelope whose body fails deserialization, pinned end to
/// end at dispatcher level. A request is answered with an anonymous <c>HandlerFault</c> so the
/// transport can ack the frame, while an event throws <see cref="MalformedEnvelopeException"/>
/// to the transport, which keys its poison handling off that exception. The serializer alone is
/// covered in <c>MessageSerializationTests</c>; these tests cover what each dispatcher does with
/// the serializer's failure.
/// </summary>
public sealed class MalformedBodyDispatcherTests
{
    private static readonly string EchoType =
        $"{typeof(WireEnvelopeValidationTests.Echo).Assembly.GetName().Name}:{typeof(WireEnvelopeValidationTests.Echo).FullName}";

    private static readonly string EchoedType =
        $"{typeof(WireEnvelopeValidationTests.Echoed).Assembly.GetName().Name}:{typeof(WireEnvelopeValidationTests.Echoed).FullName}";

    private static readonly string PlacedType =
        $"{typeof(WireEnvelopeValidationTests.Placed).Assembly.GetName().Name}:{typeof(WireEnvelopeValidationTests.Placed).FullName}";

    [Fact]
    public async Task A_request_with_an_undecodable_body_is_answered_with_an_anonymous_handler_fault()
    {
        var handled = 0;
        using var host = await WireEnvelopeValidationTests.StartAsync(() => handled++);
        var messageId = Guid.NewGuid();
        var frame = Frame(
            $$"""
            {
              "messageId": "{{messageId}}",
              "kind": "Request",
              "messageType": "{{EchoType}}",
              "responseType": "{{EchoedType}}",
              "sentAt": "2026-09-20T10:00:00Z",
              "body": "{{UndecodableBody()}}"
            }
            """
        );

        var reply = await Broker(host).DeliverRequestAsync("echo", frame);

        using var envelope = JsonDocument.Parse(Encoding.UTF8.GetString(reply.Span));
        var root = envelope.RootElement;
        Assert.Equal("Fault", root.GetProperty("kind").GetString());
        Assert.Equal(messageId, root.GetProperty("correlationId").GetGuid());
        var fault = root.GetProperty("fault");
        Assert.Equal("HandlerFault", fault.GetProperty("errorType").GetString());
        Assert.Equal("The request handler failed.", fault.GetProperty("message").GetString());
        // The serializer's MalformedEnvelopeException names the CLR type; none of that crosses.
        var wire = Encoding.UTF8.GetString(reply.Span);
        Assert.DoesNotContain("deserialized", wire, StringComparison.Ordinal);
        Assert.DoesNotContain(nameof(JsonException), wire, StringComparison.Ordinal);
        Assert.Equal(0, handled);
    }

    [Fact]
    public async Task An_event_with_an_undecodable_body_is_poison_to_the_transport()
    {
        var handled = 0;
        using var host = await WireEnvelopeValidationTests.StartAsync(() => handled++);
        var frame = Frame(
            $$"""
            {
              "messageId": "{{Guid.NewGuid()}}",
              "kind": "Event",
              "messageType": "{{PlacedType}}",
              "responseType": "",
              "sentAt": "2026-09-20T10:00:00Z",
              "body": "{{UndecodableBody()}}"
            }
            """
        );

        var exception = await Assert.ThrowsAsync<MalformedEnvelopeException>(async () =>
            await Broker(host).DeliverEventAsync("orders", frame)
        );

        Assert.Contains("could not be deserialized", exception.Message, StringComparison.Ordinal);
        Assert.Equal(0, handled);
    }

    private static string UndecodableBody() => Convert.ToBase64String("not-json"u8.ToArray());

    private static byte[] Frame(string json) => Encoding.UTF8.GetBytes(json);

    private static WireEnvelopeValidationTests.FrameBroker Broker(IHost host) =>
        (WireEnvelopeValidationTests.FrameBroker)host.Services.GetRequiredService<IRequestBroker>();
}
