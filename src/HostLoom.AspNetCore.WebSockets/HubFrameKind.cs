using System.Text.Json.Serialization;

namespace HostLoom.AspNetCore.WebSockets;

[JsonConverter(typeof(JsonStringEnumConverter<HubFrameKind>))]
public enum HubFrameKind
{
    None = 0,
    Welcome = 1,
    Request = 2,
    Response = 3,
    Fault = 4,
    Cancel = 5,
    Subscribe = 6,
    Subscribed = 7,
    Event = 8,
    Credit = 9,
    Ack = 10,
    Unsubscribe = 11,
    Complete = 12,
}
