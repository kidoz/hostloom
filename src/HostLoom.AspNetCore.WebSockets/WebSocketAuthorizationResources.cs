namespace HostLoom.AspNetCore.WebSockets;

/// <summary>The resource supplied to an operation's ASP.NET Core authorization policy.</summary>
public sealed record WebSocketOperationResource(string Operation, RequestAddress Destination);

/// <summary>The resource supplied to a topic's ASP.NET Core authorization policy.</summary>
/// <param name="Topic">The registered public topic name.</param>
/// <param name="Key">The client-selected subscription key, when present.</param>
public sealed record WebSocketTopicResource(string Topic, string? Key)
{
    /// <summary>Creates a topic resource without a subscription key.</summary>
    public WebSocketTopicResource(string topic)
        : this(topic, null) { }

    /// <summary>Deconstructs the resource using its original topic-only shape.</summary>
    public void Deconstruct(out string topic) => topic = Topic;
}
