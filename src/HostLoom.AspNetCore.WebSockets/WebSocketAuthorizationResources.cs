namespace HostLoom.AspNetCore.WebSockets;

/// <summary>The resource supplied to an operation's ASP.NET Core authorization policy.</summary>
public sealed record WebSocketOperationResource(string Operation, RequestAddress Destination);

/// <summary>The resource supplied to a topic's ASP.NET Core authorization policy.</summary>
public sealed record WebSocketTopicResource(string Topic);
