using System.Security.Claims;

namespace HostLoom.AspNetCore.WebSockets;

/// <summary>Loads current topic values when a WebSocket subscription starts.</summary>
/// <typeparam name="TEvent">The event contract exposed by the topic.</typeparam>
public interface IWebSocketTopicSnapshotProvider<TEvent>
    where TEvent : class, IEvent
{
    /// <summary>
    /// Streams current values for the requested topic and optional key. The principal is valid only
    /// for this enumeration and must not be retained by the provider. Implementations must honor
    /// <paramref name="cancellationToken"/>.
    /// </summary>
    IAsyncEnumerable<TEvent> GetSnapshotAsync(
        WebSocketTopicSnapshotContext context,
        CancellationToken cancellationToken = default
    );
}

/// <summary>Describes the subscription whose initial snapshot is being loaded.</summary>
/// <param name="Topic">The registered public topic name.</param>
/// <param name="Key">The requested key, or <see langword="null"/> for a topic-wide snapshot.</param>
/// <param name="User">The session principal, valid only for the snapshot enumeration.</param>
public sealed record WebSocketTopicSnapshotContext(string Topic, string? Key, ClaimsPrincipal User);
