namespace HostLoom.AspNetCore.WebSockets;

public sealed class HostLoomWebSocketOptions
{
    public int MaximumMessageSize { get; set; } = 64 * 1024;

    public int ReceiveBufferSize { get; set; } = 4 * 1024;

    public int MaximumQueuedBytesPerConnection { get; set; } = 256 * 1024;

    public int MaximumQueuedFramesPerConnection { get; set; } = 512;

    public int MaximumConcurrentRequestsPerConnection { get; set; } = 8;

    public int MaximumSubscriptionsPerConnection { get; set; } = 32;

    public int MaximumCreditPerSubscription { get; set; } = 1024;

    public TimeSpan DefaultRequestTimeout { get; set; } = TimeSpan.FromSeconds(10);

    public TimeSpan MaximumRequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public bool RequireAuthenticatedUser { get; set; } = true;

    public bool IncludeRemoteFaultMessages { get; set; }

    public IList<string> ProtocolPreference { get; } =
    [
        MessagePackWebSocketHubProtocol.ProtocolName,
        ProtobufWebSocketHubProtocol.ProtocolName,
        JsonWebSocketHubProtocol.ProtocolName,
    ];

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaximumMessageSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ReceiveBufferSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaximumQueuedBytesPerConnection);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaximumQueuedFramesPerConnection);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaximumConcurrentRequestsPerConnection);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaximumSubscriptionsPerConnection);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaximumCreditPerSubscription);

        if (DefaultRequestTimeout <= TimeSpan.Zero || DefaultRequestTimeout > MaximumRequestTimeout)
        {
            throw new InvalidOperationException(
                "The default request timeout must be positive and no greater than the maximum request timeout."
            );
        }

        if (MaximumRequestTimeout <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("The maximum request timeout must be positive.");
        }

        if (ProtocolPreference.Count == 0 || ProtocolPreference.Any(string.IsNullOrWhiteSpace))
        {
            throw new InvalidOperationException(
                "At least one valid WebSocket subprotocol must be preferred."
            );
        }
    }
}
