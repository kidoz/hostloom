namespace HostLoom.AspNetCore.WebSockets;

public static class HubFaultCodes
{
    public const string InvalidFrame = "invalid_frame";
    public const string InvalidPayload = "invalid_payload";
    public const string OperationNotFound = "operation_not_found";
    public const string TopicNotFound = "topic_not_found";
    public const string Forbidden = "forbidden";
    public const string RequestTimeout = "request_timeout";
    public const string RequestFailed = "request_failed";
    public const string Canceled = "canceled";
    public const string DuplicateStream = "duplicate_stream";
    public const string CapacityExceeded = "capacity_exceeded";
    public const string SnapshotFailed = "snapshot_failed";

    /// <summary>
    /// Snapshot initialization did not finish within
    /// <see cref="HostLoomWebSocketOptions.SnapshotInitializationTimeout"/>, typically because the
    /// client withheld credit. The subscription is removed before the fault is sent.
    /// </summary>
    public const string SnapshotStalled = "snapshot_stalled";

    /// <summary>
    /// The encoded response exceeded <see cref="HostLoomWebSocketOptions.MaximumMessageSize"/>.
    /// The request stream ends with this fault; the session stays open.
    /// </summary>
    public const string MessageTooLarge = "message_too_large";
}
