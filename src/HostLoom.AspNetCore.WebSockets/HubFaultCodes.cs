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
}
