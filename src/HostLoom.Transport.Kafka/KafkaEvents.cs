using Microsoft.Extensions.Logging;

namespace HostLoom.Transport.Kafka;

/// <summary>
/// Stable event ids for every log line the Kafka transport writes, in the 1421 to 1430 block of
/// the transport range, so a logging pipeline can filter or alert on one condition without
/// matching message text. Consumer-loop lines come from listeners, subscriptions, and the reply
/// consumer alike, and name the topic the loop consumes.
/// </summary>
internal static class KafkaEvents
{
    /// <summary>Error: consuming failed; the loop waits and consumes again.</summary>
    public static readonly EventId ConsumeFailed = new(1421, "KafkaConsumeFailed");

    /// <summary>
    /// Error: a record could not be decoded, or a request failed the checks made before its
    /// handler runs; it is committed past and never handled.
    /// </summary>
    public static readonly EventId RecordMalformed = new(1422, "KafkaRecordMalformed");

    /// <summary>
    /// Error: a request was handled but its reply could not be produced; the request is committed
    /// past without running the handler again, and the caller times out.
    /// </summary>
    public static readonly EventId ReplyUnroutable = new(1423, "KafkaReplyUnroutable");

    /// <summary>Error: handling a record failed; the loop rewinds to it and retries after a backoff.</summary>
    public static readonly EventId RecordRewound = new(1424, "KafkaRecordRewound");

    /// <summary>Error: a request record failed on every attempt; it is committed past and dropped.</summary>
    public static readonly EventId RecordAttemptsExhausted = new(
        1425,
        "KafkaRecordAttemptsExhausted"
    );

    /// <summary>
    /// Error: rewinding to a failed record failed, most likely because its partition was revoked;
    /// whoever is assigned the partition next redelivers the record.
    /// </summary>
    public static readonly EventId SeekFailed = new(1426, "KafkaSeekFailed");

    /// <summary>Error: committing a handled or skipped record failed; the handler is not run again.</summary>
    public static readonly EventId CommitFailed = new(1427, "KafkaCommitFailed");

    /// <summary>Error: the consumer loop itself faulted, which is a defect; it had stopped consuming.</summary>
    public static readonly EventId LoopFaulted = new(1428, "KafkaConsumerLoopFaulted");

    /// <summary>
    /// Error: closing a stopped consumer failed, so the group may wait out the session timeout
    /// before it rebalances; the consumer is disposed all the same.
    /// </summary>
    public static readonly EventId ConsumerCloseFailed = new(1429, "KafkaConsumerCloseFailed");

    /// <summary>
    /// Debug: disposing a consumer whose subscription failed at startup failed too; the startup
    /// failure is the one reported.
    /// </summary>
    public static readonly EventId ConsumerCleanupFailed = new(1430, "KafkaConsumerCleanupFailed");
}
