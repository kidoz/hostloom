namespace HostLoom.Transport.Kafka;

/// <summary>
/// Validates the reply topic a request names. The header is written by the caller, so it is
/// checked against Kafka's topic-name rules and, when configured, an explicit allow list before
/// the handler runs; a rejected request is a malformed envelope, committed and skipped.
/// </summary>
internal static class KafkaReplyTopic
{
    /// <summary>Kafka's own limit on a topic name.</summary>
    internal const int MaxLength = 249;

    /// <summary>
    /// Whether <paramref name="name"/> is a legal Kafka topic name: non-empty, at most
    /// <see cref="MaxLength"/> characters of <c>[a-zA-Z0-9._-]</c>, and neither <c>.</c> nor <c>..</c>.
    /// </summary>
    internal static bool IsValidTopicName(string? name)
    {
        if (string.IsNullOrEmpty(name) || name.Length > MaxLength || name is "." or "..")
        {
            return false;
        }

        foreach (var character in name)
        {
            if (!char.IsAsciiLetterOrDigit(character) && character is not ('.' or '_' or '-'))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Returns <paramref name="replyTo"/> when it may be produced to, or throws
    /// <see cref="MalformedEnvelopeException"/>. The rejected value is deliberately not echoed:
    /// it is caller-controlled and would otherwise land in the log verbatim.
    /// </summary>
    internal static string Require(string replyTo, ISet<string> allowed)
    {
        if (!IsValidTopicName(replyTo))
        {
            throw new MalformedEnvelopeException(
                "Kafka request header 'hostloom-reply-to' is not a valid topic name."
            );
        }

        if (allowed.Count > 0 && !allowed.Contains(replyTo))
        {
            throw new MalformedEnvelopeException(
                "Kafka request header 'hostloom-reply-to' names a topic outside KafkaOptions.AllowedReplyTopics."
            );
        }

        return replyTo;
    }
}
