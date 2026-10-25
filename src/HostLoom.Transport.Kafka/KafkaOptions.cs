using Confluent.Kafka;

namespace HostLoom.Transport.Kafka;

public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>Stable group prefix shared by instances of the same logical service.</summary>
    public string ConsumerGroup { get; set; } = "hostloom";

    /// <summary>
    /// Topic on which this client service receives replies. Provision it with enough retention for
    /// the maximum request timeout. Each client instance uses a unique consumer group, starts at
    /// the end of the topic, and filters by correlation id. Give every calling service its own
    /// response topic, so a handler's produce ACL names exactly the topics its callers own.
    /// </summary>
    public string ResponseTopic { get; set; } = "hostloom.responses";

    public string ClientId { get; set; } =
        $"{Environment.MachineName}-{Environment.ProcessId}-{Guid.NewGuid():N}";

    public bool EnableIdempotence { get; set; } = true;

    /// <summary>
    /// Reply topics a request may name in its <c>hostloom-reply-to</c> header. Empty by default,
    /// which rejects every request before its handler runs. Add each caller's response topic.
    /// A topic outside this set is malformed unless <see cref="AllowAnyReplyTopic"/> is enabled.
    /// </summary>
    public ISet<string> AllowedReplyTopics { get; } = new HashSet<string>(StringComparer.Ordinal);

    /// <summary>
    /// Explicitly permits requests to name any syntactically valid reply topic, bypassing
    /// <see cref="AllowedReplyTopics"/>. Off by default. Enable only when every request producer
    /// is trusted to direct publications using this service's credentials.
    /// </summary>
    public bool AllowAnyReplyTopic { get; set; }

    /// <summary>
    /// When set, a request record whose Kafka timestamp is older than this is rejected as
    /// malformed instead of handled: it is skipped and committed, never re-run. Bounds how far a
    /// retained or replayed request stream can be replayed into handlers. Unset by default.
    /// </summary>
    public TimeSpan? MaxRequestAge { get; set; }

    /// <summary>
    /// Wire security for every producer and consumer this transport builds. Unset, the client
    /// library's default of plaintext applies; set it to <see cref="Confluent.Kafka.SecurityProtocol.Ssl"/>
    /// or <see cref="Confluent.Kafka.SecurityProtocol.SaslSsl"/> for anything but a local broker.
    /// </summary>
    public SecurityProtocol? SecurityProtocol { get; set; }

    /// <summary>SASL mechanism used with <see cref="SecurityProtocol"/> <c>SaslPlaintext</c> or <c>SaslSsl</c>.</summary>
    public SaslMechanism? SaslMechanism { get; set; }

    /// <summary>SASL user name. Requires a SASL <see cref="SecurityProtocol"/>.</summary>
    public string? SaslUsername { get; set; }

    /// <summary>
    /// SASL password. Requires a SASL <see cref="SecurityProtocol"/>. It is handed to the client
    /// configuration only; the transport never logs it or includes it in an exception.
    /// </summary>
    public string? SaslPassword { get; set; }

    /// <summary>Path to the CA certificate bundle that signs the brokers' TLS certificates.</summary>
    public string? SslCaLocation { get; set; }

    /// <summary>
    /// Runs on every <see cref="ProducerConfig"/> and <see cref="ConsumerConfig"/> after the
    /// transport's own settings and the typed security options above have been applied, so a
    /// deployment can set anything the client library supports: client certificates, OAuth
    /// bearer settings, socket and timeout tuning.
    /// </summary>
    public Action<ClientConfig>? ConfigureClient { get; set; }
}
