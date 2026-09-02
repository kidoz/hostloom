using StackExchange.Redis;

namespace HostLoom.Redis;

/// <summary>
/// How the process reaches Redis. One connection serves both the cache store and the lock
/// provider. Every duration is a <see cref="TimeSpan"/>; credentials come from
/// <see cref="Configuration"/> or <see cref="ConfigurationOptions"/> and are never logged.
/// </summary>
public sealed class RedisOptions
{
    /// <summary>A StackExchange.Redis configuration string, for example <c>localhost:6379,password=…</c>.</summary>
    public string? Configuration { get; set; }

    /// <summary>A prebuilt configuration; takes precedence over <see cref="Configuration"/>.</summary>
    public ConfigurationOptions? ConfigurationOptions { get; set; }

    /// <summary>
    /// Supplies an externally owned multiplexer instead of one created from the options. The
    /// package then never disposes it. Takes precedence over both configuration properties.
    /// </summary>
    public Func<CancellationToken, Task<IConnectionMultiplexer>>? ConnectionFactory { get; set; }

    /// <summary>
    /// Logical database. Exists for coexistence with keys from a previous library during a
    /// migration; separation between services is by key prefix, never by database index.
    /// </summary>
    public int DatabaseIndex { get; set; }

    /// <summary>
    /// Wraps the namespace segment of every key in <c>{…}</c> so all of a service's keys share
    /// one Redis Cluster slot.
    /// </summary>
    public bool UseHashTags { get; set; }

    /// <summary>Time allowed to establish a connection.</summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Time allowed for one command.</summary>
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Time allowed for the readiness <c>PING</c>.</summary>
    public TimeSpan HealthTimeout { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// When <see langword="true"/>, an unreachable Redis fails host startup. The default lets the
    /// service start, reports readiness unhealthy, and serves from factories until Redis recovers.
    /// </summary>
    public bool FailFast { get; set; }

    /// <summary>
    /// Name reported to the server, visible in <c>CLIENT LIST</c>. Each connection appends a
    /// short per-connection suffix so tracking can find its own subscriber connection.
    /// </summary>
    public string ClientName { get; set; } =
        $"hostloom-{Environment.MachineName}-{Environment.ProcessId}";

    /// <summary>Attempts to enable server-assisted tracking after the subscription exists.</summary>
    public int MaxClientCommandRetries { get; set; } = 3;

    /// <summary>Delay before the first retry of a <c>CLIENT</c> command; doubles per attempt.</summary>
    public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>Every violation, each naming the option key it concerns.</summary>
    public IReadOnlyList<string> Validate()
    {
        var problems = new List<string>();
        if (
            ConnectionFactory is null
            && ConfigurationOptions is null
            && string.IsNullOrWhiteSpace(Configuration)
        )
        {
            problems.Add(
                "Redis:Configuration is required (or set Redis:ConfigurationOptions or Redis:ConnectionFactory)."
            );
        }

        if (DatabaseIndex < 0)
        {
            problems.Add("Redis:DatabaseIndex must not be negative.");
        }

        if (ConnectTimeout <= TimeSpan.Zero)
        {
            problems.Add("Redis:ConnectTimeout must be positive.");
        }

        if (CommandTimeout <= TimeSpan.Zero)
        {
            problems.Add("Redis:CommandTimeout must be positive.");
        }

        if (HealthTimeout <= TimeSpan.Zero)
        {
            problems.Add("Redis:HealthTimeout must be positive.");
        }

        if (string.IsNullOrWhiteSpace(ClientName))
        {
            problems.Add("Redis:ClientName must not be empty.");
        }

        if (MaxClientCommandRetries < 0)
        {
            problems.Add("Redis:MaxClientCommandRetries must not be negative.");
        }

        if (InitialRetryDelay <= TimeSpan.Zero)
        {
            problems.Add("Redis:InitialRetryDelay must be positive.");
        }

        return problems;
    }

    internal void ThrowIfInvalid(string parameterName)
    {
        var problems = Validate();
        if (problems.Count > 0)
        {
            throw new ArgumentException(
                "RedisOptions are invalid: " + string.Join(" ", problems),
                parameterName
            );
        }
    }

    /// <summary>
    /// The configuration handed to StackExchange.Redis: a clone, so the caller's instance is not
    /// mutated, with the client name, timeouts, and fail-open connect behaviour applied.
    /// </summary>
    internal ConfigurationOptions BuildConfiguration()
    {
        var configuration =
            ConfigurationOptions?.Clone()
            ?? StackExchange.Redis.ConfigurationOptions.Parse(Configuration!);
        configuration.ClientName = ClientName;
        configuration.ConnectTimeout = (int)ConnectTimeout.TotalMilliseconds;
        configuration.AsyncTimeout = (int)CommandTimeout.TotalMilliseconds;
        configuration.SyncTimeout = (int)CommandTimeout.TotalMilliseconds;
        // Fail-open by default: the multiplexer keeps reconnecting in the background and every
        // command meanwhile fails fast with a connection exception the cache maps to "degraded".
        configuration.AbortOnConnectFail = FailFast;
        // Client tracking needs CLIENT LIST and CLIENT TRACKING, which StackExchange.Redis gates
        // behind its client-side admin flag. This grants nothing on the server; ACLs still apply.
        configuration.AllowAdmin = true;
        // RESP2 keeps pub/sub on a dedicated connection, which is the connection client tracking
        // redirects invalidations to; under RESP3 the library multiplexes both over one socket.
        configuration.Protocol = RedisProtocol.Resp2;
        return configuration;
    }
}
