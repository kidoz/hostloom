using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;

namespace HostLoom.Redis;

/// <summary>
/// The one Redis connection a process holds. Created lazily on first use from
/// <see cref="RedisOptions"/>, or wrapped around an externally owned multiplexer, shared by the
/// cache store, the invalidation channel, and the lock provider, and disposed with the host when
/// the package created it.
/// </summary>
public sealed class RedisConnection : IAsyncDisposable
{
    private static int _sequence;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _connecting = new(1, 1);
    private readonly bool _owned;
    private IConnectionMultiplexer? _multiplexer;
    private long _reconnects;
    private int _disposed;

    /// <summary>Creates a connection that will be established from <paramref name="options"/> on first use.</summary>
    public RedisConnection(RedisOptions options, ILogger<RedisConnection>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.ThrowIfInvalid(nameof(options));
        Options = options;
        ClientName = $"{options.ClientName}-{Interlocked.Increment(ref _sequence)}";
        _logger = logger ?? NullLogger<RedisConnection>.Instance;
        _owned = options.ConnectionFactory is null;
        RedisDiagnostics.Register(this);
    }

    /// <summary>Wraps an externally owned multiplexer; it is never disposed by this class.</summary>
    public RedisConnection(
        IConnectionMultiplexer multiplexer,
        RedisOptions? options = null,
        ILogger<RedisConnection>? logger = null
    )
    {
        ArgumentNullException.ThrowIfNull(multiplexer);
        Options = options ?? new RedisOptions { Configuration = "external" };
        ClientName = multiplexer.ClientName ?? Options.ClientName;
        _logger = logger ?? NullLogger<RedisConnection>.Instance;
        _owned = false;
        Attach(multiplexer);
        RedisDiagnostics.Register(this);
    }

    /// <summary>The options this connection was built from.</summary>
    public RedisOptions Options { get; }

    /// <summary>
    /// The name the server sees for this connection: <see cref="RedisOptions.ClientName"/> with a
    /// per-connection suffix, so tracking can tell its own subscriber connection apart.
    /// </summary>
    public string ClientName { get; }

    /// <summary>Whether the multiplexer exists and reports itself connected.</summary>
    public bool IsConnected => _multiplexer?.IsConnected ?? false;

    /// <summary>Times the connection was restored after a failure.</summary>
    public long Reconnects => Interlocked.Read(ref _reconnects);

    /// <summary>Returns the multiplexer, establishing it on first call.</summary>
    public async ValueTask<IConnectionMultiplexer> GetMultiplexerAsync(
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (_multiplexer is { } existing)
        {
            return existing;
        }

        await _connecting.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_multiplexer is { } raced)
            {
                return raced;
            }

            IConnectionMultiplexer created;
            if (Options.ConnectionFactory is { } factory)
            {
                created = await factory(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var configuration = Options.BuildConfiguration();
                configuration.ClientName = ClientName;
                created = await ConnectionMultiplexer
                    .ConnectAsync(configuration)
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
            }

            Attach(created);
            return created;
        }
        finally
        {
            _connecting.Release();
        }
    }

    /// <summary>The database selected by <see cref="RedisOptions.DatabaseIndex"/>.</summary>
    public async ValueTask<IDatabase> GetDatabaseAsync(
        CancellationToken cancellationToken = default
    )
    {
        var multiplexer = await GetMultiplexerAsync(cancellationToken).ConfigureAwait(false);
        return multiplexer.GetDatabase(Options.DatabaseIndex);
    }

    /// <summary>
    /// The endpoints and client name, with any password redacted, for logs and probe output.
    /// </summary>
    public string Describe()
    {
        string target;
        if (Options.ConnectionFactory is not null)
        {
            target = "externally supplied multiplexer";
        }
        else if (Options.ConfigurationOptions is not null || Options.Configuration is not null)
        {
            try
            {
                target = Options.BuildConfiguration().ToString(includePassword: false);
            }
            catch (ArgumentException)
            {
                target = "(unparseable configuration)";
            }
        }
        else
        {
            target = "(unconfigured)";
        }

        return $"{target}; database {Options.DatabaseIndex}; client {ClientName}; hash tags {(Options.UseHashTags ? "on" : "off")}";
    }

    /// <summary>Raised after the multiplexer reports a restored connection.</summary>
    public event EventHandler? Restored;

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        RedisDiagnostics.Unregister(this);
        var multiplexer = _multiplexer;
        if (multiplexer is not null)
        {
            multiplexer.ConnectionFailed -= OnConnectionFailed;
            multiplexer.ConnectionRestored -= OnConnectionRestored;
            if (_owned)
            {
                await multiplexer.DisposeAsync().ConfigureAwait(false);
            }
        }

        _connecting.Dispose();
    }

    private void Attach(IConnectionMultiplexer multiplexer)
    {
        multiplexer.ConnectionFailed += OnConnectionFailed;
        multiplexer.ConnectionRestored += OnConnectionRestored;
        _multiplexer = multiplexer;
    }

    private void OnConnectionFailed(object? sender, ConnectionFailedEventArgs args) =>
        _logger.LogWarning(
            new EventId(1301, "RedisConnectionFailed"),
            "Redis connection to {EndPoint} failed ({FailureType}); commands fail open until it is restored.",
            args.EndPoint,
            args.FailureType
        );

    private void OnConnectionRestored(object? sender, ConnectionFailedEventArgs args)
    {
        Interlocked.Increment(ref _reconnects);
        RedisDiagnostics.Reconnects.Add(
            1,
            new KeyValuePair<string, object?>(RedisDiagnostics.ClientTag, Options.ClientName)
        );
        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation(
                new EventId(1302, "RedisConnectionRestored"),
                "Redis connection to {EndPoint} restored.",
                args.EndPoint
            );
        }

        Restored?.Invoke(this, EventArgs.Empty);
    }
}
