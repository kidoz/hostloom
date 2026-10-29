using System.Globalization;
using ValkeyDotNet;

namespace HostLoom.Valkey;

/// <summary>
/// Owns a lazily connected standalone command client with bounded recovery, or borrows a
/// caller-supplied owner. Ordinary operations never replay transport failures. Stores and
/// providers borrow this connection; its creator disposes it.
/// </summary>
public sealed class ValkeyConnection : IAsyncDisposable
{
    private readonly ValkeyConnectionOwner _owner;
    private readonly bool _owned;
    private readonly Lock _subscriberGate = new();
    private ValkeyClientOptions? _resolvedSubscriberConnection;
    private int _disposed;
    internal ValkeyOptions Settings { get; }
    internal ValkeyClientOptions SubscriberConnection { get; }

    /// <summary>Validates settings without opening a socket.</summary>
    public ValkeyConnection(ValkeyOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Settings = options.Snapshot();
        var borrowed = Settings.ConnectionOwner;
        _owned = borrowed is null;
        _owner =
            borrowed
            ?? new ValkeyConnectionOwner(
                new ValkeyConnectionOwnerOptions
                {
                    Connection = Settings.Connection,
                    EnableTelemetry = Settings.EnableTelemetry,
                    MaxCommandRetries = 0,
                }
            );
        SubscriberConnection = borrowed is null
            ? Settings.Connection
            : new ValkeyClientOptions
            {
                Host = borrowed.Host,
                Port = borrowed.Port,
                Username = Settings.Connection.Username,
                Password = Settings.Connection.Password,
                ClientName = Settings.Connection.ClientName,
                Database = Settings.Connection.Database,
                Protocol = Settings.Connection.Protocol,
                UseTls = Settings.Connection.UseTls,
                CertificateValidationCallback = Settings.Connection.CertificateValidationCallback,
                ConnectTimeout = Settings.Connection.ConnectTimeout,
                ResponseDrainTimeout = Settings.Connection.ResponseDrainTimeout,
                MaxPendingRequests = Settings.Connection.MaxPendingRequests,
                MaxResponseBytes = Settings.Connection.MaxResponseBytes,
                MaxResponseElements = Settings.Connection.MaxResponseElements,
                MaxNestingDepth = Settings.Connection.MaxNestingDepth,
            };
    }

    /// <summary>A snapshot of connection state; use a health probe to establish readiness.</summary>
    public ValkeyConnectionState State => _owner.State;

    internal Task<RespValue> ExecuteAsync(ValkeyCommand command, CancellationToken token)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        return _owner.ExecuteWithDeadlineAsync(command, Settings.CommandTimeout, token);
    }

    internal Task<RespValue> ScriptAsync(
        ValkeyScript script,
        IReadOnlyList<ValkeyArgument> keys,
        IReadOnlyList<ValkeyArgument> arguments,
        CancellationToken token
    )
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        return _owner.ExecuteScriptWithDeadlineAsync(
            script,
            keys,
            arguments,
            Settings.CommandTimeout,
            token
        );
    }

    internal async Task<IReadOnlyList<RespValue>> PipelineAsync(
        IReadOnlyList<ValkeyCommand> commands,
        CancellationToken token
    )
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        var replies = await _owner
            .ExecutePipelineWithDeadlineAsync(commands, Settings.CommandTimeout, token)
            .ConfigureAwait(false);
        foreach (var reply in replies)
        {
            reply.ThrowIfError();
        }
        return replies;
    }

    internal async Task<ValkeyClientOptions> GetSubscriberConnectionAsync(CancellationToken token)
    {
        lock (_subscriberGate)
        {
            if (_resolvedSubscriberConnection is { } cached)
                return cached;
        }

        if (_owned)
            return SubscriberConnection;

        var reply = await ExecuteAsync(new ValkeyCommand("CLIENT", "INFO"), token)
            .ConfigureAwait(false);
        reply.ThrowIfError();
        var database = ReadDatabase(reply.AsString());
        var current = SubscriberConnection;
        var resolved = new ValkeyClientOptions
        {
            Host = current.Host,
            Port = current.Port,
            Username = current.Username,
            Password = current.Password,
            ClientName = current.ClientName,
            Database = database,
            Protocol = current.Protocol,
            UseTls = current.UseTls,
            CertificateValidationCallback = current.CertificateValidationCallback,
            ConnectTimeout = current.ConnectTimeout,
            ResponseDrainTimeout = current.ResponseDrainTimeout,
            MaxPendingRequests = current.MaxPendingRequests,
            MaxResponseBytes = current.MaxResponseBytes,
            MaxResponseElements = current.MaxResponseElements,
            MaxNestingDepth = current.MaxNestingDepth,
        };
        lock (_subscriberGate)
            return _resolvedSubscriberConnection ??= resolved;
    }

    private static int ReadDatabase(string? info)
    {
        foreach (var field in (info ?? "").Split(' ', StringSplitOptions.RemoveEmptyEntries))
        {
            if (
                field.StartsWith("db=", StringComparison.Ordinal)
                && int.TryParse(
                    field.AsSpan(3),
                    NumberStyles.None,
                    CultureInfo.InvariantCulture,
                    out var database
                )
            )
                return database;
        }

        throw new ValkeyProtocolException("CLIENT INFO did not report a valid database.");
    }

    internal async Task PingAsync(CancellationToken token)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        var reply = await _owner
            .ExecuteWithDeadlineAsync(new ValkeyCommand("PING"), Settings.HealthTimeout, token)
            .ConfigureAwait(false);
        if (!string.Equals(reply.AsString(), "PONG", StringComparison.Ordinal))
        {
            throw new ValkeyProtocolException("Unexpected PING response.");
        }
    }

    /// <summary>
    /// Disposes an owned command owner and prevents further operations through this wrapper. A
    /// caller-supplied owner remains caller-owned. Safe to call repeatedly.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0 || !_owned)
        {
            return ValueTask.CompletedTask;
        }

        return _owner.DisposeAsync();
    }
}
