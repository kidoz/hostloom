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
