using ValkeyDotNet;

namespace HostLoom.Valkey;

/// <summary>
/// Owns a lazily connected standalone command client with bounded recovery. Ordinary operations
/// never replay transport failures. Stores and providers borrow this connection; its creator disposes it.
/// </summary>
public sealed class ValkeyConnection : IAsyncDisposable
{
    private readonly ValkeyConnectionOwner _owner;
    internal ValkeyOptions Settings { get; }

    /// <summary>Validates settings without opening a socket.</summary>
    public ValkeyConnection(ValkeyOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Settings = options.Snapshot();
        _owner = new ValkeyConnectionOwner(
            new ValkeyConnectionOwnerOptions
            {
                Connection = Settings.Connection,
                EnableTelemetry = Settings.EnableTelemetry,
                MaxCommandRetries = 0,
            }
        );
    }

    /// <summary>A snapshot of connection state; use a health probe to establish readiness.</summary>
    public ValkeyConnectionState State => _owner.State;

    internal Task<RespValue> ExecuteAsync(ValkeyCommand command, CancellationToken token) =>
        _owner.ExecuteWithDeadlineAsync(command, Settings.CommandTimeout, token);

    internal Task<RespValue> ScriptAsync(
        ValkeyScript script,
        IReadOnlyList<ValkeyArgument> keys,
        IReadOnlyList<ValkeyArgument> arguments,
        CancellationToken token
    ) =>
        _owner.ExecuteScriptWithDeadlineAsync(
            script,
            keys,
            arguments,
            Settings.CommandTimeout,
            token
        );

    internal async Task<IReadOnlyList<RespValue>> PipelineAsync(
        IReadOnlyList<ValkeyCommand> commands,
        CancellationToken token
    )
    {
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
        var reply = await _owner
            .ExecuteWithDeadlineAsync(new ValkeyCommand("PING"), Settings.HealthTimeout, token)
            .ConfigureAwait(false);
        if (!string.Equals(reply.AsString(), "PONG", StringComparison.Ordinal))
        {
            throw new ValkeyProtocolException("Unexpected PING response.");
        }
    }

    /// <summary>Disposes the command owner and prevents further operations. Safe to call repeatedly.</summary>
    public ValueTask DisposeAsync() => _owner.DisposeAsync();
}
