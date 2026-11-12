using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using HostLoom.DataGrid;
using HostLoom.DataGrid.ProcessProbe;
using HostLoom.DataGrid.Runtime;
using HostLoom.DataGrid.Storage;
using HostLoom.DataGrid.Transport;

if (!OperatingSystem.IsLinux() || args.Length is not (9 or 10))
    throw new InvalidOperationException(
        "Owned Linux process-test harness requires nine arguments and an optional storage pause."
    );
var directory = args[0];
var member = int.Parse(args[1], CultureInfo.InvariantCulture);
var epoch = long.Parse(args[2], CultureInfo.InvariantCulture);
var config = ProbeConfiguration.Create(
    args.Skip(3).Take(3).Select(s => int.Parse(s, CultureInfo.InvariantCulture)).ToArray()
);
var port = int.Parse(args[6], CultureInfo.InvariantCulture);
var clock = new ProbeClock(long.Parse(args[7], CultureInfo.InvariantCulture));
if (args[8] != "owned-test")
    throw new InvalidOperationException("Test ownership marker required.");
using var lifetime = new CancellationTokenSource(TimeSpan.FromMinutes(3));
var token = lifetime.Token;
using var root = X509CertificateLoader.LoadCertificateFromFile(Path.Combine(directory, "root.cer"));
using var certificate = X509CertificateLoader.LoadPkcs12FromFile(
    Path.Combine(directory, $"member-{member}.pfx"),
    null,
    X509KeyStorageFlags.EphemeralKeySet
);
var fingerprints = Enumerable
    .Range(0, 3)
    .Select(i => SHA256.HashData(File.ReadAllBytes(Path.Combine(directory, $"member-{i}.cer"))))
    .ToArray();
using var client = X509CertificateLoader.LoadCertificateFromFile(
    Path.Combine(directory, "client.cer")
);
var credentials = new DataGridPeerCredentials(
    config,
    member,
    certificate,
    root,
    fingerprints,
    SHA256.HashData("hostloom-process-probe-v1"u8)
);
var output = new ProbeOutput();
DataGridStorageStep? pauseStep =
    args.Length == 10
        ? (DataGridStorageStep)int.Parse(args[9], CultureInfo.InvariantCulture)
        : null;
if (
    pauseStep
    is not null
        and not (
            DataGridStorageStep.SnapshotFlushed
            or DataGridStorageStep.GenerationDirectoryFlushed
            or DataGridStorageStep.AnchorDirectoryFlushed
        )
)
    throw new InvalidOperationException("Unsupported storage pause.");
var storagePaused = 0;
var policy = new DataGridClientAccessPolicy(
    config,
    root,
    [
        new(
            ProbeConfiguration.Principal,
            client.GetCertHash(HashAlgorithmName.SHA256),
            true,
            [new(ProbeConfiguration.Map, DataGridMapActions.Read | DataGridMapActions.Mutate)]
        ),
    ]
);
await using var security = new DataGridSecurityContext(credentials, policy);
await using var runtime = OpenRuntime();
var receiver = new PausingReceiver(runtime, output);
using var listener = new TcpListener(IPAddress.Loopback, port);
await using var server = new DataGridClientServer(security, listener, receiver);
server.Start();
output.Write("READY");
while (await Console.In.ReadLineAsync(token) is { } line)
{
    switch (line)
    {
        case "status":
            var status = runtime.Status;
            output.Write(
                FormattableString.Invariant(
                    $"STATUS {(int)status.Role} {status.Term} {status.CommitIndex} {status.AppliedIndex} {(runtime.Failure is null && server.FatalError is null ? 0 : 1)} {status.SnapshotIndex}"
                )
            );
            break;
        case "pause":
            receiver.Arm();
            output.Write("ARMED");
            break;
        case "lose":
            receiver.LoseReply();
            output.Write("LOST");
            break;
        case "compact":
            await runtime.CompactAsync(token);
            output.Write("COMPACTED");
            break;
        case "stop":
            return;
        default:
            if (!line.StartsWith("clock ", StringComparison.Ordinal))
                throw new InvalidOperationException("Unknown test control.");
            clock.Set(long.Parse(line.AsSpan(6), CultureInfo.InvariantCulture));
            output.Write("CLOCK");
            break;
    }
}

[SuppressMessage(
    "Reliability",
    "CA2000:Dispose objects before losing scope",
    Justification = "Successful construction transfers the store to the runtime; failed construction releases it."
)]
DataGridPeerRuntime OpenRuntime()
{
    var store = DataGridFileStore.Open(
        Path.Combine(directory, $"node-{member}"),
        config,
        member,
        observe: step =>
        {
            if (step != pauseStep || Interlocked.Exchange(ref storagePaused, 1) != 0)
                return;
            output.Write(FormattableString.Invariant($"STORAGEPAUSED {(int)step}"));
            // Only the owning parent kills this process; the lifetime bounds orphaned probes.
            token.WaitHandle.WaitOne();
            token.ThrowIfCancellationRequested();
        }
    );
    try
    {
        return new(
            store,
            credentials,
            new TcpListener(
                IPAddress.Parse($"127.0.0.{2 + member}"),
                config.GetMember(member).Port
            ),
            Enumerable.Repeat(IPAddress.Parse($"127.0.0.{10 + member}"), 3).ToArray(),
            epoch,
            clock,
            security
        );
    }
    catch
    {
        store.Dispose();
        throw;
    }
}

internal sealed class ProbeClock(long ticks) : TimeProvider
{
    private long _ticks = ticks;

    public override DateTimeOffset GetUtcNow() => new(Volatile.Read(ref _ticks), TimeSpan.Zero);

    internal void Set(long value) => Volatile.Write(ref _ticks, value);
}

internal sealed class ProbeOutput
{
    private readonly object _sync = new();

    internal void Write(string line)
    {
        lock (_sync)
        {
            Console.WriteLine(line);
            Console.Out.Flush();
        }
    }
}

internal sealed class PausingReceiver(DataGridPeerRuntime runtime, ProbeOutput output)
    : IDataGridClientReceiver
{
    private int _armed;
    private TaskCompletionSource? _replyLoss;

    internal void Arm()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (Interlocked.CompareExchange(ref _replyLoss, gate, null) is not null)
            throw new InvalidOperationException("A reply pause is already armed.");
        Interlocked.Exchange(ref _armed, 1);
    }

    internal void LoseReply()
    {
        if (Volatile.Read(ref _replyLoss)?.TrySetResult() != true)
            throw new InvalidOperationException("No pending reply to lose.");
    }

    public async Task<DataGridClientReply> ExecuteAsync(
        Guid principal,
        DataGridClientRequest request,
        CancellationToken cancellationToken
    )
    {
        var result = await runtime
            .ExecuteAsync(principal, request, cancellationToken)
            .ConfigureAwait(false);
        if (
            result.Kind == DataGridMessageKind.MutationResult
            && Interlocked.Exchange(ref _armed, 0) == 1
        )
        {
            var gate = Volatile.Read(ref _replyLoss)!;
            output.Write("PAUSED"); // Durable actor completion happened; no result is sent to the client.
            try
            {
                await gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                throw new IOException("Injected owned-test reply loss.");
            }
            finally
            {
                Interlocked.CompareExchange(ref _replyLoss, null, gate);
            }
        }
        return result;
    }
}
