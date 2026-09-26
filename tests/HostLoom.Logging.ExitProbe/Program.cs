using System.Globalization;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;

// Logs a backlog through a deliberately slow sink, then ends the process the way the first
// argument names, never disposing the provider: "exit" calls Environment.Exit, "return" returns
// from Main, and "unhandled" throws. LoggingProcessExitTests counts what reaches stdout.
var ending = args[0];
var records = int.Parse(args[1], CultureInfo.InvariantCulture);

// CA2000: never disposed on purpose; what the process exit gets out without disposal is the test.
#pragma warning disable CA2000
var provider = new HostLoomLoggerProvider(
    new JsonLogFormatter(),
    new SlowSink(StreamLogSink.Console()),
    new HostLoomLoggerOptions
    {
        QueueFullPolicy = QueueFullPolicy.Block,
        QueueCapacity = 1024,
        BatchSize = 16,
    }
);
#pragma warning restore CA2000
var logger = provider.CreateLogger("ExitProbe");
for (var i = 0; i < records; i++)
{
    logger.LogFast(LogLevel.Information, $"record {i}");
}

switch (ending)
{
    case "exit":
        Environment.Exit(3);
        break;
    case "unhandled":
        throw new InvalidOperationException("the probe crashed on purpose");
    default:
        return;
}

/// <summary>Takes a millisecond per batch, so a backlog is still queued when the process ends.</summary>
internal sealed class SlowSink(ILogSink inner) : ILogSink
{
    public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken)
    {
        Thread.Sleep(1);
        inner.Write(payload, cancellationToken);
    }

    public ValueTask FlushAsync(CancellationToken cancellationToken) =>
        inner.FlushAsync(cancellationToken);

    public ValueTask DisposeAsync() => inner.DisposeAsync();
}
