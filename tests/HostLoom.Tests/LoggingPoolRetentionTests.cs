using System.Runtime.CompilerServices;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

// Another logging test renting an entry would clear references in the old implementation and
// hide this regression. Keep the GC check isolated from every other test's pool traffic.
[CollectionDefinition(nameof(LoggingPoolIsolation), DisableParallelization = true)]
public sealed class LoggingPoolIsolation;

[Collection(nameof(LoggingPoolIsolation))]
public sealed class LoggingPoolRetentionTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void Caller_names_are_released_before_the_record_waits_in_the_queue(
        bool wrapped,
        bool rejected
    )
    {
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
#pragma warning disable CA2000 // The provider owns the sink.
        using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            new HeldSink(entered, release),
            new() { MaxFieldNameLength = 256 }
        );
#pragma warning restore CA2000
        using var factory = LoggerFactory.Create(builder => builder.AddProvider(provider));
        var logger = wrapped
            ? factory.CreateLogger("Retention")
            : provider.CreateLogger("Retention");
        logger.LogInformation("hold the writer");
        try
        {
            Assert.True(
                entered.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken)
            );
            var name = LogNamedValue(logger, rejected);
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            Assert.False(name.IsAlive);
        }
        finally
        {
            release.Set();
        }
        Assert.True(provider.Flush(TimeSpan.FromSeconds(5)));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference LogNamedValue(ILogger logger, bool rejected)
    {
        var name = new string('x', rejected ? 1_000_000 : 200);
        var weak = new WeakReference(name);
        var handler = new LogMessageHandler(0, 1, logger, LogLevel.Information, out _);
        handler.AppendFormatted(42, name: name);
        logger.LogFast(LogLevel.Information, ref handler);
        return weak;
    }

    private sealed class HeldSink(ManualResetEventSlim entered, ManualResetEventSlim release)
        : ILogSink
    {
        public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken)
        {
            entered.Set();
            if (!release.Wait(TimeSpan.FromSeconds(5), cancellationToken))
                throw new TimeoutException("writer gate did not open");
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [Fact]
    public void A_delivered_exception_graph_is_collectible_without_reusing_its_entry()
    {
        var payload = LogException();
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        Assert.False(payload.IsAlive);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference LogException()
    {
        var payload = new byte[1024 * 1024];
        var weak = new WeakReference(payload);
        var exception = new InvalidOperationException("request failed");
        exception.Data["payload"] = payload;
        using var output = new MemoryStream();
#pragma warning disable CA2000 // The provider owns the sink.
        using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            new StreamLogSink(output, leaveOpen: true),
            new()
        );
#pragma warning restore CA2000
        provider.CreateLogger("Retention").LogError(exception, "request failed");
        Assert.True(provider.Flush(TimeSpan.FromSeconds(5)));
        return weak;
    }
}
