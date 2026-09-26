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
