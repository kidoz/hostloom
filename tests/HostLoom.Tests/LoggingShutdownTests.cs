using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

public sealed class LoggingShutdownTests
{
    [Theory]
    [InlineData("dispose")]
    [InlineData("callback")]
    [InlineData("throwing-callback")]
    public async Task Shutdown_is_bounded_even_before_an_async_operation_returns(string mode)
    {
#pragma warning disable CA2000 // Ownership transfers to the provider, including its abandonment policy.
        var sink = CreateSink(mode);
#pragma warning restore CA2000
        await using var provider = new HostLoomLoggerProvider(
            new JsonLogFormatter(),
            sink,
            new HostLoomLoggerOptions
            {
                ShutdownTimeout = TimeSpan.FromMilliseconds(20),
                BatchSize = 1,
            }
        );
        Task? disposing = null;
        try
        {
            if (mode != "dispose")
            {
                provider.CreateLogger("Catalog").LogInformation("Catalog refreshed.");
                await sink.Writing.Task.WaitAsync(
                    TimeSpan.FromSeconds(5),
                    TestContext.Current.CancellationToken
                );
            }
            disposing = Task.Run(
                async () => await provider.DisposeAsync(),
                TestContext.Current.CancellationToken
            );
            await sink.Entered.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken
            );
            await disposing.WaitAsync(
                TimeSpan.FromSeconds(3),
                TestContext.Current.CancellationToken
            );
        }
        finally
        {
            sink.Release.TrySetResult();
            if (disposing is not null)
                await disposing.WaitAsync(
                    TimeSpan.FromSeconds(5),
                    TestContext.Current.CancellationToken
                );
        }
        await provider.DisposeAsync();
    }

    // Ownership transfers to the logger provider; the signals do not own native handles.
    private static CoordinatedSink CreateSink(string mode) => new(mode);

    private sealed class CoordinatedSink(string mode) : ILogSink
    {
        public TaskCompletionSource Writing { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Entered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken)
        {
            using var callback = cancellationToken.Register(() =>
            {
                Entered.TrySetResult();
                if (mode == "throwing-callback")
                    throw new IOException("Callback failed.");
                Release.Task.GetAwaiter().GetResult();
            });
            Writing.TrySetResult();
            // Wait for the callback itself, not the token's wait handle: cancellation sets the
            // handle before it runs callbacks, so a Write woken by the handle could dispose its
            // registration first and the blocking callback under test would never run at all.
            Entered.Task.Wait(CancellationToken.None);
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;
#pragma warning disable CA1849 // Synchronous blocking before returning the ValueTask is the regression scenario.
        public ValueTask DisposeAsync()
        {
            if (mode == "dispose")
            {
                Entered.TrySetResult();
                Release.Task.GetAwaiter().GetResult();
            }
            return ValueTask.CompletedTask;
        }
#pragma warning restore CA1849
    }
}
