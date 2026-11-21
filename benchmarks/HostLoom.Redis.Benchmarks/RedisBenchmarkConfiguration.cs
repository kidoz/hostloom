using StackExchange.Redis;

namespace HostLoom.Redis.Benchmarks;

internal static class RedisBenchmarkConfiguration
{
    private const string Variable = "HOSTLOOM_BENCHMARK_REDIS";

    internal static string Value =>
        Environment.GetEnvironmentVariable(Variable) ?? "localhost:6379";

    internal static async Task<ConnectionMultiplexer> ConnectAsync()
    {
        ConnectionMultiplexer? multiplexer = null;
        try
        {
            multiplexer = await ConnectionMultiplexer.ConnectAsync(Value).ConfigureAwait(false);
            await multiplexer.GetDatabase().PingAsync().ConfigureAwait(false);
            return multiplexer;
        }
        catch (Exception exception)
        {
            if (multiplexer is not null)
                await multiplexer.DisposeAsync().ConfigureAwait(false);
            throw new InvalidOperationException(
                $"Redis benchmark connection failed ({exception.GetType().Name}). Start Redis or check {Variable}."
            );
        }
    }
}
