using StackExchange.Redis;

namespace HostLoom.Redis.Benchmarks;

internal static class RedisBenchmarkConfiguration
{
    private const string Variable = "HOSTLOOM_BENCHMARK_REDIS";

    internal static string Value =>
        Environment.GetEnvironmentVariable(Variable) ?? "localhost:6379";

    internal static async Task VerifyAsync(IConnectionMultiplexer multiplexer)
    {
        try
        {
            await multiplexer.GetDatabase().PingAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            throw new InvalidOperationException(
                $"Redis benchmark endpoint '{Value}' is unavailable. Start Redis or set {Variable}.",
                exception
            );
        }
    }
}
