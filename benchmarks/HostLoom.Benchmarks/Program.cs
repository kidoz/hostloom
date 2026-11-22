using BenchmarkDotNet.Running;

namespace HostLoom.Benchmarks;

internal static class Program
{
    public static async Task Main(string[] args)
    {
        if (args is ["--actor-catalog"])
        {
            await ActorCatalogComparison.RunAsync().ConfigureAwait(false);
            return;
        }
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
