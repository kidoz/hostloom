using System.Text.Json;
using System.Text.Json.Serialization;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// A Native AOT publish of this program must produce no trim or AOT warnings. It composes the
// in-process distributed store so a serialized second-tier round trip through a source-generated
// JsonSerializerContext is compiled and executed, not just typed in-process storage.
var builder = Host.CreateApplicationBuilder(args);
builder
    .Services.AddHostLoomCaching(caching => caching.Namespace = "sample")
    .UseStore<InMemoryDistributedCacheStore>("InMemoryDistributed")
    .UseSystemTextJson(new JsonSerializerOptions { TypeInfoResolver = SampleJsonContext.Default });
builder.Services.AddHostLoomLocking(locking => locking.Namespace = "sample").UseInMemory();

using var host = builder.Build();
await host.StartAsync().ConfigureAwait(false);

var cache = host.Services.GetRequiredService<ICache>();
var store = host.Services.GetRequiredService<IDistributedCacheStore>();
var distributedLock = host.Services.GetRequiredService<IDistributedLock>();

var first = await cache
    .GetOrCreateAsync(
        "catalog:eu",
        static _ => ValueTask.FromResult(new Catalog("eu", ["books", "music"])),
        TimeSpan.FromMinutes(5)
    )
    .ConfigureAwait(false);
var stored = await store.GetAsync("sample:cache:data:catalog:eu").ConfigureAwait(false);
Console.WriteLine($"factory result: {first!.Region} with {first.Categories.Count} categories");
Console.WriteLine($"second tier holds {stored!.Value.Payload.Length} bytes");

// A second cache over the same store starts with an empty in-process tier, so this read is a
// serialized round trip through the JsonSerializerContext.
var second = new TieredCache(
    new CachingOptions { Namespace = "sample" },
    store,
    host.Services.GetRequiredService<ICacheValueSerializer>()
);
await using (second.ConfigureAwait(false))
{
    var lookup = await second.TryGetAsync<Catalog>("catalog:eu").ConfigureAwait(false);
    Console.WriteLine($"second instance: found={lookup.Found} tier={lookup.Tier}");
}

var held = await distributedLock
    .ExecuteWithLockAsync("catalog:refresh", static _ => ValueTask.FromResult(true))
    .ConfigureAwait(false);
Console.WriteLine($"lock executed: {held}");

foreach (var line in CachingProbe.Describe(cache).Lines)
{
    Console.WriteLine(line);
}

foreach (var line in LockingProbe.Describe(distributedLock).Lines)
{
    Console.WriteLine(line);
}

await host.StopAsync().ConfigureAwait(false);

internal sealed record Catalog(string Region, IReadOnlyList<string> Categories);

[JsonSerializable(typeof(Catalog))]
internal sealed partial class SampleJsonContext : JsonSerializerContext;
