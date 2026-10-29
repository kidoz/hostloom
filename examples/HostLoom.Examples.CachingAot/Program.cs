using System.Text.Json;
using System.Text.Json.Serialization;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// A Native AOT publish of this program must produce no trim or AOT warnings. It composes the
// in-process distributed store so a serialized second-tier round trip through a source-generated
// JsonSerializerContext is compiled and executed, not just typed in-process storage.
const string profile = "catalog";
var time = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<TimeProvider>(time);
builder
    .Services.AddHostLoomCaching(profile, caching => caching.Namespace = "sample")
    .UseStore<InMemoryDistributedCacheStore>("InMemoryDistributed")
    .UseSystemTextJson(new JsonSerializerOptions { TypeInfoResolver = SampleJsonContext.Default })
    .AddDistributedCacheAdapter(enableSlidingExpiration: true);
builder.Services.AddHostLoomLocking(profile, locking => locking.Namespace = "sample").UseInMemory();

using var host = builder.Build();
await host.StartAsync().ConfigureAwait(false);

var cache = host.Services.GetRequiredKeyedService<ICache>(profile);
var store = host.Services.GetRequiredKeyedService<IDistributedCacheStore>(profile);
var distributedLock = host.Services.GetRequiredKeyedService<IDistributedLock>(profile);
var distributedCache = host.Services.GetRequiredKeyedService<IDistributedCache>(profile);

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
    host.Services.GetRequiredKeyedService<ICacheValueSerializer>(profile)
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

byte[] slidingPayload = [1, 2, 3, 4];
var slidingOptions = new DistributedCacheEntryOptions
{
    AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(10),
    SlidingExpiration = TimeSpan.FromMinutes(1),
};
await distributedCache
    .SetAsync("catalog:sliding", slidingPayload, slidingOptions)
    .ConfigureAwait(false);
time.Advance(TimeSpan.FromSeconds(30));
var renewed = await distributedCache.GetAsync("catalog:sliding").ConfigureAwait(false);
time.Advance(TimeSpan.FromSeconds(30));
await distributedCache.RefreshAsync("catalog:sliding").ConfigureAwait(false);
var afterRefresh = await distributedCache.GetAsync("catalog:sliding").ConfigureAwait(false);
if (renewed is null || afterRefresh is null || !afterRefresh.AsSpan().SequenceEqual(slidingPayload))
{
    throw new InvalidOperationException("Named-profile sliding renewal failed.");
}

time.Advance(TimeSpan.FromSeconds(61));
if (await distributedCache.GetAsync("catalog:sliding").ConfigureAwait(false) is not null)
{
    throw new InvalidOperationException("Sliding entry did not expire after inactivity.");
}

Console.WriteLine("named profile and sliding read/refresh/inactivity expiration passed");

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

internal sealed class ManualTimeProvider(DateTimeOffset now) : TimeProvider
{
    private DateTimeOffset _now = now;

    public override DateTimeOffset GetUtcNow() => _now;

    public void Advance(TimeSpan duration) => _now += duration;
}
