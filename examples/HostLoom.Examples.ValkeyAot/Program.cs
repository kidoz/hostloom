using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using HostLoom.Valkey;
using Microsoft.Extensions.DependencyInjection;
using ValkeyDotNet;

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
var token = timeout.Token;
var port = int.Parse(
    Environment.GetEnvironmentVariable("HOSTLOOM_VALKEY_PORT") ?? "16379",
    CultureInfo.InvariantCulture
);
var ns = "valkey-aot-" + Guid.NewGuid().ToString("N");
var services = new ServiceCollection();
var json = new JsonSerializerOptions { TypeInfoResolver = CatalogJson.Default };
services
    .AddHostLoomCaching(options => options.Namespace = ns)
    .UseValkey(options =>
        options.Connection = new ValkeyClientOptions { Host = "localhost", Port = port }
    )
    .UseSystemTextJson(json);
services.AddHostLoomLocking(options => options.Namespace = ns).UseValkey();
var container = services.BuildServiceProvider(
    new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
);
await using var containerLifetime = container.ConfigureAwait(false);
var writer = container.GetRequiredService<ICache>();
var reader = new TieredCache(
    new CachingOptions { Namespace = ns },
    container.GetRequiredService<IDistributedCacheStore>(),
    new SystemTextJsonCacheValueSerializer(json)
);
await using var readerLifetime = reader.ConfigureAwait(false);
var channel = (ValkeyCacheInvalidationChannel)
    container.GetRequiredService<ICacheInvalidationChannel>();
var invalidated = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
using var subscription = channel.Subscribe(message =>
{
    if (message.Keys.Contains("catalog:eu"))
        invalidated.TrySetResult();
});
await channel.StartAsync(token).ConfigureAwait(false);
try
{
    await writer
        .SetAsync(
            "catalog:eu",
            new Catalog("eu", 2),
            new CacheEntryOptions(TimeSpan.FromSeconds(30)),
            token
        )
        .ConfigureAwait(false);
    var lookup = await reader.TryGetAsync<Catalog>("catalog:eu", token).ConfigureAwait(false);
    if (!lookup.Found || lookup.Tier != CacheTier.L2 || lookup.Value != new Catalog("eu", 2))
        throw new InvalidOperationException("Serialized Valkey L2 round trip failed.");
    var locking = container.GetRequiredService<IDistributedLock>();
    var handle = await locking
        .TryAcquireAsync("catalog:refresh", cancellationToken: token)
        .ConfigureAwait(false);
    if (handle is null)
        throw new InvalidOperationException("Valkey lease acquisition failed.");
    await using var handleLifetime = handle.ConfigureAwait(false);
    if (
        !handle.IsHeld
        || !await handle.ExtendAsync(TimeSpan.FromSeconds(10), token).ConfigureAwait(false)
    )
        throw new InvalidOperationException("Valkey lease acquisition or extension failed.");
    await writer.RemoveAsync("catalog:eu", token).ConfigureAwait(false);
    await invalidated.Task.WaitAsync(token).ConfigureAwait(false);
    Console.WriteLine(
        "Valkey serialized L2, lock acquisition/extension/release scope, and invalidation passed."
    );
}
finally
{
    await writer.RemoveAsync("catalog:eu", token).ConfigureAwait(false);
}

internal sealed record Catalog(string Region, int Items);

[JsonSerializable(typeof(Catalog))]
internal sealed partial class CatalogJson : JsonSerializerContext;
