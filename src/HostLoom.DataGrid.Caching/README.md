# HostLoom.DataGrid.Caching

Experimental, non-packable L2 store for an explicitly configured DataGrid map. The
application supplies a [DataGrid client](../HostLoom.DataGrid.Client/README.md)
whose principal may open sessions and read and mutate the selected map. The application
owns and disposes the client.

For a named cache profile, register the concrete store under the same key and select
it with the existing cache builder. Keep the local cache tier disabled while committed
cross-client invalidation is pending.

```csharp
services.AddKeyedSingleton("catalog", new DataGridCacheStore(client, mapId));
services.AddHostLoomCaching("catalog", options =>
    {
        options.Namespace = "catalog";
        options.L1.Enabled = false;
    })
    .UseStore<DataGridCacheStore>("DataGrid")
    .UseSystemTextJson(jsonOptions);
```

`jsonOptions` comes from the application and can use a source-generated JSON context.
Applications using constructors can supply the same store to `TieredCache` with
`L1.Enabled = false`.

- Fully prefixed keys use strict UTF-8, up to 1024 bytes. Values are opaque bytes up to
  64 KiB. A read returns the value and its remaining TTL from the same committed grid
  view. Positive durations use replicated grid time. A nonpositive write duration is
  a no-op, and conditional insertion returns false.
- Bulk reads use one grid read with up to 15 requested keys, including duplicates.
  This fits a reply even when every value is at its maximum size. Bulk writes and
  removals use one mutation of up to 128 requested items within the frame budget.
  Bulk mutations can apply some items before reporting a rejected item.
- Each store serializes mutations through a lazily opened session and admits at most
  eight active or waiting mutations. The SDK retries a request with its original
  logical identity. An uncertain failure ends local use of the session. A later
  operation can open a fresh session, and abandoned sessions expire on grid time.
- Store capabilities are limited to the base operations. Tag mutation and sliding
  expiration are unsupported. Retry and refusal outcomes map to the cache failure
  contract, and caller cancellation propagates with possible admission noted when known.

The prototype is qualified only by the scoped tests in the local Linux TLS fixture.
Production and Native AOT qualification are pending.
