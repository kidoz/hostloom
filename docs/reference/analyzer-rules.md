# Analyzer rules

`HostLoom.Analyzers` ships Roslyn analyzers that enforce correct
asynchronous, dependency-injection, mapping, and caching usage at compile
time. The package is optional and has no runtime dependency. The canonical
rule documentation lives at `src/HostLoom.Analyzers/README.md` in the
repository.

| Rule | Reports |
| --- | --- |
| `HLM0001` | An available cancellation token omitted from a HostLoom async call |
| `HLM0002` | Synchronous blocking (`.Result`, `.Wait()`, …) over a HostLoom async operation |
| `HLM0003` | Singleton registration of handlers or behaviors that should follow HostLoom's per-delivery scope |
| `HLM0004` | A destination member an explicit map never assigns |
| `HLM0005` | A map body whose completeness cannot be verified |
| `HLM0006` | The scoped mapping dispatcher captured in a singleton |
| `HLM0007` | A cache or lock key built from a token, secret, password, or API key without `FromSensitive` |
| `HLM0008` | A get-or-create factory that declares its cancellation token and never forwards it |

## Why these rules exist

- **`HLM0001`/`HLM0002`** — HostLoom is `ValueTask`-based end to end;
  dropped tokens and sync-over-async are the two classic ways that surface
  as production stalls. Both rules cover the cache and lock contracts
  (`ICache`, `IDistributedLock`, `ILockHandle`) as well as the messaging
  runtime.
- **`HLM0003`** — every delivery attempt gets its own
  dependency-injection scope. A handler registered as a singleton silently
  opts out of that isolation and shares state across retries.
- **`HLM0004`/`HLM0005`** — explicit mapping trades convention magic for
  compiler visibility; these rules close the remaining gap, keeping a
  forgotten member from shipping as silent data loss.
- **`HLM0006`** — the `IMapper` dispatcher is scoped; capturing it in a
  singleton pins the first scope forever.
- **`HLM0007`** — a cache or lock key reaches the backend, the logs, and
  the spans; `CacheKey.FromSensitive` and `LockKey.FromSensitive` hash a
  credential so the key stays unique without carrying it.
- **`HLM0008`** — a get-or-create factory receives the caller's token so its
  work stops with the request; declaring the token and ignoring it keeps the
  work, and the per-key guard, alive after the caller has gone.
