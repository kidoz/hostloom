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

## Composition generator diagnostics (in development)

These diagnostics come from the `HostLoom.Composition.Generators` analyzer project, separately from
`HostLoom.Analyzers`. The generator is currently consumed by project reference and is not packaged
for release yet. All eight diagnostics have error severity and point to authored declarations or
usage; conflicts also carry the other rule location.

| Rule | Reports |
| --- | --- |
| `HLM0009` | Unsupported declaration syntax, invalid factory pairing, or invocation/delegate capture of a declaration method |
| `HLM0010` | Unbounded, empty, invalid or inaccessible selection, or namespace guard violation |
| `HLM0011` | Missing, repeated or invalid lifetime/cardinality, or repeated/invalid strategy |
| `HLM0012` | Missing/incompatible service projection or unavailable public constructor |
| `HLM0013` | Duplicate, cardinality or lifetime conflict within a generated plan |
| `HLM0014` | Invalid or unsatisfied implementation count |
| `HLM0015` | Unsupported open-generic mapping, constraints or trimming requirements |
| `HLM0016` | Proven singleton capture through known plan constructor paths |

See the [generator reference](../../src/HostLoom.Composition.Generators/README.md) for supported
syntax and validation limits. Constructor dependency resolution remains a final-provider check.

The existing HLM0003 does not inspect generated code. Capture diagnostics examine known plan
edges only; unknown dependencies and uncertain constructor choices still require provider/scope
validation and known closed-service resolution tests.
