# HostLoom.Generators.Testing

Deterministic and scripted entropy for tests built on `HostLoom.Generators`: a seeded
`IRandomSource` for repeatable shape tests, a scripted `IRandomSource` that forces exact draws,
and a thread-safe async numeric sequence.

**Never in production.** These implementations exist so tests can pin outcomes. Seeded
pseudorandomness and scripted values are not secure; production code draws from
`SecureRandomSource.Instance`.

- `SeededRandomSource(seed)` — the same seed produces the same sequence, so a test is
  repeatable across runs. Not cryptographically unbiased; use it for shape and membership
  tests only.
- `ScriptedRandomSource(values...)` — every `NextInt32`/`NextInt64` call dequeues the next
  scripted value, which must lie inside the requested range; `WithBytes(bytes...)` scripts what
  `Fill` hands out. Consumption is strict: an exhausted script or an out-of-range value throws,
  so a test that under-scripts fails loudly. This is how a test forces endpoints, specific
  characters, and collisions.
- `AsyncNumericSequence(seed, increment)` — `NextAsync` returns `seed + increment`, then
  successive increments, thread-safe via `Interlocked`. Process-local; no distributed
  uniqueness.

```csharp
// Force a collision: the script draws "AB7" twice before a distinct candidate arrives.
var candidates = new Queue<string>(["AB7", "AB7", "CD9"]);
var result = DistinctBatch.Generate(_ => candidates.Dequeue(), count: 2);

Assert.Equal(DistinctBatchStatus.Success, result.Status);
Assert.Equal(["AB7", "CD9"], result.Values);
Assert.Equal(3, result.Attempts); // one attempt spent on the duplicate
```

Combining the scripted source with a generator pins the whole draw, for example
`new CouponCodeGenerator(new ScriptedRandomSource(0, 0, 0, 0, 0, 0))` produces the six-character
code made of the alphabet's first character.
