# HostLoom.Generators.Benchmarks

```text
dotnet run --project benchmarks/HostLoom.Generators.Benchmarks -c Release -- --filter "*"
```

Measures per-call latency, throughput, and allocations for every generator shape in
`HostLoom.Generators`:

| Case                                    | Question it answers                                  |
| --------------------------------------- | ---------------------------------------------------- |
| `NumericCode_FiveDigits`                | What does a fixed-width confirmation code cost?      |
| `Coupon_SixChar_SinglePart` / `Eight`   | What does a single-part coupon cost at 6 and 8 chars? |
| `RandomString_AlphaNumeric_10/20/32`    | How does a random string scale with length?          |
| `Token_Base64Url_32Bytes`               | What does an opaque 32-byte token cost?              |
| `DistinctBatch_10_000`                  | What does a 10,000-item distinct batch cost?         |
| `Concurrent_RandomString_SharedSingleton` | Does one shared singleton serialize under contention? |

The concurrent row drives one shared `RandomStringGenerator` — the recommended singleton
lifetime — from 1,000 `Parallel.For` workers; its allocation column includes the parallel
scaffolding, so it is not directly comparable with the single-threaded rows.

## No targets — record, don't gate

The generators audit supplies no latency or allocation targets. These numbers exist to be
recorded and compared between runs and machines, not to gate a build: there is no baseline file
under `benchmarks/baselines/` and no `just` recipe checking them. If a regression gate is ever
wanted, that is a deliberate follow-up decision, not something to infer from this project's
existence.

## Smoke run

Run every case once to verify discovery, setup, and execution:

```text
dotnet run --project benchmarks/HostLoom.Generators.Benchmarks -c Release -- --job Dry --filter "*"
```

Dry-job timings do not establish a performance baseline.
