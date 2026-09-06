# Composition measurements

This non-packable executable measures plan creation, application, passive probes and optional
ledger work separately, with handwritten and Scrutor 7.0.0 comparisons over 100 registrations.
It also exercises 46/160/1,000-candidate Roslyn driver inputs and paired clean consumer builds.

From the repository root:

```sh
dotnet restore HostLoom.slnx
dotnet build benchmarks/HostLoom.Composition.Benchmarks -c Release --no-restore
python3 scripts/measure-composition.py --output /tmp/hostloom-composition-measurements
python3 scripts/check-composition-baseline.py --results /tmp/hostloom-composition-measurements/summary.json
PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-composition-baseline.py
```

Run on an idle machine and use a fresh output directory. The runner retains raw samples and
consumer build logs. It never updates the [reviewed baseline](../baselines/composition.json).
The gate rejects a different environment; do not use these CPU-specific timing thresholds on
arbitrary CI workers. Its deterministic Python tests can run anywhere.

Read the [performance reference](../../docs/reference/composition-performance.md) for environment,
all measured phases, cold versus warm interpretation, allocation scope, paired-build methodology,
numeric budgets and current overhead. This is a custom first-call/phase harness, not a
BenchmarkDotNet job. Scrutor and Roslyn measurement dependencies never enter runtime packages.

## Append and Replace workloads

The executable also exposes targeted application phases over a populated collection:

```sh
dotnet benchmarks/HostLoom.Composition.Benchmarks/bin/Release/net10.0/HostLoom.Composition.Benchmarks.dll verify
DOTNET_TieredCompilation=0 DOTNET_ReadyToRun=1 dotnet benchmarks/HostLoom.Composition.Benchmarks/bin/Release/net10.0/HostLoom.Composition.Benchmarks.dll apply-append
DOTNET_TieredCompilation=0 DOTNET_ReadyToRun=1 dotnet benchmarks/HostLoom.Composition.Benchmarks/bin/Release/net10.0/HostLoom.Composition.Benchmarks.dll apply-replace
```

Both prepare a 100-registration plan and 100 existing opaque scoped factory descriptors before
timing. Each timed operation creates and seeds a new collection, then applies the plan. Append
uses Many cardinality and retains the factories before adding the type registrations: 200 final
descriptors and 100 Added decisions. Replace uses One cardinality and ServiceType replacement:
100 final type descriptors and 200 interleaved Replaced/Added decisions. Factories throw if invoked;
no provider is built. The `verify` command checks these descriptor and report sequences too.

Each command emits one first-call measurement and 15 warm batch averages after 64 warmups, using
32 calls per batch. For a comparison, build the same workload against both runtime versions, retain
the raw JSON from five launches per version, and alternate which version runs first. Compare
`apply` as an unchanged One/Throw control. These additional phases are not included in the default
measurement script or reviewed budget checker; they have no reviewed regression thresholds yet.

## Generator workload profiles

The generator command accepts an optional workload after the candidate count:

```sh
DOTNET_TieredCompilation=0 DOTNET_ReadyToRun=1 dotnet benchmarks/HostLoom.Composition.Benchmarks/bin/Release/net10.0/HostLoom.Composition.Benchmarks.dll generator 1000 many
DOTNET_TieredCompilation=0 DOTNET_ReadyToRun=1 dotnet benchmarks/HostLoom.Composition.Benchmarks/bin/Release/net10.0/HostLoom.Composition.Benchmarks.dll generator 1000 repeated-rules
DOTNET_TieredCompilation=0 DOTNET_ReadyToRun=1 dotnet benchmarks/HostLoom.Composition.Benchmarks/bin/Release/net10.0/HostLoom.Composition.Benchmarks.dll generator 1000 captures
```

`many` is the original default: one discovery rule projects all candidates to a single transient
Many service. `repeated-rules` discovers the same candidates four times, projecting a different
interface per rule, for 4,000 registrations at count 1,000. `captures` registers 1,000 singleton
candidates plus a shared transient session, singleton inventory and transient open-generic
repository. Its constructor graph exercises exact and open-generic lookups without a scoped
capture. These are generator measurements; no generated plan or service factory is executed.

Each profile records first-run cost and 15 sequences of fresh-driver, unchanged, unrelated-edit
and rule-edit runs. The capture profile edits Singleton to Transient so the changed declaration
stays valid; the other profiles edit Transient to Scoped. Every profile checks output reuse for
unrelated edits, requires rule edits to invalidate output, and emits a SHA-256 of the original
generated source for before/after comparisons. Run five fresh processes per version in alternating
order and preserve raw JSON. The additional profiles are outside the reviewed budget checker;
the default measurement script still uses `many` at 46, 160 and 1,000 candidates.
