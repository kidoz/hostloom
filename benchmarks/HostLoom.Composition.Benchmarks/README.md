# Composition measurements

This non-packable executable measures plan creation, application, passive probes and optional
ledger work separately, with handwritten and Scrutor 7.0.0 comparisons over 100 registrations.
It also exercises 46/160/1,000-candidate Roslyn driver inputs and paired clean consumer builds.

From the repository root:

```sh
dotnet restore HostLoom.slnx
dotnet build benchmarks/HostLoom.Composition.Benchmarks -c Release --no-restore
python3 eng/measure-composition.py --output /tmp/hostloom-composition-measurements
python3 eng/check-composition-baseline.py --results /tmp/hostloom-composition-measurements/summary.json
PYTHONDONTWRITEBYTECODE=1 python3 eng/test-composition-baseline.py
```

Run on an idle machine and use a fresh output directory. The runner retains raw samples and
consumer build logs. It never updates the [reviewed baseline](../baselines/composition.json).
The gate rejects a different environment; do not use these CPU-specific timing thresholds on
arbitrary CI workers. Its deterministic Python tests can run anywhere.

Read the [performance reference](../../docs/reference/composition-performance.md) for environment,
all measured phases, cold versus warm interpretation, allocation scope, paired-build methodology,
numeric budgets and current overhead. This is a custom first-call/phase harness, not a
BenchmarkDotNet job. Scrutor and Roslyn measurement dependencies never enter runtime packages.
