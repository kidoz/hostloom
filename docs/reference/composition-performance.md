# Composition performance evidence

Initial reference measurement: **2026-09-05**, Apple M4 Max, 16 logical processors, Arm64,
macOS 26.6.2, .NET 10.0.11, SDK 10.0.400, Roslyn assembly 5.9.0.0, C# 14. Scrutor is pinned
at 7.0.0 and appears only in the benchmark project. Measurements use
`DOTNET_TieredCompilation=0` and `DOTNET_ReadyToRun=1`; defaults or another machine are not directly
comparable. These results describe the measured implementation, not a guarantee for all hosts.

Creating and applying 100 generated registrations takes a warm median **292.70 µs / 174,432 managed
bytes**, versus **0.75 µs / 8,624 bytes** for handwritten registration and **116.98 µs / 125,241
bytes** for Scrutor. Provenance and validation have a measurable startup cost. The generated plan
is not faster in this comparison. A passive probe alone cannot stand in for total startup cost.

For 1,000 generator candidates, paired clean consumer builds add **107.48 ms median / 131.53 ms
p95**, within the **200 ms added-build target** on this machine. The standalone driver's first
process run costs **917.73 ms median**, including cold Roslyn/JIT work, and does **not** meet a
200 ms first-process latency bound. These are separate measurements.

## Reproduce

From the repository root, build once, then run on an otherwise idle reference machine:

```sh
dotnet restore HostLoom.slnx
dotnet build benchmarks/HostLoom.Composition.Benchmarks -c Release --no-restore
python3 eng/measure-composition.py --output /tmp/hostloom-composition-measurements
python3 eng/check-composition-baseline.py --results /tmp/hostloom-composition-measurements/summary.json
PYTHONDONTWRITEBYTECODE=1 python3 eng/test-composition-baseline.py
```

Use a new output directory for each run. The measurement command retains every raw sample,
verification result, consumer project and build log. It never updates the reviewed baseline.
The [numeric baseline](https://github.com/kidoz/hostloom/blob/main/benchmarks/baselines/composition.json)
contains medians, nearest-rank p95, minima, maxima, sample counts, paired build values, environment,
source hash and absolute budgets. The source hash covers C# under the runtime, generator and
benchmark projects, excluding build output. Review other dependencies and the linked ledger adapter
alongside that hash when reproducing the measurement.

This is a dedicated Stopwatch/allocation harness, separate from the repository's BenchmarkDotNet
suites. It measures first calls and isolated phases explicitly. It performs no timer/delegate/loop
or sink-overhead subtraction; results near a few nanoseconds should be read at that scale, without
assuming timer-level precision. Allocation is `GC.GetAllocatedBytesForCurrentThread`, including
managed work on that thread but excluding other threads, native compiler/JIT allocations and process
memory. The result-record allocation and JSON output are outside the measured interval.

## Runtime phases

The fixed input is 100 concrete `CatalogNNN` implementations of inherited closed `ICatalog<ItemNNN>`
interfaces, with 100 item types and one open abstract base. Each method registers the same 100
unkeyed scoped type descriptors into a new empty `ServiceCollection` using default capacity; no
provider is built and no service is resolved. Scrutor scans the benchmark assembly using
`AssignableTo(typeof(ICatalog<>)).AsImplementedInterfaces().WithScopedLifetime()`.
Before measurement, a separate process checks descriptor multiset equivalence for all three
methods and sequence equivalence for generated and handwritten registration.

Every runtime case has five fresh processes. Each process measures its first call once, performs
64 warm-up calls, then records 15 samples of 32 calls (100,000 calls for Probe). GC collection and
finalizer waiting happen between samples, outside timing. Warm distributions therefore contain
75 per-operation sample averages, not 2,400 individually timed calls. First-call distributions
contain five observations. Process launch is excluded in both cases.

| Case | First median ms | First managed B | Warm median µs | Warm p95 µs | Warm managed B/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| plan | 13.892 | 70,064 | 130.233 | 139.349 | 67,568.00 |
| apply | 3.848 | 113,952 | 155.173 | 163.730 | 106,864.00 |
| probe | 0.018 | 0 | 0.002 | 0.002 | 0.00 |
| total | 18.859 | 184,016 | 292.695 | 332.630 | 174,432.00 |
| handwritten | 5.960 | 8,648 | 0.746 | 1.021 | 8,624.00 |
| scrutor | 10.489 | 206,096 | 116.982 | 125.940 | 125,241.00 |
| ledger-record | 6.775 | 397,008 | 141.053 | 154.293 | 312,191.50 |
| ledger-report | 6.221 | 713,840 | 101.990 | 108.240 | 449,392.00 |
| total-ledger | 29.940 | 1,294,864 | 562.814 | 593.853 | 941,305.75 |

`plan` includes factory execution, plan validation and origin/rejection snapshot construction; its
first call also includes initial generated metadata and JIT work. `apply` includes new collection,
validation, identity tracking and the application report, with the plan prepared before timing.
`probe` uses a precreated plan. `total` starts without a plan and measures creation plus application;
it does not reuse precomputed metadata from an earlier call in the first-call process.

`ledger-record` prepares the plan/application report before timing, then creates and fills an
optional ledger. `ledger-report` prepares that ledger and measures its snapshot and report formatting.
`total-ledger` includes plan creation, application, ledger recording, snapshot and formatting. The
exact application-owned adapter source is linked into the benchmark. Its logger enables every
level and actually formats messages, but performs no console, disk or network I/O. Production log
sinks can add further cost. Isolated first calls have different prerequisite/JIT states, so summing
their cold times does not equal the directly measured total.

## Generator driver and incrementality

Each synthetic input has 46, 160 or 1,000 concrete catalog candidates, plus an interface, abstract
base, rule owner and unrelated class. All candidates implement one inherited service, registered
as transient Many. These sizes are neutral scale probes, not audited external application counts.
Three syntax trees separate declarations, candidate types and unrelated method bodies.

Five fresh processes run each size. Syntax parsing, metadata-reference creation and driver creation
are outside driver timing. `process-first` measures the first `RunGenerators`, including lazy Roslyn
symbol work and JIT. Each process then performs 15 sequences: fresh compilation/driver, unchanged
rerun, unrelated body edit, and lifetime-rule edit. Fresh compilations reuse immutable syntax trees
and metadata references; they are cold with respect to generator caches but warm with respect to
process/compiler code. Incremental cases retain the driver. No additional generator warm-up removes
the first use of edit/no-op paths, so p95 includes those paths' startup spikes. No consumer emit or
MSBuild time appears in this driver measurement.

| Candidates | Phase | Median ms | p95 ms | Managed B median | Managed B p95 |
| --- | --- | ---: | ---: | ---: | ---: |
| 46 | process-first | 924.055 | 935.607 | 7,508,952 | 7,509,768 |
| 46 | fresh-driver | 0.952 | 2.714 | 595,600 | 603,896 |
| 46 | unchanged | 0.042 | 21.098 | 24,008 | 26,760 |
| 46 | unrelated-edit | 0.665 | 9.336 | 431,072 | 437,024 |
| 46 | rule-edit | 0.676 | 6.182 | 433,464 | 445,360 |
| 160 | process-first | 889.065 | 902.564 | 8,472,560 | 8,482,624 |
| 160 | fresh-driver | 1.914 | 3.837 | 1,453,744 | 1,461,568 |
| 160 | unchanged | 0.050 | 20.192 | 24,008 | 26,760 |
| 160 | unrelated-edit | 1.553 | 10.304 | 1,268,536 | 1,274,040 |
| 160 | rule-edit | 1.612 | 11.616 | 1,270,264 | 1,281,984 |
| 1000 | process-first | 917.733 | 1095.923 | 15,639,864 | 15,640,680 |
| 1000 | fresh-driver | 15.353 | 25.948 | 7,708,312 | 7,716,176 |
| 1000 | unchanged | 0.093 | 22.051 | 25,088 | 30,240 |
| 1000 | unrelated-edit | 14.645 | 26.991 | 7,390,056 | 7,396,720 |
| 1000 | rule-edit | 14.139 | 25.547 | 7,387,120 | 7,400,024 |

Every unrelated body edit must preserve emitted source byte-for-byte and show only Cached or
Unchanged outputs at the tracked `CompositionSource` step. The baseline observed **Cached**.
This does not claim that all semantic analysis was skipped. Rule edits must change the output.
Deterministic tests additionally change inherited attributes, inherited interfaces, accessibility
and rules with the same driver, then revert them and require the original output to return.

## Added consumer build cost

The harness exports two standalone consumers per size. Both compile the same candidate and rule
sources with the same runtime references. The baseline consumer contains the generated factory
as ordinary source and has no generator/analyzer; the other creates that factory during compilation.
This isolates the added generator/analyzer integration cost from compiling the explicit descriptors.
It is distinct from the runtime handwritten comparison above. Export validates the generated
compilation before timing.

Restore is performed once outside timing. Five pairs alternate which consumer builds first,
using the pinned SDK and this command in each consumer directory:

```sh
dotnet build Consumer.csproj -c Release --no-restore -t:Rebuild -p:UseSharedCompilation=false
```

Each build runs a fresh compiler process; filesystem caches are not flushed. Elapsed time includes
MSBuild and compiler startup, diagnostics and emit. Subtract baseline from generated **within each
pair**, then summarize those differences. Negative differences are retained, never clamped. Wall
clock differences include scheduling noise and are evidence on this reference host, not an exact
attribution of every CPU instruction to the generator. Managed allocation is measured by the driver
above; paired child-process builds do not claim an allocation measurement.

| Candidates | Baseline median ms | Generated median ms | Added median ms | Added p95 ms | Added range ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 46 | 1374.28 | 1518.34 | 144.06 | 295.48 | 76.81–295.48 |
| 160 | 1368.49 | 1465.42 | 116.10 | 149.94 | 78.16–149.94 |
| 1000 | 1517.20 | 1636.40 | 107.48 | 131.53 | 84.64–131.53 |

With five pairs, nearest-rank p95 is the maximum. The 46-candidate first-pair outlier illustrates
startup noise; the 200 ms p95 target is specifically enforced on the 1,000-candidate case.

## Reviewed regression budgets

The checker requires the exact reference environment and measurement method. It rejects missing
cases, nonfinite values and an unrelated edit that fails to reuse output. Every recorded runtime
and driver phase has absolute numeric ceilings in the baseline: median and p95 time are the initial
values plus 50%, rounded up to nanoseconds; managed allocation median and p95 allow 10%, rounded up
to bytes. Zero-allocation cases retain a zero-byte budget. These are regression margins chosen for
launch/JIT and short-operation variability observed in the pilot and baseline, not confidence
intervals or promises of universal performance.

| Warm runtime case | Median ceiling µs | p95 ceiling µs | Managed B median ceiling | Managed B p95 ceiling |
| --- | ---: | ---: | ---: | ---: |
| plan | 195.350 | 209.024 | 74,325 | 74,325 |
| apply | 232.760 | 245.596 | 117,551 | 117,642 |
| probe | 0.003 | 0.003 | 0 | 0 |
| total | 439.043 | 498.946 | 191,876 | 191,923 |
| handwritten | 1.120 | 1.532 | 9,487 | 9,487 |
| scrutor | 175.473 | 188.911 | 137,766 | 137,766 |
| ledger-record | 211.581 | 231.440 | 343,411 | 343,411 |
| ledger-report | 152.985 | 162.360 | 494,332 | 494,332 |
| total-ledger | 844.221 | 890.780 | 1,035,437 | 1,035,437 |

In addition to those per-phase budgets, every paired-build median must remain at or below 200 ms;
the 1,000-candidate paired-build p95 and fresh-driver p95 must both remain at or below 200 ms.
The first-process driver has its own published baseline budget and is not mislabeled as added
compiler cost. Timing gates belong on this reference environment, not an arbitrary shared CI runner.
The deterministic checker tests can run anywhere and exercise failure paths without benchmarking.
Baseline replacement requires explicit review of changed code, environment, raw samples and budgets;
measurement and checking commands never regenerate it automatically.

Package-only consumers and executed native samples establish packaging/AOT evidence separately.
No external application migration or real broker performance is claimed by these measurements.
