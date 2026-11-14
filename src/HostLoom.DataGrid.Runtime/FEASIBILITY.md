# Reproduce a grid feasibility case

The opt-in runner exercises the public TLS SDK against three independently stoppable member
processes in the selected [local Linux ARM64/overlayfs fixture](ENVIRONMENT.md). It records
observations and capacity refusals. Numerical service targets are unset; a completed run is
execution evidence, not performance acceptance or production qualification.

## Before running

Use the repository's locked Python environment, Docker with native ARM64 Linux, the pinned SDK
image and a restored local NuGet cache described in the environment profile. Each execution
creates a new isolated container with two CPU quota, two GiB memory/no swap, no external network
and separate member directories. Build servers are shut down before starting the load driver.
The source snapshot and artifacts are private local files.
Do not run competing builds or benchmarks when collecting measurements. Other host activity
and Docker Desktop scheduling remain uncontrolled and must be considered when comparing runs.

Print the full plan without starting Docker:

```bash
just data-grid-feasibility-plan
```

It contains 27 cases: batches 1/32/128, concurrency 1/16/64, and seeds 17/29/43. Each full case
requests 100000 initial key writes, sixty seconds of warmup and a 300-second sample. Execute
one coordinate at a time, retaining every repeat, refusal and incomplete result:

```bash
uv run --locked python -B scripts/run-data-grid-feasibility.py \
  --execute --batch-size 128 --concurrency 1 --seed 43 \
  --report-dir /tmp/grid-feasibility-new-case
```

Use a new report directory for every execution. For runner validation, add `--smoke`: this uses
256 keys, one second of warmup and a five-second sample. Smoke is explicitly labeled and must
never be reported as the full workload. The runner accepts no external endpoints or existing
containers. It verifies ownership and removes only its exact container on exit/interruption.

## Optional process GC budgets

Add `--bounded-memory` to select the experimental `shared-2g-bounded-gc-v1` profile. The
container still has two GiB/no swap and two CPU quota. Only the load driver and member runtimes
receive explicit GC heap hard limits; restore/build processes use their normal configuration:

| Process | GC heap hard limit |
| --- | --- |
| Load driver | 192 MiB |
| Each of three members, including restarted members | 448 MiB |
| Sum of the four limits | 1536 MiB |
| Remaining container budget, unassigned | 512 MiB |

The runner sets `DOTNET_GCHeapHardLimit` with hexadecimal byte values (`C000000` and `1C000000`).
A [GC heap hard limit](https://learn.microsoft.com/en-us/dotnet/core/runtime-config/garbage-collector#heap-hard-limit)
bounds GC heap/bookkeeping commit, not process RSS or total container memory. The remaining
512 MiB is not a reservation: native allocation, JIT, stacks, TLS, file cache and kernel charges
also consume the container budget. Earlier/more frequent collection can reduce throughput or
still fail to accommodate live/transient objects. This is an experiment, not a production GC
policy, a service NFR or established 100000-key capacity.

Without the flag, the profile is `runtime_defaults`. Reports record the selected profile and
effective runtime budgets. The validator requires the expected driver budget and observations
from all three correctly budgeted members before accepting bounded-profile evidence. Keep
profiles separate when comparing results; do not combine their measurements into one matrix.

## Workload and limits

All keys are 32 bytes, all values 1024 bytes with a self-checking SHA-256 payload. The fixed
seed selects reproducible key sequences. Each worker uses its own SDK/session and one outstanding
invocation. Four new reads alternate with one new Put; unresolved writes retry the same encoded
command, session and sequence before any newer mutation. Retries can change the observed
read/write invocation ratio. This is closed-loop load: slow replies reduce arrivals and can hide
overload latency. No open-loop arrival model or coordinated-omission correction is claimed.

Even keys use Never expiration; odd keys use relative sixty-second TTL. Population writes Never
keys first, then TTL keys, using four setup SDKs and a separate setup principal. It acknowledges
every initial write or records infeasibility. Acknowledged population is not a claim that all
100000 keys were simultaneously live: TTL starts at each write. There is no refill during
warmup/sample, so expired reads can miss and writes can recreate keys. Values returned by reads
are checked outside invocation timing. Batch size applies to individual keys per SDK request;
all keys in a write batch use the same expiration kind.

The fixture explicitly configures one map at 100000 keys/128 MiB within the existing group
100000-key/256-MiB ceiling. Runtime admission limits remain active: 64 sessions/principal,
eight outstanding operations/principal, eight client starts/second at a listener, and SDK
limits of eight calls, two connection starts/second, three attempts and five-second deadline
per instance. Setup and workload use separate synthetic principals with the same map grants.
These byte quotas charge logical state, not managed heap or resident memory; see the
[resource inventory](RESOURCE_LIMITS.md) for the remaining storage, snapshot and transport ceilings.
High requested concurrency does not imply high admitted throughput. Refusals and unknown
outcomes are observations, not successful operations or permission to raise production limits.

## Measurement definitions

Each phase retains every started SDK invocation, including cancellation and late completion.
The stopwatch surrounds `ExecuteAsync`, including SDK queueing/retry/network/actor time but
excluding request construction, checksum validation and raw-file serialization. Individual
invocation durations use microseconds. p50/p95/p99 use nearest rank of individual observations,
never averages divided by batch size. Successful, all normally returned calls (including
refusals), and resolved logical writes have separate distributions. Late or cancelled calls
are excluded from percentiles and counted as censored. Empty distributions contain null values.
Logical write latency includes retries, but a write originating in warmup and resolving in the
sample is excluded from the sample's logical distribution and remains in raw observations.

Request throughput is successful in-window SDK invocations divided by requested phase seconds;
item throughput multiplies by processed batch items. Successful reads include misses. Item
rejections make a call unsuccessful. Failure rates divide by **all started invocations**, including
late/cancelled calls: unknown means `OutcomeUnknown`; refusal means a returned failure with
`MayHaveBeenAdmitted == false`; error includes cancellations, returned failures and item failures.
SDK timeout counts come from the dedicated `hostloom.datagrid.client.timeouts` counter, which
excludes caller cancellation. Unknown outcomes are not inferred to be timeouts. The counter
listener is active only during its measured phase.

One-second samples record client/member process RSS, cumulative CPU time, raw Linux `/proc/PID/io`
counters, member store-directory bytes/file counts, and container cgroup memory current/peak
bytes and OOM events. The cgroup peak includes the preceding build; event increments and member
samples identify changes during load. Missing/racing processes/files are
recorded as unavailable. CPU deltas can be derived from cumulative observations; RSS sums can
double-count shared pages. Directory bytes are not physical disk writes or free space, and
`/proc/io` is not network I/O. Network bytes, allocation profiles and complete GC pause distributions are not measured. Reported framework, server-GC and processor count describe the executing process;
JIT/tiering/PGO use image defaults. Monitoring/counter/checksum overhead is included in the load
process; raw call serialization occurs after each phase and can add an inter-phase gap.

The process sampler also records approximate managed bytes, cumulative all-thread allocation,
last-GC heap/committed/fragmented bytes, generation collection counts and cgroup `memory.stat`.
It queries a read-only probe command and never forces collection. Approximate managed bytes can
include garbage awaiting collection; last-GC values are not current live-object counts or peaks.
Allocation counters may lag. GC memory-load/available-memory observations belong to each runtime
and are not private reservations of the container budget. RSS, GC committed memory and cgroup
anonymous/file/kernel charges have different accounting and must not be added together.

The sampler also retains member role/term, leader hint, commit/applied/snapshot positions,
ready/stopped flags, and runtime/listener failure outer/base type and bounded base stack.
Exception messages and payloads are omitted. Status and failure fields are cached passive
observations, not an atomic state transition or a live quorum assertion. Probe request/response
bounds and resource rows use the driver's stopwatch timeline. GC/CPU counters restart with a new
PID; retained term and log positions are separate persistence state.
GC observations include cumulative pause milliseconds, last completed GC index, cumulative pause
percentage and the two pause durations associated with that GC. The
[cumulative pause duration](https://learn.microsoft.com/en-us/dotnet/api/system.gc.gettotalpauseduration?view=net-10.0) and
[last-GC fields](https://learn.microsoft.com/en-us/dotnet/api/system.gcmemoryinfo?view=net-10.0) are counters,
not a complete pause-event trace. Repeated GC indexes repeat the same last-GC observations;
several collections can occur between samples. Pause deltas can correlate with lost progress
but cannot alone assign causality or reconstruct every pause's timing.

`resolutions.jsonl` retains setup, settle and recovery SDK invocations outside the timed phases.
Each row has logical identity, mutation digest, phase, driver start/end seconds, returned failure
and physical attempt member/ID/submitted/wire-reply metadata. Request/value bytes are not retained.
Cancelled invocations have null attempt data because the SDK did not return an outcome; this is
not evidence of zero attempts or non-admission. Completed SDK outcomes expose their observed
attempts but no individual attempt latency. The journal flushes after each record and stops at
10000 records rather than truncating. Its hashing/serialization/flush overhead affects setup
and diagnostic workload feasibility, outside measured invocation durations.

After sampling, the runner drains unresolved identities within a bounded budget. It then writes
a canary, verifies application on all members, kills the current leader, and measures time to a
survivor's strong read with exact value/version and retained mutation replay. It rejoins that
member, abruptly stops all three and measures whole-group restart to retained replay/read.
Recovery is outside workload timing. These are elapsed observations, not RTO acceptance, power
loss, host failure or an arbitrary concurrent-history correctness proof.

## Reports and stop conditions

The environment artifacts include source archive/hash, pinned tuple, source/build log, test XML
and exact-container cleanup result. Additional files are:

| File | Contents |
| --- | --- |
| `plan.json` | Coordinate, smoke/full label, process memory profile, entrypoint hash and repeat seeds |
| `feasibility/case.json` | Effective limits, timing rules, phase summaries, rates, population/recovery results and case status |
| `feasibility/warmup-calls.jsonl`, `sample-calls.jsonl` | Per-invocation timings, outcomes, batch items, mutation sequence/digest and cross-phase resolution flags |
| `feasibility/resources.jsonl` | One-second process, member progress/fault, GC-pause and directory observations by phase |
| `feasibility/resolutions.jsonl` | Bounded setup/settle/recovery call and physical-attempt provenance |

The validator recomputes percentiles, throughput and failure accounting from raw observations
and rejects identity changes, missing diagnostics/attempt provenance, inconsistent counts and false smoke/full labels. Its controller
may execute successfully while the case is `infeasible` or `incomplete_budget`; inspect both
statuses in `report.json`. A completed case requires resolved writes and verified recovery.
Checksum/identity corruption, unexpected driver exceptions, failed/skipped driver, lost raw observations,
or failed ownership/cleanup fail execution. Partial population acknowledgements and the stopping phase are retained on failure. An interrupted
setup mutation may have been admitted; acknowledged progress is not rollback evidence.
`unresolved_mutations` counts timed workers, which have not started during population. Resolving
one immutable identity is bounded to 45 seconds; an expired resolution reports the last returned
failure separately from an enclosing deadline. Population is bounded to 300 seconds, identity drain
to 45 seconds, the case to 840 seconds and the container command to 900 seconds. Each phase
permits at most 100000 invocation records; reaching that bound stops rather than truncates.
No numerical latency/throughput stop-go threshold is invented. A full 27-case campaign and
explicit target decisions remain necessary before performance acceptance.

An increase in cgroup OOM-kill count after the driver's first resource snapshot immediately
cancels the case and reports `infeasible`. The triggering snapshot is flushed before cancellation.
The runner does not keep timing a baseline workload after an unintended OOM member loss.

Full-size capacity remains unqualified in this two-GiB fixture. Runtime-default attempts hit
cgroup OOM; bounded-GC attempts expose managed allocation failures without a cgroup kill. A
member's runtime can fail while its probe process remains observable. Captured member faults
invalidate workload feasibility even when the driver correctly records an `incomplete_budget`
case. A completed smoke does not resolve this constraint, and changed partial counts/timings
are not accepted capacity or throughput gains.

Publication readback now compares disk bytes against the validated canonical snapshot through
a 16-KiB buffer. Focused 10000-key checks reduce managed allocation for this operation from
about 50 MB to 33 KB, retaining exact readback, seed parsing and fresh-Open semantic validation.
The subsequent full-size repeat acknowledges 56912 keys, then stops during population with a
managed OOM while constructing the expected snapshot for the independent committed-prefix
comparison. No full-size warmup, sample or retained recovery was completed.

Next reduce that temporary canonical encoding cost without weakening the known-prefix check,
then resolve the [incoming snapshot admission ceiling](RESOURCE_LIMITS.md#retained-result-and-snapshot-bounds)
with explicit bounded ownership. Both process profiles remain unqualified until full workload,
catch-up and recovery evidence exists. Lifetime/last-GC pause observations provide context;
they are not a complete event trace or a numerical service target.
