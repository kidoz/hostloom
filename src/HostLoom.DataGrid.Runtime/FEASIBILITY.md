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

Publication readback compares disk bytes against the validated canonical snapshot through a
16-KiB buffer. The independent known-prefix check also uses the same canonical encoder as
snapshot capture, comparing forward bytes without creating a second full encoded image. Focused
10000-key checks retain about 33 KB of managed readback allocation; prefix-check overhead above
required reconstruction falls from 11.75 MB to 0.60 MB. Reconstruction itself still allocates
application state. The v1 golden bytes, semantic/digest checks and fresh Open validation remain.

Before the incoming handoff change, the full-size repeat acknowledged 78928 keys, then stopped during population with a
managed OOM during independent reconstruction/replay, before canonical comparison. No full-size
warmup, sample or retained recovery was completed. An allocation stack identifies where the
last request failed; it does not by itself identify the dominant retained memory owner. Changed
partial counts/timings do not establish supported capacity or a throughput improvement.

The [incoming snapshot handoff](RESOURCE_LIMITS.md#retained-result-and-snapshot-bounds) now
reserves one exact-byte image separately from ordinary control metadata and waits for actual
owner completion across cancellation. Nineteen focused Linux checks include a real TLS image
above 16 MiB, reopen/replay and credential/stop/fault ownership. The following full-size repeat
acknowledges 91216 keys, then stops during population with managed OOM in snapshot capture and
independent prefix reconstruction. No cgroup OOM kill or full-size warmup/sample/recovery occurs.
This closes the former incoming admission defect; it does not establish aggregate memory fit.

Those attempts motivated retained-owner/overlap measurements and the verifier change below.
Independent prefix/disk checks remain. Partial counts are not a throughput comparison. Both
process profiles remain unqualified until full workload, catch-up and recovery evidence exists.
Lifetime/last-GC pause observations provide context, not a complete event trace or service target.


A subsequent store change makes the independent prefix verifier borrow read-only value slices
from its already selected immutable snapshot. It rebuilds its own keys, indexes and replay state;
public restore and entry/snapshot reads retain copy isolation. For the same 10000-key/1024-byte
case, known-prefix Persist allocation falls from 14.44 MB to 4.04 MB. This is an allocation
observation, not full-size workload acceptance. Capture, candidate validation, metadata and replay
still allocate; the unchanged full coordinate and recovery gates remain necessary.

The corresponding unprofiled full-size repeat stops during population at 98640 acknowledged
writes, with managed OOM in exact snapshot capture on two members. It completes no full-size
warmup/sample/recovery and records no cgroup OOM kill. That result motivated bounded encoded content across every runtime layer, described below.
Numerical targets and production capacity remain open.

Runtime snapshot content now uses immutable owned blocks at most 64 KiB through capture,
ordered SHA, canonical comparison, logical section parsing, disk write/readback/fresh Open and
peer send/assembly. Public GetContent remains an explicit contiguous copy; runtime paths do
not call it. Ordinary restore still copies values; the independent store verifier borrows
within a block and copies crossing values once. V1 golden bytes and all semantic/digest/disk,
retry/authority and actual-completion ownership checks remain. The 11.15-MB shape regression
now retains byte arrays at most 65536 bytes, with bounded block metadata; this does not bound
aggregate application/image/verifier/native memory.

The corresponding full repeat acknowledges 99664 initial writes, then stops in population
with managed OOM during the temporary semantic-validation application/value/tree reconstruction
on two members. It completes no full warmup/sample/recovery and records no cgroup OOM kill.
The subsequent validation change uses the shared v1 parser to scan candidate snapshots. It
checks digest, identity, ordering, versions, quotas and sessions while advancing over bounded
value payloads. The independent prior-prefix verifier still reconstructs and replays durable
state. First validation of a 10000-entry image on macOS ARM64 allocated 161520 bytes after the
change, down from 14243528 bytes on the original implementation.

The full repeat for batch 128, concurrency 1 and seed 43 completed in the same bounded-memory
fixture. It acknowledged all 100000 initial writes, finished warmup and sampling, settled
pending identities, and verified leader-loss and whole-group recovery. The 300-second sample
had 599 successful calls within the window out of 600 started. Cgroup OOM counters remained at
zero.

The concurrency 16 follow-up used the same batch, seed and memory profile. It acknowledged
100000 writes and settled all outstanding mutations. A member hit managed OOM while capturing
a compaction snapshot during the sample, with zero cgroup OOMs or OOM kills. The precheck that
requires all three members to apply the recovery canary failed, leaving the intentional
leader-loss and whole-group restart checks pending. The sample includes time spent in a degraded
quorum and recorded 2376 in-window successes and 2324 refusals out of 4701 started calls.
One of 27 coordinates has completed. The remaining matrix work, contextual measurements and
target decisions remain open.
