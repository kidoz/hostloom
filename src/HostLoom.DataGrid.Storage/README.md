# HostLoom.DataGrid.Storage

Experimental, non-packable file storage for the native data-grid prototype. The dependency-free
grid kernel has no filesystem dependency; this project translates its internal persistence
effects into WAL records and generation publication. It has no network, host or cache adapter.

`DataGridCurrentAnchor` implements the 88-byte v1 HLGC anchor with nonempty incarnation, member
and generation identities, network-order UUIDs, and SHA-256. The internal file store uses CURRENT
as its sole recovery authority. It never chooses the newest file or falls back to an old generation
after a selected generation fails validation. Bootstrap requires an explicitly empty, existing
owned directory. Open never silently bootstraps a missing CURRENT.

The internal `DataGridFileStore` requires serialized ownership. It holds an exclusive LOCK file
through disposal and validates identity/configuration, frame sequence/checksum and all WAL payload
transitions before exposing a recovered image. Recovery applies only through the durable commit
checkpoint and preserves the uncommitted suffix. Every incomplete header, payload or digest,
unknown format/kind, malformed command, invalid term/vote/log transition or corrupt referenced
snapshot refuses startup. No automatic truncation, repair or generation fallback occurs.

Persist validates a candidate before writing. It appends term/vote, logical truncation/replacement
and commit records in dependency order and calls `FileStream.Flush(true)` before completing the
effect. The caller must then acknowledge that exact effect to the Raft actor. File storage cannot
establish quorum by itself. An I/O error or cancellation after submission faults the owner and
requires validated restart. Cancellation before submission leaves the owner usable. Admission
capacity checks reject before writing. Tokens are checked between synchronous persistence steps;
they cannot interrupt an operating-system flush.

Snapshot installation/compaction publishes a new generation: write, flush and verify an owned
image; rename it uniquely and flush the directory; validate and flush the seeded WAL; rename it
and flush the directory; write/flush a temporary CURRENT, atomically replace CURRENT and flush
the directory again. Only then does the owner switch WALs and complete the effect. Local term/vote
and a matching uncommitted suffix survive publication. Known application prefixes are checked
against the proposed snapshot. Old generations/images and incomplete temporaries remain charged
to the directory budget until collected. Internal CollectObsoleteFiles validates the current
anchor/ownership and the bounded inventory, then removes at most 32 known obsolete files per
call. It keeps CURRENT, LOCK, the selected WAL and its referenced snapshot. Before the first
deletion it flushes the recovered WAL and directory, sealing a CURRENT selection recovered
after an interrupted rename; it flushes the directory again after deletion. No active publication
runs concurrently on this serialized owner. Unknown files, changed roots or I/O failures fence
the collector; there is no fallback, truncation or silent repair. Runtime startup, periodic
maintenance and compaction preparation drive collection. A full batch defers compaction to a
later turn. Inventories already outside the startup limits still require explicit recovery.

The prototype permits Linux/macOS x64 and arm64. Directory handles use native `open` with
directory/no-follow/close-on-exec flags, and `RandomAccess.FlushToDisk` supplies directory flushing.
The directory and its parents must be private to a trusted local owner. Path checks reject
reparse points, subdirectories and foreign filenames/identities; they are not protection against
a local adversary racing path replacement, creating hard links or substituting special files.
Linux filesystem and host/power-loss qualification remain pending. Current evidence includes macOS local files/child-process recovery and Linux ARM64 container
recovery/integration tests; it is not a supported production durability claim. Linux ARM64 uses
its architecture-specific directory-open flags, distinct from x64.

Limits are 1 GiB per WAL, 4 GiB total owned file bytes, 1 GiB reserved free space and 4096 files.
Publication reserves new files while retaining all old files' charge. Tests can lower limits or
raise the free-space reserve. The internal Raft prototype still restricts retained logs to
4096 entries/16 MiB and snapshots to the shared 512-MiB format ceiling; recovery also caps a
generation at one million records. Current group quotas bound encoded state below that ceiling;
larger allocations/transfers are not a measured production resource guarantee.
These are bounded prototype settings. They do not establish production memory, disk or recovery
time acceptance. RollGeneration can reset physical record inventory without resetting logical
positions; it requires space for the old and new generation together.

`DataGridFileStorageTests` uses owned temporary directories, injected I/O-boundary interruptions,
every torn prefix of an uncommitted record, rehashed-invalid transitions and killed child writers.
`HostLoom.DataGrid.StorageProbe` exists only for those process tests. Separate runtime tests cover three local member processes, public-client histories and Linux
loopback TLS. These experiments do not establish independent-host isolation, bounded native-I/O
shutdown, supported-device durability or Native AOT.


Internal `Progress` exposes cached WAL/snapshot bytes, current-generation record count and commit
position from the last successful owned publication, plus fault/closed flags. It performs no
filesystem access and remains readable after disposal. Failed writes can change physical bytes
without publishing new metadata. Generation rollover can reduce record count without reducing
logical positions; these fields are not total directory usage, free space or device durability.

`DataGridStorageDiagnostics.MeterName` (`HostLoom.DataGrid.Storage`) records Open recovery count
and duration with fixed `recovered`/`failed` outcomes. It includes ownership/inventory/WAL/snapshot
validation and constructor refusals, excludes bootstrap and subsequent actor replay/startup, and
emits no path, certificate, key/value, identity or exception-message labels. Measurements establish
emission, not collector delivery or a recovery-time target. Synchronous listeners are trusted
instrumentation code and must not throw or block.


Runtime qualification also opens stores with lowered WAL/directory budgets or an unattainable
free-space reserve. Refusal fences the actor, preserves the original WAL bytes and commit state,
and releases directory ownership after actual cleanup. A deliberately blocked storage observer
checks ownership retention through a runtime disposal timeout and successful reopen after release.
These are declared-budget and deterministic blocking checks, not an actual disk-full, power-loss,
filesystem/device-durability or uninterruptible native-I/O experiment.


Recovery telemetry reports each explicit Open and the configured maps covered by its group-wide
started/recovered/failed phases. Durations are group Open time repeated for each map, excluding
subsequent runtime construction/replay. A refusal before ownership is an attempted Open failure,
not a claim that another owner stopped. Only bounded ordinals/fixed outcomes are labels; paths
and exception messages are excluded. See the [signal inventory](../../docs/reference/observability.md#experimental-native-grid-instruments).


The [effective resource inventory](../HostLoom.DataGrid.Runtime/RESOURCE_LIMITS.md) records each owner/scope, tighter admission
limits, refusal/release behavior, derived retained-result bounds and executable boundary checks.
These implementation ceilings are not process-memory, performance or recovery-time guarantees.


## Reproduce bounded disk exhaustion

Run `bash scripts/test-data-grid-disk-full.sh` from the repository root after restoring the solution.
It requires Docker, the local .NET SDK/NuGet cache and `uuidgen`. The runner uses a pinned Linux ARM64
SDK image, read-only source/cache mounts, a private build copy, a two-GiB container memory limit
and three private 32-MiB tmpfs mounts. It fills only a random owned directory on those mounts,
checks owner markers and mount type/capacity, and removes its exact container on exit. It has a
fifteen-minute overall timeout; each fill has a five-second/32-MiB bound. It does not select an
external path or fill the host filesystem. Set `HOSTLOOM_DATA_GRID_FULL_TESTS=1` to include the
broader grid suite and local-process experiments. The separate long session-capacity opt-in
remains disabled. Normal solution tests skip the nine disk-full cases.

The test-only free-space reserve is zero because the bounded filesystem is smaller than the
normal one-GiB reserve. The WAL/directory/file ceilings and production defaults are unchanged.
A separate one-MiB reserve case demonstrates pre-I/O refusal. Observation points fill tmpfs
after successful reservation; normal FileStream writes/flushes must then report errno 28.
A small write can still fit an already allocated final page; the TLS experiment releases space
after any successful flush and repeats the bounded fill before the next write.

The directed tests cover zero-byte and partial WAL appends, snapshot/seeded-generation/CURRENT
publication, retained immutable request identities, and corrupt/torn selected-root refusal.
Publication failure preserves the old CURRENT and selected WAL, allowing restart and replay
after space release. A clean append failure can also restart. **A partial selected-WAL append
leaves a torn tail: freeing space alone does not make that member restartable.** Startup preserves
its files and refuses; no automatic repair, truncation, older-generation fallback or member reseed
is implemented. Keep that member stopped and preserve the directory for diagnosis. A healthy
two-member majority can recover its acknowledged requests and resolve the original unknown
request identity, but this does not restore the third member. A safe member recovery/rejoin
procedure remains an open implementation requirement. Never delete CURRENT or truncate the WAL
as an implicit recovery step.

Tmpfs exercises real allocation/write ENOSPC and buffered flush failure. The observed seeded-WAL
and anchor exceptions surface from disposal retrying buffered output during unwind; they do not
prove that the fsync syscall itself returned ENOSPC. The first flush error is not separately
retained. This experiment does not establish
block-device delayed allocation, fsync/device failure, storage persistence across container
removal, host/power-loss durability, recovery time or workload capacity.
