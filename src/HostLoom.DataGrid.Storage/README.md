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
to the directory budget. This slice has no automatic garbage collector; eventual capacity
requires a separately designed owned-file cleanup workflow.

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
4096 entries/16 MiB and snapshots to 8 MiB; recovery also caps a generation at one million records.
These are bounded prototype settings. They do not establish production memory, disk or recovery
time acceptance. RollGeneration can reset physical record inventory without resetting logical
positions; it requires space for the old and new generation together.

`DataGridFileStorageTests` uses owned temporary directories, injected I/O-boundary interruptions,
every torn prefix of an uncommitted record, rehashed-invalid transitions and killed child writers.
`HostLoom.DataGrid.StorageProbe` exists only for those process tests. There is no three-process
deployment or public-client linearizability, real disk-full experiment,
bounded OS-I/O shutdown, telemetry or Native AOT evidence yet. The separate transport/runtime
projects now exercise actual Linux loopback TLS and file-backed consensus.
