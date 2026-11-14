# HostLoom.DataGrid

Experimental, dependency-free byte-map/session model, internal three-member Raft transition
model and binary contract primitives. This non-packable project has no network transport,
filesystem dependency or cache backend. The separate [storage project](../HostLoom.DataGrid.Storage/README.md)
implements experimental WAL/recovery and generation publication. Kernel tests cover modeled quorum/commit behavior;
they establish no real-cluster, device-durability, performance or Native AOT support.

`DataGridKey` copies its input and uses ordinal byte equality. Text conversion uses strict UTF-8
without normalization, so canonically equivalent Unicode strings can be distinct keys. Keys
contain 1–1024 bytes. Exported arrays are independent copies; hash codes do not define routing.

`DataGridEntryVersion` identifies a cluster incarnation, positive committed log index, and
zero-based item index within a command. The local state machine never reuses committed positions after deletion or snapshot restore. Only equality within that incarnation has meaning.
The default version means no entry and cannot be serialized. The fixed 28-byte encoding uses
network-order UUID bytes and big-endian signed integers. These versions are not fencing tokens
for external resources.

`DataGridTimestamp` represents committed grid time as UTC ticks (100 ns), preserving
`TimeSpan` precision. A leader samples its injected `TimeProvider` to propose a timestamp;
application uses only committed proposals and advances to the maximum of previous and proposed
time. Sampling alone does not advance replicated state. Snapshot recovery must retain the time
high-water mark. These helpers do not implement replication or persistence.

`DataGridExpiration` holds an absolute deadline or `Never` (also the default). `After` accepts a
positive TTL and rejects an unrepresentable deadline without rounding or clamping. At or beyond
the deadline an entry is logically absent, even before cleanup. Remaining TTL is exact in grid
time: positive while live, zero when expired, and null for no expiry. A map must not return an
expired payload. Replacement creates a new deadline; a retried mutation retains its original one.

A backward clock jump freezes grid time until the clock catches up; expired data cannot return
when application preserves the high-water mark. A forward jump can expire data early in elapsed
time. No physical elapsed-time accuracy is promised across arbitrary clock jumps, pauses or
outages. Strong reads must establish authority and commit a fresh time sample before capturing
value/version/remaining TTL together. The later cache adapter still requires conformance evidence.

`DataGridWalRecord` encodes owned v1 WAL frames with a 20-byte big-endian header and SHA-256
trailer. The 2 MiB limit includes header and checksum. Decoding requires exactly one complete
frame and its expected positive generation-local record number; unknown formats/kinds, invalid
lengths, gaps, trailing bytes and checksum failures throw `InvalidDataException`. Payloads are
opaque to this codec; a valid checksum does not validate a Raft transition or command.

`DataGridSnapshotDescriptor` encodes the fixed 72-byte manifest payload: snapshot UUID,
committed index/term, file size and SHA-256 digest. It bounds images to 512 MiB and owns its digest.
The descriptor requires a checksummed WAL envelope and does not validate or publish a snapshot
file. Neither codec performs I/O, durable flushing, recovery, truncation or snapshot installation.
The initial recovery design refuses incomplete/corrupt records and requires explicit verified
recovery; no automatic torn-tail repair is implemented. Checksums do not authenticate hostile data.

`DataGridMutationCommand` encodes canonical ordered Put, PutIfAbsent, CompareExchange, Remove and CompareRemove
batches. It owns item/value inputs, rejects duplicate keys and invalid operation/expiry fields,
and hashes the canonical bytes for retained retry identity. Commands carry map/session/request,
not leader time or physical attempt UUID. The 1048414-byte cap reserves all headers needed for
one replicated AppendRequest. This codec does not execute operations or retain deduplication state.

`DataGridProtocolFrame` encodes a bounded 72-byte-header v1 envelope with incarnation, sender,
session/request and attempt identities. Its decoder checks expected connection identities and
message-kind/session shape; it does not authenticate or authorize callers, validate peer payloads,
or dispatch operations. `DataGridMutationCommand.ReadFrom(frame)` also checks command/envelope
session/request agreement and CAS incarnation. No plaintext deployment is supported by the design;
TLS and authenticated dispatch belong to the separate experimental transport/runtime layers.
`DataGridClientCodec` now validates unary client hello, request, result and failure bodies; it
performs no networking or authorization. Local HLGR wrappers and HLGS/HLGB images have in-memory
codecs. No distributed correctness claim follows from format validation.

`DataGridBootstrapConfiguration` validates immutable three-member and per-map declarations,
orders UUIDs by network bytes, and encodes canonical HLGF bytes and their SHA-256 digest. Maps
explicitly identify authoritative/derived ownership, durable/refill responsibility, supported
policy codes and key/byte quotas. Per-map quota sums must fit group capacity. Unsupported policies,
missing responsibilities, duplicate IDs/names/endpoints and invalid text/DNS declarations reject.
The declaration encoder performs no DNS/TLS/bootstrap I/O or runtime quota enforcement.

`DataGridKeyComparer` provides raw byte ordering for local map trees and snapshot cursors.
The runtime design uses ordered keys to avoid relying on untrusted hash distribution. The
fair admission scheduler remains unimplemented. The internal consensus model is driven through
explicit effects; no production performance, availability, durability or Native AOT support
follows from model tests.

`DataGridLocalStateMachine` applies typed `DataGridCommittedCommand` inputs at contiguous committed
positions. A single caller must serialize Apply, Read/ReadMany and snapshot capture; it is not
thread safe. A command advances only the supplied committed grid time. Local reads establish no
quorum or fresh-time barrier. The driver must supply those before using them as distributed reads.

The model supports per-key mutations, explicit versions, deadline/policy and per-map/group quota
checks, partial batch results, and bounded latest-result session replay. Logical expiration occurs
before physical reclamation; Reclaim inspects at most 128 candidates with a persisted wrapping
cursor. TimeAdvance retires at most 128 due sessions from the bounded inventory. Unknown old
sessions and retired request numbers never execute again. No background task or local clock runs.

`DataGridCommittedCommandCodec` implements HLGR wrapper bytes. `SaveSnapshot` produces an owned
HLGS/HLGB image/descriptor; `Restore` validates a separate candidate's hash, configuration,
positions, sections, quotas, sessions and retained results before returning it. It preserves time,
deadlines, versions and allocator/cursor state. These methods do not flush files or publish CURRENT.
The separate `HostLoom.DataGrid.Testing` project supplies bounded in-memory delivery/journal seams
and seeded stop/restart/fault scripts. Real persistence/network behavior remains later work.


The internal `Consensus` model has exactly three fixed voters in canonical configuration order.
Every method consumes one serialized event and returns persistence, packet, timer or completion
effects. Term/vote/log/commit/snapshot changes require an acknowledged immutable memory transaction
before dependent replies. Restart epochs and correlated tokens reject stale persistence completions
and RPC replies. An uncertain write faults the member until restart from the actual modeled store.

A leader commits only current-term entries persisted by two voters and applies after its own
commit checkpoint is persisted. A new leader first commits a time entry. Each distributed-model
read commits another current-term time entry and captures the value at that index; it uses no
lease. A minority leader cannot complete writes or reads. Unknown mutation outcomes retain their
logical identity for retry; connection loss does not create a new session or request number.

Compaction retains uncommitted suffixes and local term/vote. Snapshot catch-up validates an owned
application image, retains a matching suffix and preserves the receiver's hard state. The model transfer
uses one typed in-memory image; the separate live peer layer uses bounded chunks. Fixture limits
are 4096 log entries/16 MiB encoded log bytes and the shared 512-MiB v1 snapshot ceiling.
Client admission reserves 128 entries and one maximum encoded command for internal work.
Configured state has a conservative encoded upper bound of 291016890 bytes, including metadata
and retained results. These bounds do not establish supported heap usage or production capacity.
The runtime schedules replicated cleanup and proactive compaction; a standalone model driver
must explicitly drive those effects. Compaction preparation distinguishes busy, unchanged and
oversized requests. The separate
[storage layer](../HostLoom.DataGrid.Storage/README.md) now implements WAL/publication effects
with local-file and killed-process tests. Separate [TLS peers](../HostLoom.DataGrid.Transport/README.md)
and an [actor runtime](../HostLoom.DataGrid.Runtime/README.md) now integrate the effects in Linux
loopback experiments. The experimental [client SDK](../HostLoom.DataGrid.Client/README.md) now
adds authenticated unary calls and bounded history checks including pending/uncertain writes
and absolute expiry. Opt-in process tests cover leader kill, minority recovery and full-group
restart. Extended histories, deployment and production filesystem qualification remain pending. The kernel itself
performs no I/O.


The [effective resource inventory](../HostLoom.DataGrid.Runtime/RESOURCE_LIMITS.md) records each owner/scope, tighter admission
limits, refusal/release behavior, derived retained-result bounds and executable boundary checks.
These implementation ceilings are not process-memory, performance or recovery-time guarantees.
