# HostLoom.DataGrid.Runtime

Experimental, non-packable integration of the internal Raft actor, file store and TLS peers.
An internal client receiver now connects the separate authenticated listener to actor admission,
with the TLS-bound principal supplying session ownership. There is no dependency-injection host
or production support claim. Internal Propose/Read calls are local test entry points. Both three-actor and opt-in three-process Linux tests communicate over actual loopback TLS
and retain separate owned directories. The process runner is test infrastructure.

The runtime serializes all actor/application/storage access. It consumes persistence effects
through the file store, acknowledges their exact tokens after flush/publication, then sends
dependent replies. A write failure faults the actor and leaves admitted mutations uncertain.
Only a current-term quorum commit can complete an operation. Reads append fresh committed-time
barriers. Batch reads capture the ordered vector and grid time before the actor advances to the
next committed command; oversized responses fail with capacity instead of faulting the actor. A Ready diagnostic alone is not evidence that a quorum is currently reachable.

Separate control/client queues reserve 256/128 items and 16 MiB each. Control receives at most
eight consecutive turns before an available local client gets a turn. Up to 128 local operations,
256 active timers and 64 diagnostic waiters are admitted. There is one sender worker and one
latest pending request per peer; replacing work cancels the preceding exchange rather than
building a chain of tasks. Correlation/term checks discard obsolete replies. Snapshot messages
refer to bounded immutable images and are encoded as individual 64 KiB chunks at the transport.
The shared queue component enforces both count and byte limits atomically and drains its
reservations on shutdown/fault. Within the client lane, authenticated principals have FIFO queues
and take one turn each in round-robin order. A new or returning principal joins the rotation's
tail. Queue counts release on dequeue; a separate operation reservation retains the charge
through queued, executing and pending consensus work. Each authenticated principal holds at most
eight operations/4 MiB within the shared 128-operation/16-MiB client budget. Trusted local
Propose/Read test calls share one FIFO and the global budget without the external-principal cap.
The TLS-bound identity selects the same budget for reads, mutations and both session operations
across credential generations; it is never a metric label.

Caller cancellation, disconnect or physical timeout cannot refund retained actor work. Canceled
queued actions release when dequeued without executing; pending charges release on a terminal
Raft completion or actual owner stop/fault drain. A terminal uncertain completion ends the wait's
reservation and does not prove rollback: log and replay state have separate lifetimes and limits.
Admission stops before cancellation is signaled. Failed queue insertion releases its reservation;
shutdown clears pending completions and releases each charge once. Empty identity accounting is
removed on release and bounded by at most 128 held operations. Encoded charges include requested
keys (including trusted single-key reads), or canonical committed commands, plus 128 bytes of
submission overhead. They exclude response/log/replay state, copies and managed heap overhead.

Portable tests cover concurrent producers, byte/item saturation, principal churn and exact turn
ordering. Linux wire cases verify definite per-principal refusal and another principal's queue
admission during blocked persistence, alongside full-lane fault/shutdown cleanup. Further cases
retain a principal's share after canceled pending reads, reject all four request kinds over TLS,
allow a second principal and reuse the share after quorum recovery or a higher term. Blocked-WAL
cases distinguish caller timeout from actual operation release on recovery, stop or storage fault.
The anonymous `runtime.admission.active`/`runtime.admission.bytes` meter series observe retained
charges; queue series still count only waiting work and `runtime.operations.active` counts callers.
These overlapping boundaries must not be summed as independent operations or heap bytes. Zero
operation reservations do not establish completed transport/storage cleanup or expired replay state. This scheduling
rule does not guarantee fair socket/handshake admission, end-to-end completion or CPU time;
global request/pending limits, OS I/O and quorum availability still constrain progress. These
prototype budgets establish no production memory or workload capacity.

Local operations have an independent five-second physical deadline. A timed-out mutation is
OutcomeUnknown; a timed-out read is Unavailable. Caller cancellation after posting may also
leave a mutation admitted. Original logical session/request bytes stay caller-owned for retry;
the runtime does not regenerate them or replay through another backend. Retained results survive
leader change and file recovery. Process epochs must be positive and not reused by the owner
across restarts; wire attempts encode the epoch and monotonic sequence.

Authenticated snapshot progress removes stale authority and persists a higher term before
admission; complete installation uses the existing Raft matching/suffix and file-publication
rules. Already durable committed coverage can finish a retried/older image without rollback or
redundant publication. Snapshot RPC timers allow the selected 60-second transfer window.

Construction verifies store/member/configuration agreement, transfers store/listener ownership
and unwinds startup failure. An optional internal security context is shared with the client
listener so one reload publishes member credentials and client policy together. Keep that owner
alive until both runtime and client listener finish disposal; it owns captured certificate material.
Reload retires connections without rolling back work already admitted to the actor. Disposal
stops admission, cancels and joins timers, peer senders and TLS sessions, then releases storage.
Concurrent disposals join the same shutdown task. A ten-second wait reports a timeout if a
synchronous filesystem call cannot stop; the actor retains ownership until that call returns
and its cleanup actually finishes. This does not establish a hard OS-I/O shutdown bound.

Tests cover live election/replication/reads, one-node leader restart, retained-result retry,
whole-group restart, minority unavailability, uncertain write recovery and chunked snapshot
catch-up. Separate live-actor tests cover atomic credential publication, revocation, expiry and
root replacement; process tests currently exercise construction from the shared security owner.
Broader fault/partition schedules, deployment hosting, rolling credential orchestration,
telemetry, full resource/security qualification and workload feasibility remain required.


`DataGridRuntimeDiagnostics.MeterName` (`HostLoom.DataGrid.Runtime`) publishes fixed-label queue
item/encoded-byte deltas and refusals, local operation counts/active callers/duration, and
synchronous persistence duration. Start metric collection before construction to observe a full
up-down baseline. Labels contain fixed operation/outcome/lane/reason values only. These are local
caller and persistence-call observations, not unique committed-write counts or quorum readiness.
Canceled callers may leave admitted work; no-op persistence calls are included. No owner registry,
value/key/identity labels, exporter, tracing or complete operational telemetry is added.


Internal `Diagnostics` captures immutable local consensus positions, charged map counts/bytes and
limits, leader-observed replica acknowledgement/lag, cached successful storage publication state,
and historical successful actor read-barrier index/term. Reading the snapshot performs no actor,
network or filesystem work. Collections are bounded and read-only; publication timestamps reveal
cached observations during blocked work. Stop preserves final metadata and clears readiness/replica
observations. A local-ready hint and an old read barrier do not verify a currently reachable quorum.
Replica lag reflects historical validated acknowledgements, not fresh liveness or peer application.

Publication-driven gauges use bounded member/peer ordinals 0–2 and map ordinals 0–15. Equal ordinals
from multiple groups in one process collide; use per-runtime snapshots for unambiguous observations
or isolate group export resources. No group GUID, arbitrary map name, value/key label, callback
registry or automatic health integration is added. Expired entries remain charged until reclamation;
storage sizes describe the last successful publication, not current physical files after failure.


Definite refusal before actor enqueue returns `CapacityExceeded` (full shared/principal item/byte budget) or
`QuorumUnavailable` (stopped), with admission false. This applies to reads and side-effecting
session/mutation requests. Work already queued or possibly submitted retains conservative
uncertainty, and the SDK still accumulates uncertainty across attempts. No new wire code or
retry identity is introduced.

The actor owns final queue/cancellation-source disposal independently of `DisposeAsync`'s bounded
wait. Concurrent disposals join one cached ten-second wait; timeout does not release the store or
mean cleanup finished. Internal `Completion` observes actual actor/transport/storage/synchronization
cleanup after blocked work returns. A cached disposal timeout stays the result of that wait;
it does not prevent eventual cleanup. Stopped/faulted owners can be disposed after completion
without touching a disposed cancellation source. This does not interrupt arbitrary OS I/O or
establish a hard wall-clock shutdown guarantee.
