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
reservations on shutdown/fault. Portable tests cover concurrent producers, byte saturation and
control/client scheduling; live tests fill the client lane during blocked persistence and verify
fault/shutdown cleanup. These limits do not prove production memory or fair scheduling among
external principals.

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
