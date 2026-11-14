# Experimental data-grid resource limits

These are enforced prototype ceilings and accounting units, not measured service capacity or
accepted memory, latency or recovery budgets. The non-packable prototype has three fixed voters,
one replicated group and majority two. Every replica holds the group's full application state.
There is no supported deployment host or process-wide resource manager.

Use this inventory when composing the kernel, storage, transport, runtime and SDK. Its scopes
matter: creating another listener, runtime, security context, SDK or experiment recorder creates
another corresponding budget. Existing package READMEs describe the detailed contracts.

## Owners, limits and release boundaries

| Resource / owner and scope | Effective limits | Refusal and actual release | Concrete checks |
| --- | --- | --- | --- |
| Bootstrap configuration / one group and one access policy | Three members, up to sixteen maps; one local access policy has 1–128 principals, at most sixteen map grants each. Denylist input is at most 4096 hash records, counting duplicates. | Invalid or duplicate declarations refuse before runtime use. Configuration metadata is immutable; changing quotas changes the configuration digest. These are configuration bounds, not live-object memory estimates. | [Configuration](../../tests/HostLoom.Tests/DataGridP0CompletionTests.cs), [credential bounds](../../tests/HostLoom.Tests/DataGridCredentialTests.cs), [client codec](../../tests/HostLoom.Tests/DataGridClientCodecTests.cs). |
| Socket reservations / one peer transport plus one client listener | Peer connection gate sixteen, shared by inbound and outbound; client connection gate 112 inbound. Their nominal combined gate is 128. Backlog arguments sixteen/112 are OS listen requests, not additional managed reservations. | Capacity refuses without creating an unbounded session-task queue. Socket/session finally paths release gates. Outgoing peer tracking is separately bounded below and overlaps socket work. | [Transport budgets](../../tests/HostLoom.Tests/DataGridTransportDiagnosticsTests.cs), [live admission](../../tests/HostLoom.Tests/DataGridTransportAdmissionTlsTests.cs), [churn](../../tests/HostLoom.Tests/DataGridTransportChurnTlsTests.cs). |
| Handshakes and starts / same composition | Peer four handshakes, 24 starts/s with burst 24 shared across directions; client twelve handshakes, eight starts/s with burst eight inbound. Nominal combined gates/rates are sixteen/32, not process-global pools. TLS/hello has a five-second deadline. | Rate/capacity refusals close the socket; successful validation releases handshake reservation before dispatch. Actual session ownership can outlive cancellation until its code returns. | Transport budget burst/refill tests use a controlled clock; live stalled-handshake/deadline/stop and repeated-disconnect tests cover release. |
| Authenticated identity reservations / each transport or client listener | Two peer connections per remote member, shared across directions; only two other members exist. Eight active client connections per principal; principal dictionary cap 128, tightened by the listener's 112 connection gate. | New certificate generations do not reset principal identity accounting. These connection reservations are distinct from actor operation charges. | Live member-ceiling tests; principal idempotent-release and credential/churn tests. |
| Client request buffers / one client listener | Twelve concurrent request reservations, each charging 2 MiB for bounded request/response work: 24 MiB encoded reservation total. Unary client sessions have a five-second lifetime. | Slow bodies/readers and retained receivers keep reservations until actual handler return. Timeout or disconnect does not terminate remote actor work. TLS/OS buffers, copies and heap overhead are excluded from the charge. | [Slow body/reader and wire capacity](../../tests/HostLoom.Tests/DataGridOverloadTlsTests.cs), transport admission and churn tests. |
| Actor queues / one runtime | Control 256 items/16 MiB; client 128 items/16 MiB. Each authenticated principal queues at most eight items/4 MiB. At most eight control turns precede an available client turn; principals rotate FIFO queues. | Queue charge releases on dequeue/drain, including canceled queued work only when skipped. Queue item/byte/principal/stopped refusal is definite. This is scheduling fairness, not CPU or end-to-end latency fairness. | [Queue tests](../../tests/HostLoom.Tests/DataGridDiagnosticsTests.cs), [fair scheduling](../../tests/HostLoom.Tests/DataGridFairAdmissionTests.cs), [wire overload](../../tests/HostLoom.Tests/DataGridOverloadTlsTests.cs). |
| Retained operations / one runtime | Shared client 128 operations/16 MiB, eight/4 MiB per principal; trusted local calls share the global pool. Control has its own 256-item/16-MiB pool. Encoded submission charge includes keys/canonical command plus 128 bytes. | Reservations cover queue, execution and pending consensus; they release once on definite refusal, canceled dequeue, terminal completion or actual owner drain. Caller cancellation or five-second waiting deadline cannot refund owned work. One executing control item leaves room for 255 waiting controls. | [Operation budget](../../tests/HostLoom.Tests/DataGridOperationBudgetTests.cs), [live pending/timeout](../../tests/HostLoom.Tests/DataGridOperationBudgetTlsTests.cs), [control budget](../../tests/HostLoom.Tests/DataGridControlBudgetTests.cs), [control lifetime](../../tests/HostLoom.Tests/DataGridControlLifetimeTlsTests.cs). |
| Consensus waits, senders, timers and watchers / one runtime | At most 128 pending local/client waits; up to 256 active timers; 64 diagnostic waits shared between leader/applied watchers. Each of two peer sender workers has one active and one latest pending request. Peer transport tracks at most sixteen outgoing invocations. | Latest pending payload replaces the previous one instead of extending a task chain. Correlation/term checks discard stale replies. Cancellation/stop drains watchers and timers, and disposal joins sender/outgoing tasks before final credential/gate disposal. Invocation tracking can remain held after a socket gate releases. | [Watcher lifetime](../../tests/HostLoom.Tests/DataGridWatcherLifetimeTlsTests.cs), control lifetime, [peer progress](../../tests/HostLoom.Tests/DataGridProgressTests.cs), [live catch-up](../../tests/HostLoom.Tests/DataGridProgressTlsTests.cs). |
| Credential snapshots / one security context or SDK | One current and at most one retiring generation. Shared peer/client security allows 128 captured users across both generations; the SDK store allows eight and shares its eight-call scope. Separate security contexts have separate caps. | Reload closes old exchanges but retains material until every old user releases. A second reload refuses while retirement is pending. There is no premature release after a transport gate, caller or disposal wait ends. | [Credential ownership/rotation](../../tests/HostLoom.Tests/DataGridCredentialTests.cs), [live rotation](../../tests/HostLoom.Tests/DataGridCredentialTlsTests.cs), [128-capture cross-generation saturation](../../tests/HostLoom.Tests/DataGridResourceBoundaryTests.cs). |
| Map data / one replicated group, with tighter map quotas | Up to 100000 charged keys and 256 MiB encoded key/value bytes across maps. Each map declares lower/equal key and byte ceilings. Keys 1–1024 bytes; values up to 64 KiB; batch/read vectors up to 128 items, also constrained by encoded frame size. | Item quota refusal does not modify data/version/charge; it is a completed retained result. Replacement accounts for its size delta, so replacing an existing key need not consume another key. Expired physical entries stay charged until deterministic reclaim or overwrite; live entries are not evicted. | [Local model](../../tests/HostLoom.Tests/DataGridLocalStateMachineTests.cs), [live key/byte quota and recovery](../../tests/HostLoom.Tests/DataGridResourceBoundaryTlsTests.cs), [TTL quota release](../../tests/HostLoom.Tests/DataGridOperationalTelemetryTlsTests.cs), existing batch-read response refusal. |
| Sessions and latest mutation results / one replicated group | 4096 retained sessions total, 64 per principal; maximum ten-minute logical lifetime. One latest result per session; result charge ceiling 64 MiB. Reachable maximum charge is 16261120 bytes, derived below. | Close retains the tombstone until its cutoff; close is not immediate capacity release. A time-advance command retires at most 128 due sessions. Old requests cannot allocate a new identity implicitly; repeated retained results never execute again. | [Live principal/group pressure](../../tests/HostLoom.Tests/DataGridSessionPressureTlsTests.cs), [maximum vector wire/snapshot/replay](../../tests/HostLoom.Tests/DataGridResourceBoundaryTests.cs), session/temporal model tests. |
| Raft retained log / each replica of the group | Hard 4096 entries/16 MiB encoded commands. Client admission stops at 3968 entries or 16 MiB minus one maximum committed command. Internal maintenance retains headroom and respects the 128-uncommitted-entry envelope. | Runtime attempts committed-prefix compaction at 1024 retained entries or 4 MiB command bytes, preserving suffix/term/vote. A full client admission budget refuses; missing quorum cannot be bypassed. | [Reserved log admission and compaction refusal](../../tests/HostLoom.Tests/DataGridMaintenanceTests.cs), Raft invariants, live automatic maintenance and session pressure. |
| Snapshots and peer frames / each peer transport/store | One incoming image per transport; shared 512-MiB snapshot-format envelope. Transfer uses 64-KiB chunks with a sixty-second window after validated hello. Ordinary frames are at most 1 MiB including the 72-byte header; WAL records are at most 2 MiB including framing/digest. | Invalid lengths fail before declared payload allocation. A 128-item batch of 64-KiB values cannot fit the frame just because its item count is legal. Local snapshot refusal is explicit; failed or canceled transfers release only after actual owner return. The format envelope is not supported physical-memory capacity. | [Frame/storage formats](../../tests/HostLoom.Tests/DataGridStorageFormatTests.cs), peer codecs/admission, [snapshot above 8 MiB and interrupted transfer](../../tests/HostLoom.Tests/DataGridMaintenanceTlsTests.cs), compaction TooLarge checks. |
| Files and reservations / each exclusively owned store directory | WAL default 1 GiB, directory default 4 GiB and hard 4096 files, free-space reserve default 1 GiB. Internal experiment limits can tighten byte ceilings/reserve. Old and temporary files remain charged until deletion; generation publication requires overlapping old/new space. | Failed reservation preserves the selected root; uncertain writes fence the owner. Runtime collection validates/seals CURRENT, deletes at most 32 recognized obsolete files, then flushes the directory. A full garbage batch defers compaction; startup does not salvage an already-invalid inventory. | [Lowered WAL/directory/free reserve, publication, ownership and killed collector](../../tests/HostLoom.Tests/DataGridFileStorageTests.cs), runtime budget refusal, automatic maintenance. These are not actual ENOSPC/device tests. |
| SDK calls, attempts and reconnect / each SDK instance | Eight admitted calls; at most three physical attempts and two followed redirects per call; one five-second deadline across reconnect, TLS and retries. Connection starts use a shared two/s bucket with burst two; jitter is 50–500 ms and bounded server hints stay within that range. | The ninth call refuses before an attempt. A call waiting for a reconnect token still holds its SDK reservation. Every attempt retains the same immutable logical request; only the attempt UUID changes. A later refusal cannot erase earlier uncertain submission. Disable/disposal cancels and joins calls before releasing material. | [Eight-call retry pressure, ninth refusal, redirect cycle and unchanged bytes](../../tests/HostLoom.Tests/DataGridResourceBoundaryTlsTests.cs), [lost replies/disable](../../tests/HostLoom.Tests/DataGridClientTlsTests.cs), [deadline versus cancellation](../../tests/HostLoom.Tests/DataGridOperationalTelemetryTlsTests.cs). |
| Experiment recorder/checker / each explicitly supplied recorder or check | Recorder 8192 operations/64 MiB charged request/response/attempt bytes; checker 32 normalized operations/2 MiB payload and 10000 states by default, maximum 100000. Expanded physical attempts consume the same operation/search bounds. | Exhaustion is an exception/pending observation or Inconclusive, never silent truncation or Valid. Recorder data lasts while the application retains it; there is no automatic pruning. These charges do not bound every managed search allocation or elapsed scheduling time. | [Recorder operation/response boundaries](../../tests/HostLoom.Tests/DataGridResourceBoundaryTests.cs), [checker budget/unknown tests](../../tests/HostLoom.Tests/DataGridClientHistoryTests.cs), [normalized attempts](../../tests/HostLoom.Tests/DataGridAttemptHistoryTests.cs). |
| Startup and shutdown / each runtime and its transferred resources | Failed startup owns rollback. Runtime DisposeAsync has one cached ten-second waiting bound; internal Completion follows actual owner cleanup. SDK and transport/listener disposal join their owned work without an equivalent hard cleanup-time guarantee. | Timeout does not release the directory or dispose in-use synchronization/credential material. Blocked synchronous I/O or an uncooperative receiver can outlive the waiting bound. Shared external security remains caller-owned. Stopped client listeners cannot restart; bind failure before stop can retry. | [Startup rollback](../../tests/HostLoom.Tests/DataGridStartupLifetimeTlsTests.cs), [listener lifetime](../../tests/HostLoom.Tests/DataGridClientListenerLifetimeTlsTests.cs), [timeout retains ownership](../../tests/HostLoom.Tests/DataGridOverloadTlsTests.cs), control lifetime. |
| Subscriptions, invalidation queues and watermarks | No owner or implementation in this prototype. | Deferred to cache/subscription integration; no fabricated zero-size saturation or healthy telemetry. | These require their own admission, gap/flush and watermark checks when implemented. |

The nominal transport sum of 128 connections, sixteen handshakes and 32 starts/s belongs to one
peer/client composition. The peer gates are shared across both directions, and authenticated
peer/principal/credential limits can be tighter than those sums. Sixteen outgoing invocation
records are not sixteen extra socket reservations. Additional SDK instances do not multiply
server, principal or group quotas, but they do have separate SDK buckets.

A socket/request reservation and an actor reservation can describe the same operation at different
or overlapping stages. Queue bytes are a subset while work waits; retained operations include
execution/pending work. Consequently, 24 MiB transport + 16 MiB client + 16 MiB control is not a
formula for maximum heap. TLS, arrays/copies, replay/application state, credentials, kernel buffers
and runtime/test infrastructure have other costs. The supported memory signal is process-wide;
capacity and peak-memory qualification still require actual workload/environment measurements.

## Retained-result and snapshot bounds

Each retained mutation item has a two-byte status, one-byte version marker and 28 version bytes.
The vector has a two-byte count, so the largest reachable result charge is:

```text
2 + 31 × 128 = 3970 bytes per retained vector
4096 × 3970 = 16261120 bytes across all retained sessions
16261120 < 64 × 1024 × 1024
```

Rejections also reserve fixed version fields, so they cannot exceed this bound. This makes the
64-MiB result ceiling unreachable through current bounded commands; an overflow experiment for
that ceiling would require an impossible state. The maximum-vector test instead executes 128
successful items, checks actual encoded reply length and ordinal versions, restores the snapshot,
replays the original result and rejects changed identity without changing data or charge. Session
record metadata and runtime object overhead are not included in this charge.

Selected data/session quotas imply a conservative 291016890-byte maximum encoded v1 snapshot,
within its existing 512-MiB format envelope. This structural bound includes entry metadata and
retained vectors. It does not qualify a full 256-MiB dataset, heap amplification, a sixty-second
maximum-size transfer, recovery latency, mixed builds or downgrade behavior. Members must use
matching builds; those deployment measurements remain separate.

## What the directed checks establish

The new TLS quota checks select either a one-key map or a byte ceiling exactly filled by the
first value. A refused second key leaves the original value/version/charge intact; replacing the
existing key still works. Compaction/restart preserves the refusal. Deleting the original frees
space, but replaying that retained refusal still cannot insert data; a new sequence can use the
released capacity. These are declared quota boundaries, not heap-pressure experiments.

Retry pressure uses eight independent sessions on one SDK. A receiver gate makes the ninth-call
refusal deterministic, then repeated pre-admission capacity responses exercise attempts and the
shared reconnect bucket. The test checks immutable bytes, entered versus submitted attempts,
SDK deadline emission, the cumulative observed dispatch allowance and zero mutation. Refusal stays
armed until client servers actually drain, so late submitted requests cannot accidentally reach
the actor. A redirect cycle separately stops after two followed redirects/three attempts. Exact
retry intervals are jittered; observed elapsed times are test observations, not latency promises.

The recorder boundary checks reject a 8193rd operation without dropping earlier records and
refuse the next large response while retaining its pending operation. A wire-valid immutable
read reply is shared in that test to exercise charge arithmetic without allocating 64 MiB of
identical values. The capture test holds 128 credential users across current/retiring material,
refuses overflow and another retirement, and proves idempotent release permits reuse.

Existing deterministic, actor/TLS and independent local-process tests remain distinct evidence.
Checks cover declared-budget refusal and controlled process/interruption boundaries. They do not
establish actual disk-full handling, independent-host isolation, device power-loss behavior,
Native AOT, physical TTL accuracy, collector delivery or numerical release NFRs. A listener,
filesystem or injected receiver that cannot return still owns its resources until actual cleanup.
