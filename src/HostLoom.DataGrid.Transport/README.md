# HostLoom.DataGrid.Transport

Experimental, non-packable TLS peer and client transport. It references only the grid kernel. The separate
[runtime](../HostLoom.DataGrid.Runtime/README.md) owns actors and storage. Listeners remain internal; public immutable client grant policies and the separate
[client SDK](../HostLoom.DataGrid.Client/README.md) now exist. There is no deployment API.

The prototype requires Linux and TLS 1.3, mutual authentication, private custom trust, valid
chains/expiry/EKU, exact DNS SANs, and SHA-256 leaf-certificate bindings to three configured
members. Trusted certificates grant no membership without an explicit binding. Wildcard/common-
name fallback is disabled. A local denylist and expiry apply; there is no implicit online CRL
dependency. Construction captures owned certificate handles and public-only root copies; the
caller can then dispose its inputs. One internal security owner publishes peer credentials,
client trust, pins, denylists and map/session grants together for a fixed member/configuration/build.
Invalid replacements leave the active generation intact. Publication closes all old-generation
sockets and cancels their operations. Each request checks its captured generation immediately
before actor admission; an operation already admitted can still commit. The returned retirement
task joins old users and cancellation callbacks before disposing their material. At most one
generation may retire alongside the current generation; another reload fails while it drains.
Local/root/remote certificate expiry also closes connections, with a one-second polling interval
and admission-time checks. Per-principal connection accounting survives policy replacement. There is no plaintext or TLS 1.2 fallback. macOS server execution is rejected because
the required .NET TLS 1.3 server path is unavailable there.

Every connection carries one RPC. A validated MemberHello exchanges exact protocol/command/
snapshot/WAL versions, build and configuration hashes before peer dispatch. The owner supplies
the build digest; deriving it from deployed artifacts remains a hosting responsibility. The 72-byte v1
envelope binds incarnation and sender to the authenticated member, with zero session fields and
an echoed physical attempt. Vote/append codecs validate directions, boolean/count/term/index
rules, canonical command wrappers and exact body consumption. The stream reader rejects a bad
four-byte length before allocating the bounded frame. Client/admin/read-barrier opcodes are not
implemented by this peer listener. Reads in the runtime use committed log barriers.

Snapshots use Start, sequential chunks of at most 64 KiB, and correlated progress replies.
One transfer per receiver reserves at most the current 8 MiB prototype image limit for 60 seconds.
Duplicate chunks compare already received bytes; gaps, changed retries, foreign IDs and overflow
reject. Authenticated progress observes/persists the leader term and refreshes its election timer
before installation. Complete length/hash/application validation precedes actor dispatch.
New-image completion waits for the actor's durable generation publication. An already committed
prefix can acknowledge durable coverage without replacing newer state or publishing an older
image. Thus completion confirms coverage, not that the requested snapshot UUID became CURRENT.

Peer admission allows 16 incoming/outgoing connections, two per member, four concurrent
handshakes and 24 handshake starts/second with burst 24. Client admission reserves 112
connections, 12 handshakes, eight starts/second with burst eight, eight connections per bound
principal and 12 active requests. Together the two listeners allow 128 connections, 16
handshakes and 32 starts/second. Peer/client encoded work reservations split eight/24 MiB;
these raw-byte budgets do not measure managed-memory amplification. TLS/hello and regular calls
have five-second deadlines. Snapshot calls have a 60-second deadline. Each connection processes one request, so it has no unbounded writer queue
or pending-call map. Excess work closes/rejects; retries belong to Raft, preserving correlation.
Disposal stops new outgoing calls and joins registered outgoing invocations plus accepted sessions
before disposing credential material or semaphore gates. This also applies with shared credentials;
late sends throw a stopped IOException. The ownership tracker is capped at 16 outgoing calls;
its capacity refusal uses the existing outbound connection/capacity reason. Gate reservation
counters still describe gate leases, not retiring invocation tasks. The runtime drains its sender workers before releasing
the transport. Local send completion or cancellation does not prove remote rollback.

The separate client ALPN accepts ClientHello and unary mutation/read/session messages only.
Client certificates require private-root chain/client EKU/expiry validation and explicit leaf-pin
principal binding. Immutable per-map read/mutation grants and a separate session grant default
to deny and run before actor admission. Up to 128 principals, 16 grants each and 4096 denied
fingerprints are configured explicitly. The envelope sender must equal the TLS-bound principal;
that principal supplies session ownership to the actor. Successful client responses bind the
member, logical session/request and physical attempt. Unknown directions, counts, flags,
versions, trailing bytes and unsupported hellos reject before dispatch. Client builds may differ;
protocol/command/configuration must match. Client request/handshake deadlines are five seconds.

Linux container tests exercise certificate/build/member rejection, TLS-version refusal,
malformed authenticated frames, encrypted-record tampering, handshake saturation and snapshot
completion, live credential replacement, failed-replacement isolation, revocation and expiry.
Root/leaf/pin replacement is tested across three live actors and the SDK. Publication is atomic
per owner; there is no cluster-wide atomic cutover, dual-root overlap, file watcher or zero-outage
rolling-rotation guarantee. Broader production abuse/resource isolation, deployment, performance
and Native AOT qualification remain open. The SDK and separate process fixture provide bounded
history and failure evidence; they do not establish full production readiness.


`DataGridTransportDiagnostics.MeterName` (`HostLoom.DataGrid.Transport`) publishes fixed-label
reservation deltas, reserved encoded-work/image bytes, explicit admission refusals and terminal
exchange outcomes. Labels are role, direction, resource and fixed reason/outcome vocabulary;
member/principal counters aggregate without identity labels. One socket can hold multiple kinds
of reservation. Client requests reserve two MiB of encoded work; inbound snapshots reserve their
descriptor length. Neither value measures managed heap usage. Collect before constructing owners
to observe a complete up-down baseline; no instance registry or payload history is retained.

A valid failure reply can complete a transport exchange; completion does not assert application
success. Admission refusals and terminal exchange outcomes are separate counters, and can describe
the same failed exchange. Deadline, shutdown and linked credential cancellation share `canceled`;
explicit retired-generation exceptions use `retired`. IO outcomes do not infer remote cause or
mutation fate. The SDK records its own calls/attempts in its separate meter. Listeners are trusted
synchronous diagnostic code and must not throw or block.

Portable checks cover every semaphore budget, concurrent/idempotent release, anonymous principal
limits and exact monotonic handshake burst/refill. Directed Linux tests cover stalled handshake
capacity/deadline/stop, bound-member overflow and recovery, unauthorized/unbound clients, malformed
requests/lengths and snapshot disconnect/reuse without actor installation. These checks preserve
existing TLS/hello/admission rules and do not complete the broader production abuse/resource gate.


Additional controlled Linux cases fill all 12 request reservations with two authorized principals
that send hello but withhold request bodies. The next request reservation is refused, peer
control on the same security owner remains usable, and deadline/stop releases the 24-MiB encoded
work reservation. After expiry a normal request can dispatch again. A separate non-reading client
receives a near-limit response while later clients remain usable and managed reservations drain.
That write can complete into OS buffers or terminate by deadline; these tests do not force every
backpressure path or measure kernel socket memory, long-running churn or principal fairness.

Directed Linux contention checks now repeat 16 disconnects at each of four stages: raw TCP,
authenticated TLS before hello, validated hello before a request, and an incomplete frame body.
Every cycle returns connection/handshake/principal/request reservations to baseline without
application dispatch; peer vote exchanges remain usable between cycles and a final client read
completes. Separate cases fill all 12 client handshake reservations, reject the next attempt,
preserve peer progress and release on the physical deadline or concurrent disposal.

A credential-reload case holds eight dispatched requests for one principal in a controlled
receiver after cancellation. Replacement closes old sockets and cancels those calls, while the
principal quota stays charged until the receiver actually returns. A replacement certificate for
the same principal cannot bypass the quota; another principal and peer traffic remain usable.
Retirement waits for release, a second reload is refused while it is pending, and the replacement
identity can read after drain. This deliberately retained test receiver is not a durable actor
or a supported extension API. These cases establish directed cleanup and quota behavior, not
sustained-load fairness, full connection saturation, every TCP reset path, device durability or
numerical workload acceptance.

Additional Linux lifetime checks stop outgoing calls during stalled TLS using owned or shared
credentials, including concurrent caller cancellation. Disposal returns after their original
tasks complete and reservations release. A snapshot case stops the local sender while a controlled
remote receiver still owns the complete image; stopping that receiver then releases its transfer.
A real peer deadline can end an inbound session while its blocked actor action remains charged
by the runtime's separate retained-control budget. These checks do not establish a hard bound
for arbitrary OS I/O or an uncooperative receiver.
