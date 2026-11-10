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
Disposal joins accepted sessions; the runtime drains its outgoing workers before disposal.

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
