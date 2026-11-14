# HostLoom.DataGrid.Client

Experimental, non-packable unary client for the native data-grid prototype. It depends on
transport and the dependency-free grid contracts. The server/actor composition is still internal;
there is no supported deployment host, cache adapter or production readiness claim.

`DataGridClientOptions` binds three configured members to client-listener ports, exact DNS SANs,
SHA-256 leaf pins and a private trust root. The certificate must hold a private key and be bound
to a principal by the server. Construction captures owned certificate handles and a public-only
root copy; input certificates may be disposed once construction returns.
Mutual TLS 1.3 and the client-specific ALPN precede version/configuration hello and each request.
The live server path is tested on Linux; there is no plaintext or TLS-version fallback.

`ReloadCredentials(options)` validates and atomically publishes replacement client material,
server pins and trust. Principal, cluster/configuration, build and routes must stay unchanged.
Invalid replacements leave current calls and credentials intact. Publication closes old exchanges
and cancels their calls; an uncertain mutation retains `MayHaveBeenAdmitted` and its original
logical identity. Retry the original request after reload. Await the returned `Retired` task to
join old users and release their material. Another reload fails while that generation drains;
current calls remain bounded to eight. Inputs may be disposed after publication returns. Expiry
is checked at admission and by a one-second connection-retirement timer. There is no automatic
file watcher, identity migration or cluster-wide cutover coordinator.

Construct one immutable `DataGridClientRequest` per logical operation. The caller opens a session
with a fresh nonce and an explicit not-after time within five seconds of grid time, then supplies
strictly increasing mutation/close request numbers within that session. Serialize use of each
session; the SDK does not assign sequences, renew sessions or choose a replacement identity.
A retry reuses the same request, including its session, sequence, mutation bytes, expiration and
expected version. Only the physical attempt UUID changes. Conditional rejection is a completed
mutation vector. Batch reads capture values, versions and remaining TTLs at one committed time;
a vector that exceeds frame capacity fails as a whole.

`ExecuteAsync` admits at most eight concurrent calls, tries at most three physical exchanges
and two redirects within five seconds, and permits two connection starts per second with burst
two per client instance. Reconnect delay includes bounded jitter (50–500 ms); server hints may
increase the delay within that bound. Routes and redirects stay within the configured members.
Confirmed post-admission capacity failures are terminal. Pre-admission capacity and availability
failures may retry. There is no alternate backend or local successful read fallback.

Inspect `DataGridClientOutcome.MayHaveBeenAdmitted`, which accumulates across attempts. A later
pre-admission denial cannot erase an earlier uncertain submission. `Attempts` records every
exchange's UUID, member and whether request writing began. Each attempt also retains its validated
wire `Reply`; null means no validated response was observed and is distinct from a synthesized
caller failure. With a shared history recorder, `InvocationOrder`/`CompletionOrder` locate each
attempt within its logical call. These are event counters, not timestamps or server commit times. Timeout/cancellation after possible
write admission returns OutcomeUnknown; reads return unavailable. Entry cancellation throws
before admission. Authentication/protocol failures are terminal and retain cumulative uncertainty.
`Disable` stops further attempts; disposal joins the bounded active calls and is idempotent.
Neither operation deletes shared maps or cancels an already committed mutation.

Optional `DataGridClientHistory` retains payload-bearing boundary observations, bounded to
8192 operations / 64 MiB. Treat it as sensitive experiment data, not automatic telemetry.
Exhaustion throws and makes the experiment inconclusive. A shared recorder orders invocation
and completion across client instances without comparing physical clocks. The independent
`DataGridClientHistoryChecker` searches at most 10000 states by default (maximum 100000),
32 operations and two MiB of payload. It starts with empty maps and pre-opened live sessions
beginning at request one. Successful reads/mutations and retained duplicate results constrain the
search. Pending/uncertain writes may apply or be omitted; their uncertain response does not bound
server completion. Ordered symbolic versions are bound by later reads, CAS expectations and
retained results, with mismatch exclusions and no delete/recreate version reuse. A Valid result
means that an allowed completion exists, not that an uncertain write definitely executed.

Never, absolute and relative TTLs are modeled. Symbolic committed times relate relative deadlines
to exact captured TTLs; the checker does not guess mutation timestamps. Reads constrain monotonic
grid time, expiry and session retirement. Lost reads and unobserved internal time advances may
also retire tombstones. Retained replay preserves the original deadline.

The default mode assumes pre-opened live sessions starting at request one. For a complete history
beginning with an empty session allocator, use:

```csharp
var result = DataGridClientHistoryChecker.Check(
    history.GetOperations(), startsWithEmptySessions: true);
```

This mode checks nonce/deadline agreement and open windows, allocation high-water, principal
ownership, sequence retirement/mismatch/gaps, close, session expiry and delayed reopen rejection.
For a semantic failure after multiple physical attempts, complete recorder evidence expands that
call into separately ordered attempts using the same immutable logical request. A submitted attempt
with no observed reply remains uncertain and may execute or be omitted; a confirmed pre-admission
refusal has no map effect. The final semantic response is checked independently of earlier
uncertainty. Missing/inconsistent orders, reused identities, a reply before submission or an
impossible retry after success remain Inconclusive. Older histories without attempt evidence
retain that result. Expanded attempts count toward the same 32-operation/two-MiB search bounds;
the witness identifies their attempt UUIDs alongside unexpanded operation UUIDs. No attempt count,
search deadline or recorder limit is raised. Quotas, map policies, invalid-item vectors, arbitrary
initial snapshots and exhausted search remain outside the model. Search includes item and
time-constraint branches, with at most 65 time variables for 32 operations. Passing a bounded
history does not establish the actual fate of an uncertain call or prove consensus correctness.

Opt-in [member-process tests](../../tests/HostLoom.DataGrid.ProcessProbe/README.md) exercise SDK
histories during leader kill, committed reply loss, minority outage, graceful and abrupt full-group
restart, interrupted follower snapshot publication and a healed peer partition. Snapshot cases
verify continued majority progress and retained replay after follower rejoin; abrupt group restart
resolves an unknown CAS using the original identity. Stale SDK reads are rejected as negative controls.
Three independent member processes retain end-to-end TLS through six
bounded test-only directed proxies. Additional cases cover relative TTL/replay, close and session
expiry with injected UTC time. Two directed overlapping CAS/conditional-delete histories also
remove and recreate the entry while the first committed reply is held. Dropping that reply
exercises automatic SDK retries; original successful/rejected results survive subsequent leader
failover, and stale reads or changed duplicate results are rejected as negative controls. These
cases use separate sequential sessions and keep only their latest request identities live.
Additional Linux actor/TLS histories hold all competing requests at the receiver before actor
admission, then release CAS/conditional-delete races and read/Put/PutIfAbsent races across
removal/recreation. They check retained results and reject stale reads/changed duplicates. Two
lost-reply cases exercise automatic retries ending in SessionExpired or RequestIdentityMismatch,
using observed attempt evidence and rejecting altered identity/failure controls. These are directed
local schedules, not exhaustive concurrency or independent-host qualification.
Remaining work includes broader partition/fault schedules, coordinated rolling rotation, full resource/telemetry
qualification, supported filesystem/device and shutdown evidence, workload targets, and any
claimed Native AOT path.


`DataGridClientDiagnostics.MeterName` (`HostLoom.DataGrid.Client`) publishes logical call counts,
active invocations, whole-call duration and entered physical attempts. Fixed operation/outcome
and Boolean admission/submission tags preserve the difference between uncertain caller outcomes
and committed/retained results. Counts include immediate capacity/disabled decisions; the active
invocation count includes those brief decisions outside the eight-exchange reservation. An entered
attempt can be canceled before it connects or writes. Entry cancellation precedes metric admission.
Collect before client construction for a complete up-down baseline. No key/value/map/endpoint/
principal/exception labels, payload history, exporter or grid trace propagation is added.


The `hostloom.datagrid.client.timeouts` counter distinguishes SDK deadline termination from
caller cancellation, disable and credential retirement. Its Boolean admission tag remains
cumulative across attempts. Runtime deadline counters describe a different boundary and must
not be added to SDK counts as unique requests. Timeout does not prove mutation rollback.
The [signal inventory](../../docs/reference/observability.md#experimental-native-grid-instruments)
records supported outcomes, capacity/recovery observations and unqualified collector behavior.
