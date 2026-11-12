# Native data-grid member process probe

Internal, non-packable child-process harness for `DataGridProcessTests`. It composes the existing
file-backed actor and mutual-TLS peer/client listeners in one independently killable process.
It opens an explicitly bootstrapped member directory; it never creates a new cluster on startup.
The peer runtime and client listener share one owned security context and captured credentials.
The parent controls do not expose live credential reload; that behavior is checked separately
in live-actor TLS tests. This is test infrastructure, with no deployment/configuration
compatibility or production claim.

Run the owning opt-in tests on Linux from the repository root:

```sh
HOSTLOOM_DATA_GRID_PROCESSES=1 dotnet test \
  --project tests/HostLoom.Tests/HostLoom.Tests.csproj --configuration Release \
  -- --filter-class '*DataGridProcessTests'
```

The tests create three members with separate directories and ephemeral loopback ports, private
short-lived test credentials under an owner-only temporary directory, and monotonically increasing
process epochs on restart. They pass all paths/routes explicitly. They never select an existing
service, shared credentials, production endpoint, or alternate backend. The parent owns startup,
SIGKILL, graceful stop, restart, stdout/stderr draining, reaping and temporary-file cleanup.
Each test has a 75-second work deadline, 90-second test timeout and ten-second child cleanup waits;
the child control loop has an independent three-minute deadline. Failed startup aborts the experiment.

Only trusted parent stdin accepts status, fixed UTC clock, arm, lose, compact and stop controls. Output contains
bounded control/status records, never values or mutation results. Arm pauses one completed mutation
after durable actor application and before the TLS reply; the parent then disables the SDK and
kills that exact member. Alternatively, lose releases the one held reply by throwing an expected
I/O error before transmission, closing that exchange and exercising the SDK's automatic retry.
The gate is bounded to one pending reply and clears after completion/cancellation. These controls
are absent from production runtime/transport paths. Clock
injection changes only the proposed grid UTC time; timers and certificate validity use real time.
An optional startup argument pauses one snapshot publication at an existing storage observation
seam, before the parent kills that exact follower. It accepts only snapshot flush, generation
directory flush or anchor directory flush. Compact asks the actor to publish its current snapshot;
status includes the snapshot index. No path or payload is accepted through these controls.
Status is observed on a bounded polling timer; recovery is accepted through SDK results/history,
not merely an internal leader diagnostic. Any runtime/listener fault aborts status checks.

The thirteen cases cover acknowledged data and retained identity after leader kill/full-group
restart, unknown outcome after commit-before-reply kill, minority unavailability/recovery,
absolute expiry after failover, a healed peer partition, relative TTL/replay with close, and
session expiry without allocator number reuse. Three cases kill a catching-up follower at snapshot
publication barriers, inspect the selected snapshot offline, then restart it and verify all-member
application and SDK replay/read results. The healthy majority keeps serving while publication is
held. Before rejoining the stale follower, both healthy members compact and gracefully restart
from their original stores to discard captured pre-compaction append payloads and require a snapshot.
That setup restart is outside the majority-progress check. Another case abruptly kills all members
after a committed CAS loses its reply and all members have applied it; original directories and
bootstrap identities recover the retained result. Earlier
full-group restart coverage uses graceful stop. Each new history accepts the observed recovery and
rejects a substituted stale SDK read. Internal positions coordinate the test and are excluded from
the history checker. TTL movement is explicitly injected, not a measurement of wall-clock
accuracy. All members share one container, host and filesystem; these tests do not establish
independent-host redundancy, power-loss survival, arbitrary packet loss/reordering or production NFRs.

Two cases hold a CAS or conditional-delete reply while another session executes the opposing
conditional operation and a third removes/recreates the entry with PutIfAbsent. SDK operation
intervals overlap, although the first mutation has already committed at the pause. After explicit
reply loss the SDK retries with distinct physical attempts and returns the original logical result.
A subsequent leader kill preserves both that result and the competing conditional rejection;
the recreated value remains. Each history rejects stale-read and changed-duplicate-result negative
controls. Separate sessions retain their latest request; this does not promise retention of every
earlier request. This directed pair covers both orders, not arbitrary concurrent schedules.

Peers bind private loopback backend addresses `127.0.0.2`–`127.0.0.4`. The parent owns six directed
TCP links on source-specific `127.0.0.10`–`127.0.0.12` addresses and configured destination ports.
They forward encrypted bytes without terminating or parsing TLS. Isolating a member closes/refuses
its four incident links; the healthy pair remains connected and client listeners remain reachable.
Healing reenables links, then SDK reads/retries and all-member applied-position checks verify
recovery. No machine-wide firewall, DNS change, certificate relaxation or product fault hook is
used. Each link has two sessions, two 16-KiB copy buffers per session and a 60-second lifetime;
completed tasks are removed before reservations are released, and shutdown joins both pumps.
