# Local prototype environment

The selected engineering baseline is **local Linux ARM64 on overlayfs**, with three statically
configured members running as independently stoppable processes inside one owned container.
The user selected this local baseline for prototype qualification. This is a synthetic test
profile, not approval to deploy production data or a supported production installation.

## Selected profile

| Property | Selection and boundary |
| --- | --- |
| Profile ID | `local-linux-arm64-overlayfs-v1` |
| OS / RID | Ubuntu 24.04, `linux-arm64`; the recorded run identifies the patch/kernel |
| Build image | `mcr.microsoft.com/dotnet/sdk@sha256:212fe4cb74627d5adf9817a4088906ae3edff1bd853bb27565d4b46731efe61e` |
| SDK / runtime | SDK 10.0.403, .NET 10.0.14 in that image; project targets .NET 10 / C# 14 |
| Engine | Native ARM64 Linux Docker engine with overlayfs and cgroup v2; Docker Desktop's shared Linux VM is allowed and recorded |
| Filesystem | Container overlayfs at `/tmp` and `/work`; private per-member directories, one exclusive owner each |
| Topology | Static three voters, majority two; one shared container/VM/host and loopback network |
| Fixture controls | Two-CPU quota with shared scheduling, two GiB memory/no swap, no external container network or published ports, all capabilities dropped |
| Time bound | Fifteen-minute container command timeout, bounded test deadlines and owned-container cleanup |
| Data and keys | Synthetic catalog bytes and generated test PKI only; no application data, operator credentials or shared services |

These fixture controls are reproducibility/blast-radius limits. They are not service capacity,
latency, availability, memory or recovery-time targets. Numerical acceptance targets remain unset.
The kernel and engine versions recorded in a report identify that run; a passing report does not
qualify every kernel/driver with the same profile name. Repeat qualification when the tuple changes.

macOS ARM64 remains an auxiliary build/model/local-file-check environment. Its skipped Linux TLS
server and member-process cases are not deployment evidence. Linux x64, Windows, native macOS TLS
deployment, other distributions/filesystems, network/shared filesystems, independent hosts and
container orchestration are outside this selected baseline until explicitly selected and tested.

## Storage and authority

WAL file flush, temporary CURRENT flush, atomic same-filesystem rename and directory flush must
succeed before a persistence effect is acknowledged to Raft. The tested calls operate on the
container's writable overlay layer. They establish process-crash/reopen behavior under that
filesystem interface, not the backing device's power-loss guarantees. See the
[WAL publication contract](../HostLoom.DataGrid.Storage/WAL_PUBLICATION.md).

The three members have distinct directories and stable configured incarnation/member/map IDs.
Normal restart never bootstraps a missing CURRENT or replaces identity. Valid legacy v1 upgrade
is forward-only through a new v2 generation; damaged published prefixes and torn legacy v1 roots
refuse. No downgrade, mixed-build operation, silent repair or operator reseed is qualified.
Directories are private to the trusted fixture account (`umask 077` for test work); this is not
protection against a malicious local administrator or another trusted process racing paths.

The overlay layer survives a child-process restart and is deleted with its owned container. The
runner does not test container/VM/host restart or filesystem persistence after container removal.
The members share every host-level failure domain. Losing two members stops strong operations;
losing the shared host is not a one-member failure. No independent-host isolation, storage-majority
loss recovery, physical device failure, delayed allocation or host/power-loss durability is claimed.
The separate [bounded ENOSPC experiment](../HostLoom.DataGrid.Storage/README.md#reproduce-bounded-disk-exhaustion)
uses owned tmpfs and remains a different filesystem/fault profile.

## Clock and security assumptions

Default runtime UTC and monotonic deadlines come from `TimeProvider.System`. Many TTL/expiry tests
inject logical clocks and explicitly advance them; the process probe controls its own logical
clock. That proves ordering/high-water behavior, not NTP discipline or a physical TTL-error bound.
System UTC is also used to create/validate test certificates. All processes share one host clock;
independent-host skew, external clock service outages and physical expiration accuracy are untested.
Strong reads require fresh quorum authority rather than a clock-based leader lease.

Peer traffic uses mutual TLS 1.3, generated private trust roots, SAN/EKU/member checks, leaf SHA-256
pins, exact build/configuration identity and bounded authenticated frames. Client certificates map
to synthetic principals and explicit per-map Read/Mutate grants. Endpoint DNS names are checked
against certificates but explicitly route to loopback; DNS infrastructure is not qualified.
Credential ownership, atomic local reload, reader drain, expiry/revocation and role/grant refusals
are exercised by the existing matrix. The private CA is fixture-owned, never installed into the
host trust store. PKI issuance/rotation services, fleet-wide revocation distribution, secret-store
integration, production retention/classification and an administration plane are not provided.

## Reproduce qualification

Restore the solution to populate the local NuGet cache, then run from the repository root:

```bash
uv run --locked python -B scripts/test-data-grid-environment.py
# Equivalent shortcut:
just test-data-grid-environment
```

Python tooling uses the repository's locked environment. The runner requires Docker, `dotnet`,
`tar` and the pinned ARM64 image (Docker may fetch it before the isolated container starts).
`--report-dir /path/to/new-directory` selects a new private artifact directory; an existing
directory is refused. No target host, endpoint, user command or existing container is accepted.

The runner verifies engine/profile, exact container ID/label, resource controls, read-only NuGet
cache, no published ports and no external network. It creates a source snapshot archive, hashes
and copies it, verifies the archive in the container and builds an owned copy. It never builds or
writes into a mounted source tree. Source extraction retains the container owner rather than
restoring the host UID. It runs all grid cases with the process fixture explicitly enabled.

Required process, peer/client TLS, credential, storage/frontier and protocol classes must be
present, pass and have no skips. Zero cases, failed/inconsistent XML counters, infrastructure
errors, absent required classes and unexpected skips fail qualification. Only these omissions
are explicit exclusions: the eleven separate tmpfs ENOSPC cases and the separately opted-in
long global-session-capacity case. They are not counted as passes; their existing evidence keeps
its own scope. New skip conditions must be assessed rather than silently accepted.

Artifacts stay in the printed private directory:

| File | Meaning |
| --- | --- |
| `environment.txt` | OS, kernel/RID, SDK/runtime/OpenSSL, filesystem, UTC and cgroup observations |
| `source.tar` | Exact copied source/configuration snapshot, excluding build/cache output |
| `run.log` | Source integrity check, restore/build and test output |
| `tests.xml` | Executed xUnit cases, outcomes and skip reasons |
| `report.json` | Profile/owner/source/runner hashes, matrix counts, explicit omissions, exit and cleanup status |

A failed preflight/build may leave no test XML; the report records failure and available artifacts.
Hashes are unsigned local integrity fingerprints, not release provenance. The container is removed
by exact owned ID after success/failure or handled interruption/termination; cleanup failures are
recorded and never treated as a successful qualification. Source/cache and unrelated containers
remain untouched. Artifacts contain source and synthetic diagnostics, not runtime PKCS#12 files.

Qualification applies only to the selected local engineering envelope. Production environment
selection, independent-host/device qualification, actual workload measurements, supported release
upgrades and claimed Native AOT paths retain separate gates.
