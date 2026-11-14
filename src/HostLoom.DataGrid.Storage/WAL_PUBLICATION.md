# WAL publication and recovery

The experimental serialized file owner uses CURRENT as the sole recovery authority. New roots
use a v2 publication frontier. WAL records and snapshots retain their existing v1 layouts.
These rules describe implemented protocol ordering; tests do not establish device/power-loss
durability, numerical performance or a supported production migration.

## CURRENT layout

All integers are big-endian; UUIDs use network order and must be nonempty.

| Offset | Bytes | v2 field |
| --- | --- | --- |
| 0 | 4 | `HLGC` magic |
| 4 | 2 | Format version 2 |
| 6 | 2 | Reserved zero |
| 8 | 16 | Incarnation UUID |
| 24 | 16 | Member UUID |
| 40 | 16 | Generation UUID |
| 56 | 8 | Published WAL bytes, signed, 1..1073741824 |
| 64 | 8 | Published record count, signed, 3..1000000 |
| 72 | 32 | SHA-256 of exactly the published WAL prefix |
| 104 | 32 | SHA-256 of bytes 0..103 |

The v2 encoded length is 136 bytes. The legacy v1 length is 88 bytes: version 1, the same
identities, then SHA-256 of bytes 0..55 at offset 56. V1 selects the entire named WAL and has
no prefix boundary. Version/length mismatches, unknown versions, nonzero reserved bits,
invalid ranges/identities or bad anchor checksum refuse startup. Hashes detect corruption;
they do not authenticate files against a malicious trusted local owner.

`DataGridCurrentAnchor.EncodedByteCount` remains the legacy 88-byte constant.
`FrontierEncodedByteCount` is 136. The three-UUID constructor still encodes v1; the frontier
constructor accepts byte count, record count and digest and encodes v2. `ReadFrom` accepts both;
`FormatVersion` identifies them. Published counts are null for v1 and `GetWalDigest` is empty.
Digest input and returned arrays are defensive copies. The codec itself performs no filesystem I/O.

## Persistence boundary

For an ordinary append, the serialized owner:

1. Reserves WAL delta bytes plus one temporary 136-byte anchor under existing directory/file/free
   reserve limits. These are admission checks, not an atomic filesystem-space reservation.
2. Writes all dependent hard-state/log/commit records and calls `FileStream.Flush(true)` on WAL.
3. Writes and flushes a unique temporary CURRENT containing the new length/count/prefix digest.
4. Atomically replaces CURRENT and flushes the directory.
5. Publishes cached progress and completes the persistence effect to Raft.

Dependent vote/replication acknowledgements and client completions follow that completed effect.
WAL flush alone is insufficient. A failure or cancellation after submission fences the owner;
admitted client outcomes remain uncertain. A crash before CURRENT replacement selects the prior
boundary. A replacement interrupted before directory flush can recover either valid root under
the filesystem's persistence assumptions; no dependent success was issued for that failed effect.

The prefix hash advances incrementally with new records. On restart it is rebuilt while reading
the selected prefix. Snapshot/rollover publication seeds and validates a fresh WAL, flushes and
renames it with directory persistence, then publishes its v2 CURRENT using the same final steps.
No parallel/background writer accesses the store.

## Recovery and rejoin

Open owns the directory and validates the inventory, CURRENT identity, selected prefix length,
record sequence/checksums, all semantic state transitions, exact record count, prefix digest and
referenced snapshot. Only then may recovered state become visible. Application replay ends at
the published commit checkpoint; the published uncommitted suffix and durable term/vote remain
available to consensus. Local recovery does not itself grant quorum authority.

A short or damaged published prefix, including a corrupt uncommitted record inside that prefix,
refuses startup with files unchanged. A valid older generation is never a fallback.

Bytes after a validated v2 frontier are explicitly unpublished. Open leaves them untouched and
ignores their contents, even if they form complete records. It never guesses publication from a
record's completeness. Before the owner can append again, Persist or RollGeneration publishes a
fresh seeded generation with the validated state and requested transition. It never overwrites
or truncates the old selected file. Interrupted rollover or another ENOSPC retains a valid prior
frontier; release space and retry validated restart. Retired files stay charged until normal
bounded collection seals the new root and deletes them. Collection compares the full CURRENT
against the owner's copy, rejecting same-generation frontier substitution before deleting files.

This permits a v2 member with a partial unpublished append to restart after space release,
retain its election/commit state and rejoin normal Raft reconciliation. The directed TLS test
checks all three members and immutable replay through another whole-group restart.

## Compatibility

Valid v1 roots still validate the entire WAL and remain unchanged on Open. The next Persist or
RollGeneration publishes a fresh v2 generation. A torn/corrupt v1 root still refuses; it has no
historical frontier from which this implementation could safely infer recovery. Keep that
member stopped and preserve the directory. No legacy salvage or operator reseed tool is provided.

Older binaries reject v2 CURRENT. Downgrade after publication and mixed-build operation are not
supported; use matching prototype binaries/build identities. The v1 record/snapshot/network
layouts do not imply that an older binary can open a new local root. Never restore an older
CURRENT to discard a vote or acknowledged effect. No production store is migrated by the test
runner. Native AOT and release upgrade compatibility retain separate qualification gates.

See [the storage README](README.md#reproduce-bounded-disk-exhaustion) for the owned tmpfs runner,
[frontier tests](../../tests/HostLoom.Tests/DataGridWalFrontierTests.cs),
[real ENOSPC tests](../../tests/HostLoom.Tests/DataGridDiskFullTests.cs) and
[member rejoin test](../../tests/HostLoom.Tests/DataGridDiskFullTlsTests.cs).
