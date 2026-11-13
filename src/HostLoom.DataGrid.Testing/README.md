# HostLoom.DataGrid.Testing

Non-packable deterministic fixtures for the local application and fixed three-member Raft
models. No sockets, processes or files are started; modeled persistence is memory retained by
the test controller. TLS and device durability are unverified.

Construct `DataGridSimulation(configuration, seed)` and Schedule typed committed commands. Step
persists a command to the injected `IDataGridSimulationJournal`, then applies it to the local
single-owner state machine. `crashAfterPersistence` stops before Apply; `loseReply` suppresses the
returned result without undoing application. Stop/Restart restore the memory checkpoint and log.
Checkpoint preserves application/session/version state and releases covered journal capacity.

DropNext, DuplicateNext and DelayNext script fake delivery; ScheduleRandom uses a reproducible
32-bit seed. Scheduling ticks do not advance grid UTC time; commands carry proposed grid time.
Injected `IDataGridSimulationTransport` and journal implementations support other deterministic
failure scripts. Successful journal append means durable only within the model's explicit rules.
Before/after append failure faults the owner until Restart, preserving uncertain outcomes.

The default network and journal each bound count to 4096 and encoded bytes to 16 MiB. Payloads
are bounded to the HLGR maximum. Journal exhaustion requires checkpointing. The trace retains
at most 8192 records/4 MiB ASCII characters; TraceTruncated signals dropped history. Keep source,
configuration and Seed when reproducing a failing trace. These are fixture limits, not measured
node memory/performance budgets. All APIs require serialized test-driver ownership.


`DataGridRaftSimulation(configuration, seed)` drives three single-owner Raft actors with one
atomic memory store per member. Step orders writes, packets and timer expirations using a seeded
scheduler; UtcTime is the simulated tick converted to UTC ticks. Election intervals are seeded
500–1000 ms, heartbeat 100 ms, peer retry 200 ms, and client deadline 5000 ms. These are fixture
settings, not workload NFRs. All driver APIs require serialized ownership.

Use Status to find a Ready leader, Propose for typed commands, Read for a quorum/time barrier,
and Outcome for physical-attempt completion. ReadLocal is diagnostic and has no quorum authority.
Ready records a committed current-term entry; an isolated leader can remain Ready, so it is not
proof of present connectivity. Operations still require fresh majority persistence. Advance and
RunUntil have event budgets; exhaustion is inconclusive and throws rather than silently passing.

SetLink/Isolate control delivery; DropNextMessage, DuplicateNextMessage and DelayNextMessage
operate on a queued packet. HoldWrites and FailNextWrite delay/fail modeled persistence.
Stop optionally persists the pending transaction before discarding its acknowledgement; Restart
uses the retained image and a new process epoch. LoseReply suppresses an observable response,
leaving an unknown mutation outcome on timeout. Compact preserves local hard state and suffixes;
a stopped follower catches up through a typed whole-image snapshot when its log was compacted.

The checker records invocations/completions and the exact committed order, replays application
results, and checks quorum persistence, real-time prefix bounds, captured read values/time,
at most one leader per term, monotonic hard state and equal committed application prefixes.
Unknown outcomes may have committed. Seed 7 deliberately injected premature success and a stale
read prefix are rejected by negative tests. This checker covers the simulation boundary, not a
network client or an independent proof of the Raft algorithm.

Raft fixtures bound live events to 4096/32 MiB packet bytes, request history to 8192, committed
and election histories to 65536 positions/terms, and retained request+command bytes to 64 MiB.
The trace retains 8192 records/4 MiB ASCII characters and marks gaps. Raft log/image limits are
4096 entries/16 MiB and the shared 512-MiB snapshot envelope. Client admission reserves 128
entries and one maximum encoded command below the hard log limits. The simulation driver still
drives maintenance explicitly; the separate runtime schedules it automatically. Capacity
exhaustion fails the experiment. These limits and atomic
memory writes are explicit model assumptions; no actual WAL, snapshot chunks, TLS or fsync runs.
