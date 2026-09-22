# Changelog

All notable changes to HostLoom are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Package versions
are derived from release tags at publish time.

## [Unreleased]

Upgrading changes no public contract, but four defaults or behaviours change and are stated under
**Changed**: RabbitMQ publishes on up to sixteen pooled channels instead of one, a Kafka event
handler that keeps failing is retried until it succeeds instead of being dropped after five
attempts, the in-memory transport no longer reports subscriber failures to the publisher and holds
an unbound request for its full timeout, and a Redis Cluster connection without hash tags on
database zero now fails hosted startup even when `FailFast` is false. A Valkey deployment with a
restricted ACL must also allow the new `:probe:*` channel suffix. RabbitMQ request handlers on one
endpoint now run up to sixteen at a time instead of one after another; a handler that is not safe
to overlap needs `RabbitMqOptions.RequestDispatchConcurrency = 1` or a receive-pipeline
concurrency limit. `RequestAsync` and `PublishAsync` now throw `MessagingTransportException` for
transport failures, so code that caught a Kafka or RabbitMQ client-library exception from them
must catch the new type and read its inner exception. Cancelling a lock acquisition no longer
stops a command already sent, a leader keeps a held lease through a transient renewal failure,
and a hosted elector whose lease exceeds `Locking:MaxLease` fails at startup.

### Added

- `RabbitMqOptions.PublishTimeout` (30 seconds) bounds an event publication end to end: waiting
  for a publisher channel, declaring the exchange, and the broker confirmation. When it elapses
  the publisher receives a `TimeoutException`; a request publication stays bounded by the request
  timeout, which now also covers client initialization. `RabbitMqOptions.MaxConcurrentPublishes`
  (16) caps the confirmed publisher channels the broker keeps in its pool; each outstanding
  publication owns one exclusively. Both are validated when the broker is constructed. A channel
  whose publication was not confirmed is never reused and closes in the background, so the caller
  returns at its deadline rather than after the broker's close reply. At most
  `MaxConcurrentPublishes` channels close at once, and a publication that needs a new channel
  meanwhile waits within its own deadline. The `hostloom.rabbitmq.channels.closing` gauge reports
  them, and broker disposal waits at most five seconds for them before logging
  `RabbitMqChannelsStillClosing` (1403) and closing the connection.
- `RabbitMqRequestBroker(IOptions<RabbitMqOptions>, ILogger<RabbitMqRequestBroker>?)`. A delivery
  whose handler throws and whose requeue also fails is logged as `RabbitMqDeliveryRejected` (1401)
  at Error with the delivery tag, the original exception, and whether `DeadLetterExchange` is
  configured, before it is rejected without requeue. Previously the failure was swallowed.
  Logging does not retain the message; configure `DeadLetterExchange` for that.
- `KafkaRequestBroker` implements `IBrokerHealthProbe`, so the readiness check no longer treats
  Kafka as always reachable. It reports unhealthy after disposal, after a failed reply-consumer
  initialization ("the next request retries initialization"), and while the reply consumer is
  still awaiting its partition assignment; otherwise healthy, stating that broker reachability is
  not probed. It reflects local consumer state only.
- `InMemoryRequestBroker(ILogger<InMemoryRequestBroker>?)`. A subscriber that throws is logged as
  `InMemoryEventFailed` (1402) at Error with the subscription name.
- The transports reference documents the two RabbitMQ options, the probe support of each
  transport, and the in-memory delivery rules below.
- `RabbitMqOptions.RequestDispatchConcurrency` (16) and `RabbitMqOptions.EventDispatchConcurrency`
  (1), the deliveries a request listener and an event subscription handle concurrently on their
  channel, each validated between 1 and `PrefetchCount`. Previously both were the client
  library's default of 1, so `PrefetchCount` only buffered deliveries and every handler on an
  endpoint ran in turn.
- `InMemoryRequestBroker(ILogger<InMemoryRequestBroker>?, TimeProvider?)`. The request timeout and
  the unbound-address wait run on the supplied clock, and a host that registers a `TimeProvider`
  before `UseInMemory()` drives them from a test clock without waiting in real time.
- Meters `HostLoom.Transport.RabbitMq` and `HostLoom.Transport.Kafka`. RabbitMQ counts
  `hostloom.rabbitmq.publishes` by outcome (`confirmed`, `returned`, `failed`, `timed_out`) with
  `hostloom.rabbitmq.publish.duration`, `hostloom.rabbitmq.deliveries.rejected` by reason
  (`malformed`, `handler_failed`), `hostloom.rabbitmq.deliveries.requeued`,
  `hostloom.rabbitmq.connections` by event (`opened`, `recovered`), and the
  `hostloom.rabbitmq.requests.pending` gauge. Kafka counts `hostloom.kafka.produced` by kind
  (`request`, `reply`, `event`), `hostloom.kafka.consumed`, `hostloom.kafka.committed`,
  `hostloom.kafka.records.skipped` by reason (`malformed`, `unroutable_reply`,
  `attempts_exhausted`), `hostloom.kafka.records.rewound`, `hostloom.kafka.loop.faults` by stage,
  `hostloom.kafka.reply_consumer.initializations` by outcome, and the
  `hostloom.kafka.requests.pending` gauge. Handler outcomes stay on the kernel's request
  instruments and are not counted twice.
- `hostloom.lock.orphan_releases`, tagged `hostloom.lock.outcome` (`released`, `absent`,
  `failed`), and the `LockOrphanRelease` (3108) Debug event, which record the release described
  under **Changed** below.
- `MessagingTransportException`, thrown by `IRequestBroker.RequestAsync` and
  `IEventBroker.PublishAsync` when a transport cannot carry a request or an event. It carries the
  `Address` and the client library's exception as `InnerException`. Whether the broker accepted
  the message is unknown when it is thrown, so a retry can deliver an event twice or run a handler
  twice.
- The broker contracts document their observable outcomes: a timeout ends with
  `RequestTimeoutException` and an unbound or unroutable address times out, caller cancellation
  carries the caller's own token, disposal throws `ObjectDisposedException`, a handler's token
  belongs to its listener, a completed publication means acceptance rather than handling, and a
  health probe states whether it reports local state or checks the broker. A shared transport
  conformance suite runs the same scenarios on the in-memory transport, RabbitMQ, and Kafka.
- `FaultingLockProvider.HoldReplies()`, `DeliverReplies()`, and `WaitForHeldReplyAsync()` hold
  acquisition replies so a test can abandon an attempt the backend already granted; `Heal()` also
  delivers held replies. Lock conformance gains `AbandonedAcquisition_LeavesTheKeyFree`, run on
  the in-process lock, Redis, and Valkey.
- The caching reference documents write visibility across instances for each invalidation mode:
  `SetAsync` and `SetIfAbsentAsync` publish nothing, so with the explicit channel another instance
  keeps its old in-process value until it expires, while tracking and broadcast observe store
  writes. In broadcast mode the writer also evicts its own entry on each write. Cache conformance
  gains cross-instance removal, overwrite, own-echo, delivery-gap, and tag-removal scenarios.
- Opt-in real-broker experiments: `HOSTLOOM_RABBITMQ_CHAOS=1` times out a publication and drops
  and restores a connection through a private TCP proxy, and `HOSTLOOM_KAFKA_CHAOS=1` pauses a
  Kafka container the test owns, including during the reply consumer's watermark query. The Kafka
  meters are also asserted against the compose broker by default.

### Changed

- RabbitMQ requests and events are published on a pool of confirmed channels, up to
  `MaxConcurrentPublishes`, and the exclusive reply queue keeps its own channel. Previously every
  request and event shared the reply-queue channel behind a single gate, so one stalled
  confirmation serialized all publication. An upgrader who needs the old serialization sets
  `MaxConcurrentPublishes = 1`; a deployment with a low `channel_max` should budget the extra
  channels per instance. Publishing an event no longer initializes the reply queue. Disposal
  cancels publishers that are waiting for a channel or a confirmation with
  `ObjectDisposedException`, waits for borrowed channels to return and at most five seconds for
  channels still closing, then closes the pool, the client channel, and the connection,
  aggregating any failures; repeated disposal shares one task.
- Kafka commits an offset after the handler returns, from the consumer loop, rather than inside
  the handler. Successful handling and the commit are separate failure boundaries: a commit that
  throws is logged and does not seek back and re-run a handler whose reply was already produced.
  Kafka can still redeliver an uncommitted record after a reassignment, so handlers must remain
  idempotent.
- A Kafka event subscription retries a record whose handler keeps throwing indefinitely, seeking
  back with the consume backoff each time, instead of logging an error and committing past it
  after five attempts. A record that never succeeds holds its partition at that offset until
  shutdown, so a handler that wants a poison event skipped must swallow it itself. Request
  subscriptions keep the five-attempt cap; malformed records and unroutable replies are still
  logged, committed, and skipped.
- The in-memory transport delivers like a broker. A handler runs on the thread pool with the
  listener's or subscription's lifetime token rather than the caller's: caller cancellation ends
  the caller's wait without cancelling accepted receiver work, and disposing the listener, as a
  host stop does, cancels work in progress instead of letting it finish. The requester of that
  work waits out its own timeout and gets `RequestTimeoutException`, as with a broker that has no
  other listener, instead of seeing the listener's cancellation. A publication is
  accepted once every subscriber has been attempted; a subscriber failure is logged at 1402 and no
  longer reaches the publisher as an `AggregateException`, so an outbox relay does not retry it. A
  request to an address with no listener waits the full request timeout before
  `RequestTimeoutException`, with an inner `TimeoutException`; previously it was refused at once.
  Disposing the transport ends every waiting request at once with `ObjectDisposedException`, every
  member throws it afterwards, and the probe reports unhealthy. Tests that relied on the old
  behaviour changed with it: `MessagingChaosTests` now expects a request whose listener stops to
  time out and an unbound request to take its budget, and `PublishSubscribeTests` no longer
  expects a failing subscriber to fault the publish.
- `RedisConnection` throws `ArgumentException` ("Redis Cluster requires UseHashTags = true and
  DatabaseIndex = 0") whenever it hands out a multiplexer, including one supplied through
  `Redis:ConnectionFactory`, if any endpoint reports Cluster topology and the options differ.
  `FailFast = false` no longer downgrades this to `RedisUnreachableAtStartup` (1304): hosted
  startup fails. An upgrader on Cluster must set `UseHashTags = true` and `DatabaseIndex = 0`.
  Hash tags wrap the namespace segment in `{…}`, so keys written before the change are under
  different names: expect a cold cache, and roll every instance together where locks or leases
  must stay mutually exclusive across the fleet.
- The Valkey invalidation channel publishes its liveness probe to a private channel,
  `{invalidation channel}:probe:{id}`, subscribed on the same socket with its own reader and
  queue, so a slow invalidation handler or an overflowed invalidation queue no longer reads as a
  dead subscriber. An ACL that names the invalidation channel must also permit the `:probe:*`
  suffix. Probe payloads that an earlier version still publishes on the invalidation channel are
  ignored.
- The Valkey reconnect backoff is reset only after a subscription has stayed established for 30
  seconds. Previously any acknowledgement reset it to 100 ms, so a socket that was acknowledged
  and dropped repeatedly reconnected at the floor rate for as long as it flapped.
- Every acknowledged Valkey subscription, including the first one of a channel's lifetime, hands
  `CacheInvalidation.Flush` to its subscribers when `FlushLocalOnReconnect` is set. Previously the
  first subscription flushed only if an earlier attempt had failed, leaving entries filled while
  the acknowledgement was pending. An internal `TimeProvider` constructor drives the backoff in
  tests.
- The lock gives each acquisition its own provider token, cancelled only when the lease has run
  out since the request was sent or when the `DistributedLock` is disposed; the caller's token and
  `MaxWait` end only the lock's wait. An acquisition whose outcome is uncertain no longer leaves a
  grant held for the whole lease: exactly one bounded, owner-checked release that the caller never
  waits for follows caller cancellation or `MaxWait` expiry once the grant arrives, a reply
  rejected because the usable lease had already run out, and a provider failure of kind `Timeout`
  or `Other`, where the caller still receives the same exception. `Unavailable` means the backend
  was not reached and releases nothing, which includes a Redis connection that drops mid-command.
  Cancelling therefore no longer stops a command already sent: an abandoned attempt can cost one
  extra write plus one release, and a provider call can outlive its caller by up to one lease.
  Providers follow standard cancellation, and the locking reference no longer claims that
  cancellation leaves nothing held.
- Disposing a `DistributedLock` ends its in-flight acquisitions with `ObjectDisposedException`,
  and every later `TryAcquireAsync` or `ExecuteWithLockAsync` throws it, even with locking
  disabled. Held handles still release.
- `FaultingLockProvider.TryAcquireAsync` is asynchronous, so an injected failure arrives as a
  faulted task instead of a synchronous throw.
- A leader whose renewal fails while its lease is still held stays leader and retries at the
  renewal interval or half the remaining lease, whichever is sooner, never more often than a
  twentieth of the lease, until an extension succeeds under the same term or the lease ends.
  Previously one transient provider failure handed leadership over and cancelled leader-only work.
  A refusal, a lost handle, lease end, resignation, and stop still end leadership at once. Such a
  failure is recorded as `failed` rather than `refused` on `hostloom.leader.renew.duration`.
- A hosted elector whose `Leadership:Lease` exceeds `Locking:MaxLease` fails at startup with a
  message naming the role and both values. Previously the lock capped the lease silently, which
  could break the rule that the renewal interval is at most half the lease. An elector built
  without the container is still capped silently.
- `RequestAsync` and `PublishAsync` report transport failures as `MessagingTransportException`.
  RabbitMQ maps nacks, closed connections and channels, an unreachable broker, and other
  client-library, socket, and I/O failures; Kafka maps a reply consumer that fails to start and
  `ProduceException`. An unroutable request still ends in `RequestTimeoutException`,
  `PublishTimeout` still throws `TimeoutException`, and a failure after disposal is
  `ObjectDisposedException`. `ListenAsync` and `SubscribeAsync` still surface the client
  library's exception, which fails host startup. A cancelled RabbitMQ caller now receives its own
  token instead of the transport's linked one, and a Kafka publication after disposal throws
  `ObjectDisposedException`.
- Invalidation generations are striped by tag as well as by key. A tag invalidation suppresses
  only in-flight fills that declare a tag sharing its stripe, an untagged fill ignores tag
  invalidations, and a fill on an unrelated tag reaches both tiers. Previously any tag
  invalidation on any instance discarded every in-flight fill, so a key with a slow factory was
  never cached while tags were being invalidated. A flush still suppresses every fill, bulk reads
  and warmup stay on the whole generation, and a tagged payload read from the distributed tier
  while a tag invalidation was applied is returned but not copied into the in-process tier. Every
  writer of a key must declare the same tags for the suppression to hold.
- StackExchange.Redis 3.3.1 and FusionCache 2.9.0 (with its System.Text.Json serializer).

### Fixed

- A Kafka reply consumer whose initial watermark query failed no longer poisons every later
  request. The failed consumer is disposed, its offsets cleared, and the next request starts a
  fresh consumer; previously the failure was latched and the documented remedy was to recreate the
  host. Disposal waits for an initialization in flight before closing.
- A tagged Redis `SetIfAbsentAsync` writes the tag memberships and the value in one script, and
  a losing writer adds no membership. Previously the conditional `SET` and the membership batch
  were two round trips, so a disconnect or caller cancellation between them left a value no tag
  removal reaches. The script also refuses a tag index that is not a set before it writes.
- Redis tracking flushes the in-process tier after the command connection is restored and
  tracking is registered on the new connection, when `FlushLocalOnReconnect` is set and the mode
  is not `Broadcast`; a fallback to the explicit channel keeps the flush pending until tracking
  registers. Previously only a re-established subscription connection flushed, on the 0.7.0
  reasoning that an interactive blip loses nothing, which does not hold for `CLIENT TRACKING`:
  it is per connection, so keys read before the drop had no invalidation.
- Disposing a stale in-memory listener or subscription handle removes only its own registration:
  a successor bound to the same address or subscription name after the first disposal is no
  longer removed by disposing the first handle again.
- The in-process tier's warning when it clears itself at 150 % of `L1.MaxEntries`
  (`CacheL1Cleared`, 1101) is logged through the cache's logger; previously the tier was built
  without one and the warning never appeared. Expired entries are swept once per interval instead
  of twice.
- The Valkey invalidation channel throttles its warnings on the injected clock and is safe when
  several threads warn at once.

## [0.9.0] - 2026-09-22

This release adds scheduled jobs, lease-based leadership, and outbox/inbox support. Upgrading
from 0.8.0 requires reviewing the source and deployment changes below: custom lock implementations
must expose `IsCoordinated`, RabbitMQ deployments must migrate queue names or explicitly select
`Legacy`, remote fault details are now opt-in, and keyed WebSocket topics refuse topic-wide
subscriptions unless explicitly enabled. The browser client has its own version and release tags.

### Added

- `HostLoom.Scheduling`, a scheduled-job kernel in the shape of the caching and locking kernels:
  `IScheduledJob`, `ScheduleTrigger.Cron` (five or six fields, names, steps, `?`, Sunday as 0
  or 7, either-day matching, time zones with daylight-saving handling), `FixedRate` counted from
  the previous due time, `FixedDelay` counted from the previous completion, and a `Scheduler`
  that runs each schedule as one sequential loop over `TimeProvider` with per-run timeout, typed
  outcomes, state, an execution-free probe, and the `HostLoom.Scheduling` meter and activity
  source. A late run starts at once and missed occurrences are not replayed; a failed run is
  logged and the schedule continues. An exclusive schedule keeps its claim past the run until the
  lease ends or its next occurrence is due, so a slower instance reaching the same occurrence is
  refused; `ScheduleState.ClaimHeldUntil` reports the hold. Two-instance cluster tests cover
  local and exclusive schedules, hand-over on stop, a lock backend outage, and a lost lease, and a
  real-Redis contention test plus an opt-in outage experiment run in the integration suite.
- `IScheduleGuard`, the contract that runs an `Exclusive` schedule on one instance at a time:
  a claim is granted or refused at once, a refused claim skips the run, and a guard failure
  skips it as `GuardFailed` rather than running unguarded.
- `HostLoom.Scheduling.DependencyInjection`: `AddHostLoomScheduling`, `AddSchedule<TJob>` with a
  fresh scope per run, a delegate overload, `UseGuard<TGuard>(name)` with the exactly-one rule,
  startup validation naming the builder method an exclusive schedule needs, and the hosted
  service that starts and stops the scheduler with the host.
- `HostLoom.Scheduling.Locking`: `DistributedLockScheduleGuard` over `IDistributedLock`, one
  skip-if-busy acquisition of `schedule:{name}` per run with automatic extension up to
  `Locking:MaxHold`, and `UseDistributedLock()`.
- `HostLoom.Scheduling.Testing`: `ManualScheduleGuard`, scripted with `Hold`, `Release`, `Lose`,
  and `FailNext`, recording every claim.
- `HostLoom.Leadership`, lease-based leader election over the distributed lock: `ILeadership`
  (`Role`, `IsLeader`, `Term`, `LeadershipToken`, `WaitForLeadershipAsync`, `OnChange`),
  `LeaderElector` with a candidate loop on a jittered retry cadence, explicit renewal that is not
  capped by `Locking:MaxHold`, immediate step-down on a refused renewal or an expired lease
  followed by one retry interval, `ResignAsync`, release on stop, a probe, the
  `HostLoom.Leadership` meter and activity source, and log events 3400 to 3408. The guarantee is
  stated as the lock's: at most one leader while clocks and the backend behave, a window bounded
  by the lease on expiry, and no fencing; `Term` counts this instance's acquisitions.
- `HostLoom.Leadership.DependencyInjection`: `AddHostLoomLeadership().AddRole(role, configure)`,
  electors and `ILeadership` keyed by role with the single role also resolving unkeyed, named
  options validated per role, and the hosted service that starts and stops every elector.
- `HostLoom.Scheduling.Leadership`: `LeaderScheduleGuard` and `UseLeader(role)`, which run
  exclusive schedules on the elected leader with no lock round trip per occurrence; a run in
  progress ends as `ClaimLost` when leadership ends.
- `HostLoom.Leadership.Testing`: `ManualLeadership`, flipped with `Acquire`, `Lose`, and `Resign`.
- `LeaderChannel<T>` in `HostLoom.Leadership`, a bounded `Channel<T>` gated on a role: every
  instance writes to it, a leader's items reach the reader, and a follower's are accepted and
  discarded so a warm standby keeps its state current without acting. `LeaderChannelOptions`
  sets the capacity, full mode (`DropOldest` by default), and drop report interval, validated by
  key. Drops are counted on `hostloom.leader.channel.dropped` by channel and reason (`follower`
  or `full`) and summarised in the log at most once per interval, with the tail flushed when the
  instance becomes leader and on disposal: `LeaderChannelFollowerDropped` (3409) at Information
  and `LeaderChannelFull` (3410) at Warning. Fault tests feed a channel on each of two electors
  through a refused renewal, a provider outage, racing writers, a throwing listener, and
  disposal mid-term, and a real-Redis case with the leader cut off measures the items that land
  on both instances against the lease.
- Two-elector tests over the in-process lock and a fake clock, and a real-Redis test plus an
  opt-in outage experiment that measures the hand-over window against the lease.
- A transactional outbox in `HostLoom`: `IOutboxStore` (append inside the caller's unit of work,
  atomic claim with a lease, mark published or failed), `OutboxMessage` carrying the encoded
  frame, `UseOutbox<TStore>()` and `UseInMemoryOutbox()` on the builder, which route every
  `IPublishEndpoint.PublishAsync` through the store from the caller's scope, and `OutboxRelay`, a
  hosted loop that drains when woken and every `Outbox:PollInterval`, publishes each frame
  unchanged through the transport, and leaves a failed message pending with its error. Delivery
  is at-least-once. Metrics `hostloom.outbox.published`, `hostloom.outbox.failed`, and
  `hostloom.outbox.lag`; log events 3300 to 3303.
- An idempotent inbox in `HostLoom`: `IInboxStore` with one set-if-absent member,
  `InboxStore.FromClaim` for a cache-backed store in one line, `InMemoryInboxStore`, and
  `InboxFilter`, appended to the receive pipeline by `UseInbox<TStore>(window)`,
  `UseInbox(factory, window)`, or `UseInMemoryInbox(window)`. It records
  `{topic}:{subscription}:{messageId}` before the handlers run, skips a delivery seen inside the
  window with an `InboxDuplicate` payload, runs anyway with an `InboxSkipped` payload when the
  store cannot answer, and passes requests through untouched. Metric
  `hostloom.inbox.duplicates`; log events 3310 and 3311.
- `HostLoomBuilder.ConfigureReceivePipeline(Action<PipeBuilder<ReceiveContext>, IServiceProvider>)`,
  for a filter that needs a service from the container, such as a store. The pipeline is still
  composed once, from the root provider.

### Changed

- Source-breaking contract additions from the security hardening below: `IsCoordinated` on
  `IDistributedLock`, `ILockHandle`, `ILeadership`, and `IScheduleGuard`;
  `IOutboxStore.MarkFailedAsync` takes the next attempt time and `MarkDeadLetteredAsync` is new;
  `OutboxMessage.NextAttemptAt`, `WebSocketTopicDescription.AllowTopicWideSubscription`,
  `LeadershipDescription.Coordinated`, and `SchedulingDescription.GuardCoordinated` are new
  positional record parameters. In-repo implementations and the Testing packages are updated.
- Remote faults no longer carry the handler's exception type and message by default. A handler
  that wants the caller to see a message throws `RemoteFaultException`; the previous behavior is
  available through `HostLoomOptions.IncludeFaultDetails`. Framework-routed faults use the stable
  types `HandlerNotFound` and `ResponseTypeMismatch`.
- A leader elector whose lock is disabled (`Locking:Enabled=false`) no longer leads on every
  instance. `LeadershipOptions.WhenUncoordinated` defaults to `Follow`; a single-instance or
  development host opts into `Lead`. The elector warns once per role either way.
- A logging scope hole such as `{User}` destructures a non-scalar value through the same
  fail-closed, mask-aware path as an event hole instead of calling `ToString()`; `{$User}` keeps
  the plain text form. The templated scope text is rendered from the captured fields.
- The inbox key is length-prefixed per component, so keys recorded before the upgrade are not
  recognised for the remainder of their window.
- Redis health-check descriptions report reachability and latency without endpoints, machine
  name, or process id; `Describe()` is unchanged for logs.
- The development `docker-compose.yml` binds RabbitMQ, Kafka, and Redis to loopback only.
- RabbitMQ defaults to versioned, role-qualified queue names that separate request routes and
  event subscription pairs. Existing deployments must follow the
  [queue migration guide](docs/how-to/use-rabbitmq.md#migrate-existing-queues) or explicitly set
  `RabbitMqOptions.QueueNaming` to `RabbitMqQueueNaming.Legacy` during rollout.
- RabbitMQ event publication waits for publisher confirmation and sets persistent delivery when
  `DurableTopics` is enabled. Unconfirmed outbox messages remain retryable; retries can duplicate
  an event whose confirmation was lost.
- The WebSocket gateway ends a subscription before it answers an invalid `credit` or `ack` frame
  with a fault, so a fault after `subscribed` is terminal on both peers. Previously the
  subscription stayed live behind the fault and kept delivering events on a stream the browser
  client had already discarded, until its orphan cleanup sent an `unsubscribe`.

### Fixed

- Lock acquisition rejects a successful backend reply that arrives after the usable lease has
  expired, reporting `LockFailureKind.Timeout` instead of granting an expired handle.
- Concurrent lock renewals, including automatic heartbeats, serialize backend calls with local
  deadline updates. Waiting honours cancellation and rechecks ownership before issuing a command,
  preventing an older renewal from overwriting a newer lease deadline.
- Kafka reply consumers retain progress across reassignments and fail initialization instead of
  falling back to an unresolved end offset. Live request/reply tests provision their owned topics.
- RabbitMQ subscriptions close channels even when cancellation callbacks throw and tolerate
  repeated disposal.
- Redis and Valkey validate canonical tagged-write domains before backend I/O, preventing values
  from being accepted and then orphaned by the tag invalidation filter.
- Logging bounds message and text-field buffer growth before encoding and falls back to the
  capped rendered message when a CLEF template exceeds the message budget.
- The npm publish step uses one environment mapping for its version and bootstrap token.
- Lock-loss callback exceptions no longer escape timer callbacks or interrupt lease accounting.
- Valkey tag invalidation removes only snapshotted members, retaining concurrently added keys for
  later invalidation. Restricted cache ACLs now also need `SREM` permission.
- Concurrent L1 mutations keep byte accounting consistent with the stored entries.
- WebSocket subscription initialization and removal detach linked cancellation resources safely.
- Pending leader-channel writes wake on leadership loss and follow the follower discard policy.
- Scheduled jobs retain stop, timeout and claim-loss outcomes after cooperative normal returns.
- Logging shutdown bounds synchronous sink disposal and cancellation callbacks as well as
  asynchronous stalls.
- A lock heartbeat that fails re-arms itself halfway to the lease end and keeps retrying until
  an extension succeeds or the lease runs out. Previously one transient backend failure ended
  automatic extension for good and the lease expired under a healthy action.
- A release that throws is logged as `LockReleaseFailed` only: the backend keeps the owner's
  lease until it expires, so it is no longer reported as a loss, counted on `hostloom.lock.lost`,
  or allowed to cancel `LostToken`. `LostToken` also stays readable after the handle is disposed.
- `RedisConnection` reports the client name of a multiplexer supplied by `Redis:ConnectionFactory`,
  so tracking finds that connection's subscriber. Previously the channel looked for the
  suffixed name it would have configured itself, never found it, and fell back to the explicit
  channel. Tracking now refuses a client name shared by more than one pub/sub connection on a
  server instead of redirecting to whichever the server listed first, and broadcast mode checks
  `notify-keyspace-events` where `CONFIG GET` is permitted instead of reporting itself enabled
  on the strength of a subscription alone.
- A tagged Redis write pipelines the tag memberships before the value, so a connection lost
  part-way leaves an index entry for an absent key rather than a value no tag removal reaches.
- `CacheFilter` runs the rest of the pipe when the lookup finds a remembered null instead of
  failing on a null payload, and the produced payload replaces the remembered absence.
- The `IDistributedCache` adapter applies `Caching:MaxPayloadBytes` to the consumer's bytes; a
  larger payload is not written, logged at error level once per key per interval, and counted
  as a `payload` error.
- The Valkey invalidation channel flushes the in-process tier when its first subscription lands
  after failed attempts, not only after a lost one, and publishes a probe to itself every
  `InvalidationProbeInterval` (30 seconds by default) and waits `CommandTimeout` for it, so a
  subscriber socket that dies without a close is replaced within
  `InvalidationProbeInterval + CommandTimeout` (35 seconds by default) instead of looking
  subscribed indefinitely.
- `RecordingCacheStore` wraps a store whose channel is a separate class, as `FaultingCacheStore`
  already did, instead of failing when the cache subscribes.
- The cache tracks invalidation generations in 1024 key stripes, so an invalidation of one key
  no longer suppresses every in-flight fill on the instance; tag and flush invalidations still
  suppress all of them, and bulk reads and warmup stay on the whole generation. The echo of this
  instance's own publish is recognised within `Caching:Invalidation:Timeout` and counted as
  `echoed` on `hostloom.cache.invalidations` instead of being applied again, so the refill that
  follows a removal is cached rather than dropped. A distributed read that completes after a
  later write to the same key no longer replaces that write's in-process entry.

### Security

- The WebSocket gateway refuses a keyless `subscribe` to a keyed topic with `forbidden` unless
  the registration passes `allowTopicWideSubscription: true`. Previously any principal who passed
  a key-ignoring topic policy received every key's events and the whole snapshot. Keyed examples
  now use `TopicKeyPolicy.SubjectOnly`.
- The gateway drops an event that exceeds `MaximumMessageSize` instead of aborting every
  subscriber, faults a stream whose snapshot value is too large with `snapshot_failed`, and
  answers an oversized response with a `message_too_large` fault. Only the outbound budget still
  aborts a connection.
- `HostLoomWebSocketOptions.SnapshotInitializationTimeout` (30 seconds) bounds how long a
  subscriber can hold a snapshot provider open by withholding credit; the stream faults with
  `snapshot_stalled` and the provider scope is disposed.
- `HostLoomWebSocketOptions.MaximumRequestsPerSecond` (100) budgets `request` frames before any
  scope, authorization, or deserialization work; every other client frame kind shares the control
  budget. Session expiry works for any lifetime, including `DateTimeOffset.MaxValue`, and cleanup
  runs even when the expiry timer faults.
- The gateway resolves every configured authorization policy name at startup and fails the host
  with the missing names; a runtime authorization exception maps to `forbidden`.
- Kafka validates the `hostloom-reply-to` header against topic-name rules and, when
  `KafkaOptions.AllowedReplyTopics` is set, against that list before the handler runs. A reply
  that cannot be produced is logged as `UnroutableReplyException`, committed, and skipped; the
  handler is not re-run. The reply consumer starts from the latest offset, and
  `KafkaOptions.MaxRequestAge` can reject stale records.
- `KafkaOptions` gains `SecurityProtocol`, `SaslMechanism`, `SaslUsername`, `SaslPassword`,
  `SslCaLocation`, and `ConfigureClient` so the transport can authenticate and use TLS; the Kafka
  guide documents a secure baseline.
- RabbitMQ accepts only server-named reply queues (`amq.gen-*`, `amq.rabbitmq.reply-to`) unless
  `RabbitMqOptions.AllowNamedReplyQueues` is set, nacks with requeue when a delivery is cancelled
  by shutdown, and declares queues with `x-dead-letter-exchange` when
  `RabbitMqOptions.DeadLetterExchange` is set.
- Envelope decoding rejects an empty `MessageId`, an empty `CorrelationId`, and a response or
  fault without one, so a sender cannot poison the inbox window with a zero id.
- The outbox retries with exponential backoff (`OutboxOptions.RetryDelay`, `MaxRetryDelay`,
  `RetryBackoffFactor`) and dead-letters after `OutboxOptions.MaxAttempts` (10), counted on
  `hostloom.outbox.dead_lettered`; the persisted failure text is the exception type name only.
- The `messaging.message.type` tag is `unknown` for an unregistered type, bounding cardinality.
- `[NotLogged]` and `[LogMasked]` are honoured on overridden virtual properties.
  `HostLoomLoggerOptions.MaxMessageLength` (16 KiB) and `MaxTextFieldLength` (8 KiB) cap message
  text, plain fields, and scope texts.
- The Redis invalidation channel drops a message over 1 MiB, over 10 000 keys and tags, or with
  an invalid key, counted on `hostloom.redis.invalidation.malformed`; the Valkey codec validates
  each item the same way. Tag members outside the namespace's data prefix are removed from the
  index instead of unlinked, counted on `hostloom.redis.tag.members_rejected` and
  `hostloom.valkey.tag.members_rejected`. The Redis guide documents the ACL and TLS baseline.
- The single-flight guard map is bounded at four times `L1.MaxEntries` and falls back to striped
  semaphores, and the `IDistributedCache` adapter validates every key.
- `CacheKey.FromSensitive(value, key)` and `LockKey.FromSensitive(value, key)` hash with
  HMAC-SHA256 for low-entropy secrets; the unkeyed form is documented for high-entropy inputs.
- `LeaderChannelOptions.DrainOnLoss` drops buffered leader-era items when leadership is lost.
  The elector loop survives an unexpected exception (`hostloom.leader.loop.faults`, backoff, and
  continue) and validates its delays against the timer limit. `UseGuard`, `AddHostLoomLeadership`,
  and `AddRole` throw when an earlier registration would silently supersede the chosen guard or
  leadership. Cron steps are bounded to the field range.
- Release workflows pass tag and version values to shell steps through environment variables.

## [0.8.0] - 2026-09-19

Upgrading changes no public contract and no default behaviour. Every addition is opt-in: an update
map exists only where `MappingBuilder.AddUpdate` registers one, it is resolved as its own service
and never through the `IMapper` dispatcher, and `HLM0017` reports at information severity. The one
fix is confined to `HostLoom.Caching.Testing` and changes how `FaultingCacheStore` composes over a
store without an invalidation channel.

No package is published for the first time; the set is the same as 0.7.0.

### Added

- `HLM0017`, an informational analyzer rule: a generic map whose destination is a type parameter
  cannot have its completeness checked by `HLM0004`, and was previously skipped in silence. The
  rule names the map and the parameter so each closed pair can be covered by a test instead.
- A committed mapping benchmark baseline (`benchmarks/baselines/mapping.json`) over the flat and
  nested maps, collection mapping, `MapMany` strategies, and map lifetimes, with
  `just benchmark-mapping-check` and `benchmark-mapping-update` recipes, so a mapping regression
  fails a gate like the cache, lock, composition, and WebSocket fan-out baselines do.
- `IUpdateMapper<TSource, TDestination>`, an explicit update contract in `HostLoom.Mapping`:
  `MapInto(source, destination)` writes into the instance it is handed and never replaces it, so a
  caller keeps the identity it already holds and every member the map does not assign keeps its
  value. The destination is constrained to a reference type; both arguments are non-null by
  contract. A creation map and an update map for one pair are distinct services and coexist.
- `MappingBuilder.AddUpdate` overloads mirroring `Add`: pair inference from the one
  `IUpdateMapper<,>` a class implements, the explicit triple, a factory, and a prebuilt instance.
  Each call registers exactly one contract, so a class that both creates and updates registers
  through `Add` and `AddUpdate`. Update maps are never resolved by the `IMapper` dispatcher and
  are therefore exempt from the singleton-dispatcher lifetime rule.
- `MappedPairRegistry.UpdatePairs` and `ContainsUpdate`, listed separately from creation pairs so
  `MappingNotFoundException` never suggests an update map the dispatcher cannot resolve.

### Changed

- The `HostLoom.Mapping` README leads with closed-map injection, the shape every adopter has used,
  and presents the dispatcher as the tool for an orchestration that maps several pairs.
- The mapping README and reference document the dictionary recipe — copy with an explicit
  comparer, name the null policy at the call site, and rely on the duplicate-key throw — instead
  of adding dictionary extensions whose whole value would be a name.
- The analyzer documentation states that `HLM0004` and `HLM0005` inspect creation maps only: a
  partial update is the normal case for `IUpdateMapper`, so its body is not checked for
  completeness.

### Fixed

- `FaultingCacheStore` over a store that offers no invalidation channel of its own, such as
  `RedisCacheStore`, no longer makes `TieredCache` throw from its constructor. It fans out nothing,
  exposes the channel it does have as `Channel`, and reports "no channel: TTL-only" to the probe.


## [0.7.0] - 2026-09-17

Upgrading changes no public contract. Two behaviours change by default: a backend invalidation
channel now clears the in-process cache tier after a reconnect, and a configured
`Caching:L1:ExpirationJitter` larger than an entry's time to live no longer reduces that entry to a
single tick. Both are stated under **Changed** and **Fixed** below. One payload change needs a
complete rolling deploy before a call site adopts it: an instance on 0.6.0 reads a cached null
written with `CacheEntryOptions.NullExpiration` as an unreadable payload and logs it as a miss.

No package is published for the first time; the set is the same as 0.6.0.

### Added

- `CacheEntryOptions.NullExpiration`: negative caching. A null factory result is remembered in
  both tiers for its own time to live as a payload with the `null` flag and no body;
  `TryGetAsync` reports it as found with a null value, and a non-nullable value type reads it as
  a miss. An instance on an earlier version reads such a payload as unreadable and logs it.
- `CacheEntryOptions.StaleGrace`: stale-while-revalidate for an outage. An expired in-process
  entry inside the grace is served while the distributed store is unavailable; one caller
  refreshes through the factory and the others receive the copy at once, as the new `hit_stale`
  outcome on `hostloom.cache.operation.duration`.
- `CacheInvalidation.Flush` and `FlushAll`: an invalidation that clears the whole in-process
  tier, counted as the new `flushed` direction on `hostloom.cache.invalidations` and logged at
  information level. The Redis channel carries it as a `*` line and the Valkey channel as a
  fourth array element; earlier versions ignore or count it as malformed.
- `Caching:Invalidation:FlushLocalOnReconnect` (default `true`): the Redis and Valkey channels
  raise the flush to their own subscribers after a reconnect. On Redis the trigger is a
  re-established subscription connection; an interactive-connection blip loses no invalidation
  and does not flush.
- `LocalCacheStore.SetNull` and `TryGetWithinGrace`, and a `staleGrace` argument on `Set`.
- A reference section on what belongs in the in-process tier.

### Changed

- After a reconnect, the Redis and Valkey invalidation channels clear the in-process tier of
  every cache subscribed to them, because invalidations published during the outage were never
  delivered. Set `Caching:Invalidation:FlushLocalOnReconnect = false` to keep the previous
  behaviour, where such entries stayed until expiry.
- `RedisCacheStore.Capabilities` includes `ServerAssistedTracking`, which was declared but never
  reported.
- Dependencies: `StackExchange.Redis` 3.2.15, `Microsoft.Extensions.*` 10.0.12,
  `Microsoft.Extensions.Caching.Hybrid` 10.10.0, `Confluent.Kafka` 2.15.1, `MessagePack` 3.1.9,
  `protobuf-net` 3.4.30, `xunit.v3` 4.0.1, and the FusionCache benchmark comparison 2.8.0.
- `LocalCacheStore.TryGet<T>` declares its value as `T?`, and `CacheLookup.Hit<T>` accepts a
  null value, for remembered absences.

### Fixed

- `Caching:L1:ExpirationJitter` subtracts at most half of an entry's time to live. A jitter
  larger than a short expiration previously left the entry alive for one tick, which disabled
  the in-process tier for that entry.

## [0.6.0] - 2026-09-06

Upgrading changes no public contract. Four changes can surface on upgrade: a repeated
`AddHostLoomMapping` call validates maps against the dispatcher lifetime it actually retained, so a
lifetime check a second call previously bypassed can now fail; the mapping completeness analysis
inspects inherited members and nested functions, so `HLM0005` can report a map it accepted before;
the circuit-breaker and rate-limit filters read monotonic `TimeProvider` timestamps, so a fake
provider must advance its timestamp as well as its timers; and an `AddOrUpdatePayload` factory that
returns a replacement or null for a payload the context itself implements throws
`InvalidOperationException`. Each is stated under **Fixed** below.

One package is published for the first time: `HostLoom.Valkey`, a standalone Valkey backend for
`HostLoom.Caching` and `HostLoom.Locking` over ValkeyDotNet 1.1.0. It depends on both
`DependencyInjection` packages and on `ValkeyDotNet`, keeps every SDK type inside the adapter, and
leaves `HostLoom.Redis` unchanged.

### Added

- `HostLoom.Valkey`: standalone ValkeyDotNet 1.1.0 cache store, tag indexes, atomic
  value/TTL reads, owner-checked coordination leases, explicit Pub/Sub invalidation,
  connection recovery, DI integration and readiness. Ambiguous writes are never replayed.
  Invalidation gaps rely on L1 expiry; tracking, keyspace notifications and cluster
  routing are outside this first adapter. Includes conformance tests and a Native AOT sample.
- Live Valkey TLS/ACL and server-restart tests, with an isolated Docker runner and a required
  release-workflow step. Covers certificate and permission rejection, client/subscriber recovery,
  script reload, database selection, bounded L1 staleness and lost-lease ownership checks.
- Composition benchmark workloads for Append/Replace over populated collections, repeated
  discovery rules and singleton dependency graphs, with reference-machine performance evidence.

### Changed

- Composition runtime validation groups descriptors once per service, and Append/Replace avoid
  unused collision searches. Replacement reports removals and retains descriptor order in one
  pass, preserving validation before collection mutation and diagnostic provenance.
- The composition generator reuses discovery and matching results within each declaration
  compilation and indexes conflict/dependency lookups, reducing compiler time and allocation
  while preserving diagnostic order and semantic invalidation after edits.
- Generated composition factories share one immutable origin per rule across registrations,
  aliases and rejected candidates within each factory call, reducing plan-creation allocations.
- Mapping and AutoMapper comparison benchmarks now live in `HostLoom.Mapping.Benchmarks`, with
  dedicated `benchmark-mapping` and `benchmark-mapping-smoke` recipes.
- Redundant C# using directives are reported as warnings in Roslyn-based editors and
  Rider/ReSharper; existing unused directives have been removed.

### Fixed

- `AddHostLoomMapping` validates map lifetimes against the actual registered unkeyed dispatcher
  on repeated calls, preventing singleton lifetime checks from being bypassed or applied to a
  dispatcher whose retained lifetime is scoped.
- Mapping completeness analysis recognizes explicit `IMapper<TSource, TDestination>.Map`
  implementations and ignores unrelated methods. Inherited class and interface members are
  checked, overridden properties count once, and concrete implementations satisfy destination
  contracts returned through a base class or interface.
- Mapping completeness analysis excludes lambda and local-function bodies from the enclosing
  map's returns and assignments. Nested functions that capture the destination local report
  `HLM0005`; constructor assignments to inherited members are recognized. Packed-analyzer tests
  cover the completeness diagnostics with an isolated package cache.
- `TieredCache.GetOrCreateAsync` rechecks both cache tiers after acquiring the per-key guard,
  even when acquisition did not wait. A delayed store miss can no longer trigger a redundant
  factory call after another caller has already filled the cache.
- Circuit-breaker calls admitted before the circuit opened can no longer close it, extend its
  reset interval, or change the current half-open trial's verdict when they finish later.
- Circuit resets and rate-limit windows use monotonic `TimeProvider` timestamps, so system-clock
  corrections do not change their intervals. Custom test providers must advance timestamps as
  well as timers when simulating elapsed time.
- `TimeoutFilter` preserves caller cancellation even when a terminal filter ignores cancellation
  and returns normally, including when the timeout has also expired.
- `PipeContext.AddOrUpdatePayload` invokes the update factory for payload types implemented by
  the context itself. The factory may update that instance in place; returning a replacement or
  null throws `InvalidOperationException` instead of storing an unreachable payload.
- `InstrumentedFilter` counts overlapping downstream intervals once and synchronizes their
  accumulation, preserving the filter's own duration when downstream calls run concurrently.
- The WebSocket client ignores credential-refresh results from an ended reconnect cycle and
  preserves manual closes made by connected-state observers without resubscribing on a closing
  socket. JSON-v1 integer bounds and Base64 payload syntax are validated for outgoing and incoming
  frames; payload bytes remain opaque. The client is independently versioned; see its
  [changelog](clients/hostloom-websocket-client/CHANGELOG.md).

## [0.5.0] - 2026-09-05

Upgrading changes one backend contract: `IDistributedCacheStore.SetIfAbsentAsync` takes the
tag-index keys `SetAsync` already took. A custom store implementation gains the parameter, and a
caller that passed a `CancellationToken` positionally names it instead. Applications that use the
packaged stores are unaffected, and `LockOptions.MaxWait` becomes the hard bound it documented.
Each break is stated under **Changed** below.

Two packages are published for the first time: `HostLoom.Composition` and
`HostLoom.Composition.Testing`. The generator and its declaration-use analyzer ship inside
`HostLoom.Composition` as compiler-only analyzer assets, so an application needs one package
reference and gains no runtime dependency beyond
`Microsoft.Extensions.DependencyInjection.Abstractions`.

### Added

- Composition performance harness with separate creation/application/probe/ledger phases, handwritten
  and pinned Scrutor comparisons, 46/160/1,000-candidate incremental measurements and paired clean
  consumer builds. Published reference-machine distributions and explicit timing/allocation budgets
  include cold startup overhead; deterministic gate tests reject regressions and invalid evidence.
- Complete composition reference, migration and explanation guides, plus incremental regression tests
  for inherited attribute/interface, accessibility and rule changes with output restoration.
- `HostLoom.Composition.Testing`: container-free registration multiset/sequence, matched-type,
  service policy and provenance assertions, with explicit semantic identities for opaque activations.
- Bundled composition generator/analyzer and build-transitive provenance props in the application
  NuGet. An isolated packed-consumer verifier checks dependency boundaries, diagnostics, helper
  consumption and optional native execution; the release workflow verifies the packed artifacts.
- Application-owned composition ledger example aggregating enumerable implementations without
  false conflicts and retaining skip/replacement/rejection details.
- Composition rule semantics: inherited attribute filters, namespace guards, all-interface and self
  alias projections, implementation counts, explicit strategies and positional open-generic pairs.
  Diagnostics HLM0014–HLM0016 cover counts, generic shape and proven singleton capture. Probes retain
  selectors, rejected candidates and known alias targets. The native sample executes generated
  aliases, open generics and synchronous/asynchronous disposal.
- Initial composition source generator and declaration DSL: explicit type sets, compile-time
  class discovery with inherited generic matching, groups, service projection, explicit lifetimes
  and cardinality, diagnostics `HLM0009`–`HLM0013`, and reviewable source snapshots. The AOT sample
  now resolves generated services.
- `HostLoom.Composition` runtime foundation: immutable explicit DI plans, rejection provenance,
  passive application reports, explicit cardinality and registration strategies, and validation
  before collection mutation. An executable Native AOT sample exercises explicit plans, scoped
  aliases and known closed open-generic resolution.

### Changed

- **Breaking.** `IDistributedCacheStore.SetIfAbsentAsync` takes the tag-index keys `SetAsync`
  already took, so an entry written with `Tags` is reachable by `RemoveByTagAsync` in the
  distributed tier as well as the in-process one. A custom store implementation gains the
  parameter; a caller that passed a `CancellationToken` positionally names it instead.
  `LocalCacheStore.SetIfAbsent` gains the same `tags` parameter before its `size` one.
- `LockOptions.MaxWait` is the hard bound it documented. Provider calls run under it and are
  cancelled at it, so a backend that never answers can no longer stretch the wait; no attempt
  starts on or after the bound, which means acquisition can now end slightly before it rather than
  making one last attempt exactly on it. `TimeSpan.Zero` still makes exactly one attempt, bounded
  only by the caller's token.
- The distributed tag index is documented as monotonic: it gains members and loses them only when
  the whole index is removed, so an entry rewritten under different tags stays in its earlier
  indexes and `RemoveByTagAsync` may evict more than currently carries the tag. Over-eviction costs
  a refill; reading an entry's tags before every write would cost a round trip on the hot path.

### Fixed

- The TypeScript WebSocket client now enforces `maximumMessageSize` against encoded UTF-8 before a
  frame reaches the socket and reports a typed error without consuming request capacity.
  Acknowledgements made during reconnect are safe no-ops because old-session sequences cannot be
  applied to a replacement session; other invalid subscription states use a typed error. The client
  also sends one `unsubscribe` for an unowned `subscribed` or `event` stream while preserving
  subscriptions deliberately created through its low-level frame API.
- Calling the TypeScript client's `connect()` during a caller-requested close now queues one shared
  reconnect until the close event completes teardown instead of rejecting with a transient
  still-closing error. Protocol-failure closes remain terminal.
- A malformed or poisoned distributed-cache payload can no longer force a large allocation. The
  uncompressed length a compressed payload declares is believed only up to
  `Caching:MaxPayloadBytes`; beyond it the entry is a miss logged as corrupt. The same bound now
  applies to the serialized body when writing, not only to the encoded payload, so an entry this
  cache wrote always reads back.
- A lock lease is measured from the request rather than from the answer. A provider starts the
  lease when it accepts the call, so `LeaseEnd`, the local expiry timer, and `ExtendAsync` all
  count the round trip as spent lease, and `IsHeld` can no longer stay true after the backend's key
  expired and another instance took it.
- A full invalidation queue is reported. `BoundedChannelFullMode.DropWrite` drops the message and
  still reports the write as successful, so the warning that watched the return value never fired;
  drops now count on `hostloom.cache.invalidations` with direction `dropped` and log once per
  `Caching:Diagnostics:DegradedLogInterval`.
- A get-or-create factory that outlives the stampede lease no longer releases it. The release is an
  unconditional delete, so releasing an expired lease removed whichever instance held it next and
  let further factories run concurrently.
- The `IDistributedCache` adapter counts store failures on `hostloom.cache.errors` and logs one
  warning per key per `Caching:Diagnostics:DegradedLogInterval`, instead of one unthrottled warning
  per failed operation during an outage.

## [0.4.0] - 2026-09-03

Upgrading adds two analyzer rules — `HLM0007` and `HLM0008` — which report as warnings by
default. A project building with `TreatWarningsAsErrors` will fail until each is addressed or
its severity is set in `.editorconfig`; that is the intended effect, but it is a build break on
upgrade rather than a silent change.

The WebSocket gateway's identifiers and JSON frame spelling change in this release, so a 0.3.0
browser client, a Protocol Buffers consumer, or a caller of `IWebSocketSessionControl` must be
updated together with the server. Each break is stated under **Changed** below. Applications that
do not use `HostLoom.AspNetCore.WebSockets` are unaffected.

Ten packages are published for the first time: `HostLoom.Caching`,
`HostLoom.Caching.DependencyInjection`, `HostLoom.Caching.Testing`, `HostLoom.Caching.Pipelines`,
`HostLoom.Locking`, `HostLoom.Locking.DependencyInjection`, `HostLoom.Locking.Testing`,
`HostLoom.Locking.Pipelines`, `HostLoom.Redis`, and `HostLoom.AspNetCore.WebSockets.Testing`.

### Added

- The initial `@hostloom/websocket-client` package structure: dependency-free ESM output,
  discriminated JSON-v1 frame types, direction-aware encoding and decoding, validation of untrusted
  frames and safe integers, and Base64 UTF-8 JSON payload helpers. Its Node tests consume the
  gateway's canonical schema and exact fixtures, and its npm dry-run verifies the publishable
  package contents. An injectable connection core now validates subprotocol and welcome
  negotiation before reporting `connected`, exposes state and validated-frame observers, sends
  validated client frames, surfaces close details, and supports explicit close and manual
  reconnect. Its request API allocates non-reused session stream identifiers, correlates responses
  and typed remote faults, enforces the welcome-advertised concurrency limit, forwards gateway
  timeouts, maps `AbortSignal` to one cancel frame, and rejects every pending request on connection
  loss. Its subscription API shares non-reused stream allocation with requests, waits for a matching
  confirmation, buffers events within initial credit until a listener exists, automatically
  replenishes credit at a configurable low watermark, sends acknowledgements, maps cancellation to
  one `unsubscribe`, and reports terminal faults. An opt-in reconnect policy now applies jittered
  exponential backoff from one to 30 seconds, resets after a validated welcome, awaits credential
  refresh before retrying close code `1008`, and retains logical subscription handles and listeners
  across resubscription with fresh session state. Manual closes and protocol failures remain
  terminal, while pending requests fail and are never replayed. The package development gate pins a
  TypeScript 7-compatible ESLint 10/Babel configuration, Prettier, and Vitest, with one
  `npm run verify` command covering format, lint, compilation, tests, and package contents.
  Independent `websocket-client-vX.Y.Z` releases now validate the package version and client
  changelog, upload the exact tarball, and publish from a protected GitHub environment through npm
  trusted publishing with provenance. Pull requests that affect the client or its canonical
  protocol artifacts run the same package gate. The initial registry publication alone uses a
  removable bootstrap token because npm requires an existing package before it accepts an OIDC
  publisher. The package carries its own version and changelog and is not part of this release's
  NuGet set; it ships when a `websocket-client-vX.Y.Z` tag is published.
- Client-initiated `ping` and server `pong` frames in the version-one hub protocol, across the
  JSON, MessagePack, and Protocol Buffers codecs. The client picks a stream identifier and the
  gateway echoes it, so round-trip time stays unambiguous with several pings in flight. The session
  answers directly, without a dependency-injection scope, a registered operation, or a transport
  hop, and retains no ping state. A `ping` shares the existing control-frame rate window, a `ping`
  addressed to the session rather than a stream and a client-sent `pong` both return
  `invalid_frame`, and browsers gain the
  liveness signal that RFC 6455 Ping and Pong cannot give them. Existing clients are unaffected
  because a `pong` is only ever sent in reply to a `ping`.
- Same-origin WebSocket handshake validation by default, exact-origin allowlists, an explicit
  missing-Origin policy for browser-only versus native clients, and a replaceable
  `IWebSocketOriginValidator`. Validation uses ASP.NET Core's effective scheme and host and rejects
  before accepting the upgrade.
- `HostLoom.AspNetCore.WebSockets.Testing`, a protocol-aware `WebSocketTestClient` over ASP.NET Core
  TestServer for driving frames and awaiting common gateway responses in consumer integration tests.
- Credential-bounded WebSocket sessions driven by `TimeProvider`, with a 12-hour maximum lifetime,
  1008 `session_expired` closure, read-only `IWebSocketSessionDirectory` snapshots, and
  `IWebSocketSessionControl` for logout or role-change disconnects by session id or subject. Host
  shutdown now waits for sessions to receive 1001 `server_shutdown` before broker listeners stop,
  and per-session control-frame rate limiting closes floods with 1008 `rate_limited`.
- Key-aware WebSocket topic authorization: `WebSocketTopicResource.Key` exposes the requested
  subscription key to ASP.NET Core policies, and `TopicKeyPolicy.SubjectOnly` provides an
  authenticated, exact subject-to-key policy that rejects foreign or missing keys before
  subscription registration.
- Scoped `IWebSocketTopicSnapshotProvider<TEvent>` registration with deterministic
  `subscribed` → snapshot → concurrent-live ordering. Snapshot events reuse the v1 `event` frame
  with sequence zero, consume credit, support credit and unsubscribe during asynchronous loading,
  filter keyed snapshots, and reserve concurrent live frames against the existing connection
  bounds. Provider failures fault only the subscription with `snapshot_failed`.
- A machine-guarded WebSocket fan-out regression baseline for one serialized 256-byte payload sent
  through JSON envelope encoding, subscription credit, and ready-writer bounded queue cycles at 1,
  100, and 500 sessions, with explicit separation from real-socket capacity evidence.
- WebSocket gateway `System.Diagnostics.Metrics` for active sessions and subscriptions, successful
  and dropped event delivery, queued event-frame sizes, session duration, generated faults, and
  handler-level handshake rejection. Tags use only bounded protocol, topic, reason, and fault-code
  values and exclude session, subject, key, payload, credential, and caller-supplied text.
- Stable WebSocket structured-log event ids `4100`–`4106` for session lifecycle, subscription
  denial, slow-client abort, handler-level handshake rejection, operation failure, and snapshot
  failure. Unknown client topics, subscription keys, payloads, credentials, handshake headers,
  caller-supplied close text, and remote fault messages are excluded from framework properties.
- WebSocket request Server activities from the `HostLoom.AspNetCore.WebSockets` source, with
  registered operation, protocol, bounded outcome, and fault-code tags. They parent the existing
  HostLoom request activity without accepting unregistered client operation names as trace identity;
  cross-process broker trace propagation remains outside this increment.
- An immutable, execution-free WebSocket gateway probe available during registration and from
  dependency injection. It describes options and registered routes without resolving application
  services or contacting a transport, and exposes ledger-shaped `WebSockets:*` decisions without
  adding a dependency on the optional diagnostics package.
- A `UseHostLoomWebSockets(WebSocketOptions)` overload for caller-controlled ASP.NET Core WebSocket
  middleware settings. The parameterless helper retains its 20-second keep-alive interval and
  10-second Pong timeout; applications that already call `UseWebSockets` omit the HostLoom helper.
- Reverse-proxy deployment guidance covering HTTP/1.1 upgrade and subprotocol forwarding,
  authentication and origin preservation, trusted public scheme/host reconstruction, an idle
  timeout above 60 seconds for the default keep-alive, and the exact per-replica fan-out condition
  under which session affinity is unnecessary.
- An in-memory transport and WebSocket gateway how-to for the broker-free per-replica topology,
  including a browser verification path, reconnect and startup boundaries, operational checks, and
  explicit triggers for moving to RabbitMQ, Kafka, or a backplane.
- `HLM0007` and `HLM0008` in `HostLoom.Analyzers`, and coverage of the cache and lock contracts by
  `HLM0001` and `HLM0002`. `HLM0007` reports a cache or lock key built from a parameter, local,
  field, or property whose name says it is a credential (`token`, `secret`, `password`,
  `refreshToken`, `apiKey`) through interpolation, concatenation, or `string.Format`, `Concat`,
  or `Join`, unless the value is wrapped in `CacheKey.FromSensitive` or `LockKey.FromSensitive`:
  a key reaches the store, the logs, and the spans, and the helper hashes the secret so the key
  stays unique without carrying it. `HLM0008` reports a factory passed to
  `ICache.GetOrCreateAsync` that names its `CancellationToken` parameter and never uses it, so
  the work it starts would outlive the caller while holding the per-key guard; a `_` parameter is
  the deliberate opt-out. `HLM0001` (omitted cancellation token) and `HLM0002` (blocking on an
  asynchronous call) now recognise `ICache`, `IDistributedLock`, and `ILockHandle` by name, so
  they apply to those contracts whether the call goes through the package or a test stub.

- `HostLoom.Caching`, a two-tier cache kernel: the consumer contract `ICache` with get-or-create,
  a state-carrying overload that captures nothing on an in-process hit, `TryGetAsync` returning a
  `CacheLookup<T>` that distinguishes a cached default value from a miss, tags, bulk reads,
  set-if-absent, and warmup; the backend contract `IDistributedCacheStore` over opaque keys and
  byte payloads with a `CacheStoreException` and `CacheFailureKind` as the only failure shape;
  `ICacheValueSerializer` with a `System.Text.Json` implementation that resolves contracts from
  the injected `TypeInfoResolver` and never calls the reflection overloads; the in-process tier
  `LocalCacheStore`, the in-process second tier `InMemoryDistributedCacheStore` that also acts as
  an invalidation channel, and `TieredCache`, the composition. A distributed-store failure never
  reaches a consumer as an exception from a read or a get-or-create: the cache degrades to the
  factory, keeps the in-process tier, records a metric, and logs one warning per key per interval.
  Every kind of key lives in its own domain under `{namespace}:cache:`, so a consumer key cannot
  collide with a stampede lease or a tag index. The kernel references only
  `Microsoft.Extensions.Logging.Abstractions`, is `IsAotCompatible`, and composes with `new`.
- `HostLoom.Caching.DependencyInjection`: `AddHostLoomCaching` with a `CachingBuilder` that
  chooses exactly one store (`UseInMemory()` or `UseStore<TStore>(name)`, a second choice throws
  naming the first), the serializer (`UseSystemTextJson`, which requires a type-info resolver,
  `UseSerializer<T>`, and the annotated `UseReflectionSerialization` opt-out), warmups that run
  in the background after startup with a readiness contributor governed by
  `Caching:Warmup:BlocksReadiness`, and a readiness check that asks the store's health probe.
  Options are validated at startup, every message naming the option key.
- `HostLoom.Locking`, a distributed lock kernel: `IDistributedLock` with a `ValueTask`,
  token-aware execute and `TryAcquireAsync` returning an `ILockHandle` that exposes `IsHeld`,
  `LeaseEnd`, a `LostToken` cancelled when the lease is lost, and `ExtendAsync`; the backend
  contract `ILockProvider` with owner tokens and compare-and-set release and extend;
  `LockRetryPolicy` with immediate, interval, linear, and exponential shapes plus additive jitter,
  defaulting to ten linear retries at a 50 ms step; typed outcomes `LockNotAcquiredException`,
  `LockProviderUnavailableException`, and `LockReentrancyException`; `Locking:Enabled = false`
  as a visible single-instance mode; and `InMemoryLockProvider` with real lease expiry on a
  `TimeProvider`. The lock is coordination, not correctness, for persisted state, and its
  documentation says so.
- `HostLoom.Locking.DependencyInjection`: `AddHostLoomLocking` with a `LockingBuilder`
  (`UseInMemory()`, `UseProvider<TProvider>(name)`, `AddHealthChecks()`), startup validation,
  and the same exactly-one rule as caching.
- Meters and activity sources named `HostLoom.Caching` and `HostLoom.Locking`, with
  `hostloom.cache.*` and `hostloom.lock.*` instruments tagged by namespace, and execution-free
  `CachingProbe` and `LockingProbe` descriptions in the spirit of `HostLoomProbe`.
- `tests/HostLoom.Conformance`, backend-neutral cache and lock scenarios with a manual clock and
  fault-injecting decorators, run by the unit suite on the in-process backends both composed with
  `new` and through the container, and reusable by the integration suite on a real backend.
- `examples/HostLoom.Examples.CachingAot`, a Native AOT sample that publishes without trim or AOT
  warnings and executes a serialized cache round trip through a source-generated
  `JsonSerializerContext`.
- `HostLoom.Redis`, the Redis backend for caching and locking over StackExchange.Redis 3.1.31.
  `UseRedis()` on either builder registers `RedisOptions`, validates them at startup, and creates
  one connection per process lazily; calling it on both builders shares that connection.
  `RedisCacheStore` uses `SET … PX`, `GET` with `PTTL`, `MGET`, `UNLINK`, and `SET … NX PX`, and
  keeps tag indexes as sets that expire with their longest member; `RedisCacheInvalidationChannel`
  fans invalidations out over `{namespace}:cache:invalidate` and keeps subscribing with backoff
  while Redis is unreachable; `RedisLockProvider` acquires with `SET … NX PX` and releases and
  extends through Lua compare-and-set. `Redis:FailFast` is false by default: an unreachable Redis
  lets the host start, readiness reports unhealthy, the cache serves from its in-process tier and
  factories, and the lock raises `LockProviderUnavailableException`, until the connection recovers.
  `UseHashTags` wraps the namespace for Redis Cluster slot affinity; a password never reaches a
  log or probe line. Meter `HostLoom.Redis` with `hostloom.redis.connection.state` and
  `hostloom.redis.reconnects`.
- The Redis conformance run: `tests/HostLoom.IntegrationTests` executes every shared cache and
  lock scenario against the `redis:7.4` service that `docker-compose.yml` now provides, on the
  wall clock, composed with `new` and through the container, plus backend tests for the key
  layout, hash tags, readiness, cross-connection invalidation, and re-subscription after the
  server kills the pub/sub connection. The suite skips honestly without a listener and fails
  under `HOSTLOOM_REQUIRE_BROKERS=1`.
- Server-side invalidation on Redis. `Caching:Invalidation:Mode` now does what it says:
  `Tracking` registers `CLIENT TRACKING ON REDIRECT … NOLOOP` to the process's subscriber
  connection, so an entry any other client modifies, deletes, expires, or evicts leaves the
  in-process tier without anyone publishing; `Broadcast` subscribes to keyspace notifications for
  the namespace's entries or the configured prefix filters; `Auto` picks tracking on Redis 6.0 or
  later from the server version and broadcast below that. Tracking is registered again after a
  reconnect, both re-establishments count on `hostloom.cache.invalidation.resubscribed`, a mode
  that cannot be enabled after `Redis:MaxClientCommandRetries` attempts falls back to the explicit
  channel with one warning, and `CachingProbe` reports the transport in effect. The package
  applies the client-side `allowAdmin` flag and RESP2 to its connection, because StackExchange.Redis
  gates `CLIENT` commands behind the former and only keeps a dedicated subscriber connection under
  the latter. Proven against Redis 7.4: automatic selection, another connection's write, the
  connection's own write ignored, server-side expiry, re-registration after the server kills the
  connections, and a second instance's in-process entry evicted by an overwrite.
- `CachingBuilder.AddDistributedCacheAdapter()`: `IDistributedCache` and `IBufferDistributedCache`
  over the chosen distributed store, so `HybridCache` and other asynchronous Microsoft consumers
  share a HostLoom backend. Entries live under `{namespace}:cache:external:` apart from the tiered
  cache's own; the synchronous members throw `NotSupportedException`; `RefreshAsync` is a no-op
  because the store has no touch operation, so sliding windows become absolute; store failures
  answer as misses and log rather than throw. Proven with `HybridCache` reading through a second
  provider what the first wrote.
- `HostLoom.Caching.Testing` and `HostLoom.Locking.Testing`: `TestCache.InMemory()` and
  `TestCache.Tiered(store, serializer)`, `TestLock.Create()`, the `FaultingCacheStore` and
  `FaultingLockProvider` decorators that fail the next `n` calls or every call with a chosen kind,
  `RecordingCacheStore` and `RecordingLockProvider` that record every call, and
  `ManualLockProvider` whose `Hold` and `Release` script the key another instance would own. Both
  reference their kernel only and are `IsAotCompatible`; the conformance suite now builds on them.
- `HostLoom.Caching.Pipelines` and `HostLoom.Locking.Pipelines`, filters for `HostLoom.Pipelines`
  over the caching and locking kernels, with `HostLoom.Pipelines` itself staying dependency-free.
  `UseCache<TContext, TPayload>` is get-or-create around the rest of the pipe: a hit puts the
  cached payload on the context and stops, a miss runs the pipe and caches the payload it leaves
  behind, and a `CacheFilterResult` records which happened. `UseDeduplication` claims an identity
  with an atomic set-if-absent before running and adds a `Deduplicated` payload instead of running
  again inside the window; when the store cannot answer it runs anyway with a
  `DeduplicationSkipped` payload, because processing twice is recoverable and dropping on an
  outage is not. `UseDistributedLock` runs the rest of the pipe under the lock for a key derived
  from the context and leaves a `HeldLock` payload whose token is cancelled on a lost lease when
  the options ask for it. Each filter also has a public constructor over the kernel service plus a
  small options object, so `HostLoom.Pipelines.DependencyInjection` resolves it through
  `AddFilter`, and each describes itself to `PipelineProbe`. Deduplication on the messaging
  receive pipeline is deliberately not wired.
- Documentation: `docs/reference/caching.md`, `docs/reference/locking.md`,
  `docs/how-to/cache-and-lock-fail-open.md`, and a caching and locking section in the README.
- Cache and lock benchmarks: the tracked HostLoom scenarios cover an in-process hit through the
  state-carrying overload, a distributed hit through the serializer, a miss under 100-way
  contention, a bulk read of 100 keys, and lock acquire/release and execute. Process-local cache
  comparisons add Microsoft `HybridCache` and FusionCache. A separate real-Redis project compares
  all three cache L2 paths and HostLoom locks against Medallion `DistributedLock.Redis`, failing
  setup when Redis is unavailable. A committed environment-checked baseline and command fail when
  deterministic HostLoom cache or lock mean time or allocations regress by more than 10%.

### Changed

- **Breaking.** `HubFrame.StreamId`, `SessionId`, and `EventId` are `Guid` values rather than a
  `ulong` and two strings, across the JSON, MessagePack, and Protocol Buffers codecs. JSON spells
  an identifier as 32 lowercase hexadecimal digits without separators and rejects every other
  spelling; the binary codecs carry the 16 big-endian bytes of RFC 4122, so all three subprotocols
  name one identifier. `Guid.Empty` addresses the session rather than a stream and is valid only on
  `welcome`. The Protocol Buffers contract changes `stream_id`, `session_id`, and `event_id` to
  `bytes` on their existing field numbers, so a 0.3.0 consumer misreads rather than fails cleanly
  and must be rebuilt from the new contract. `WebSocketSessionInfo.SessionId` and
  `IWebSocketSessionControl.DisconnectAsync` take a `Guid`, and `WebSocketTestClient` awaits a
  `Guid` stream. Clients now choose their own stream identifiers rather than allocating from a
  counter, so a stream is never reused within or across sessions and a late frame from a closed
  stream cannot be misrouted. Fan-out allocates more per event frame because a hexadecimal
  identifier is longer than a small integer; the committed fan-out baseline records the new cost.
- **Breaking.** The `hostloom.json.v1` WebSocket contract now matches its documented web defaults:
  camelCase frame kinds and omitted null optional fields, so a 0.3.0 JSON client that sends
  PascalCase kinds is rejected. It ships a JSON Schema and exact frame fixtures, reads kinds
  case-insensitively, rejects numeric or unknown kinds, and retains Base64 opaque application
  payloads. The published schema pins `welcome` to the reserved session stream, bounds every
  numeric field so that no schema-valid frame exceeds what the .NET codec and the browser client
  can read, and asserts the Base64 payload alphabet with a pattern rather than the advisory
  `contentEncoding` annotation alone.

### Fixed

- Gateway fan-out membership is reference-counted per session and topic/key pair, so unsubscribing
  one of several matching streams no longer silently stops its siblings.
- Live subscription credit replenishment no longer signals a completed snapshot initializer, which
  removes one caught `SemaphoreFullException` allocation per zero-to-positive refill.
- Browser subscriptions created synchronously by a `connected` state listener keep their stream
  routing, because reconnect restoration can no longer re-key and orphan them. Terminal
  `unsubscribe()` cleanup is idempotent, and non-string request operations reject through the
  returned promise rather than throwing synchronously.

## [0.3.0] - 2026-08-29

Upgrading adds three analyzer rules — `HLM0004`, `HLM0005`, and `HLM0006` — which report
as warnings by default. A project building with `TreatWarningsAsErrors` will fail until each
is addressed or its severity is set in `.editorconfig`; that is the intended effect, but it
is a build break on upgrade rather than a silent change.

### Added

- `HostLoom.Mapping.Testing`, composing an `IMapper` dispatcher from explicit maps with no
  container, so a unit test does not have to build an `IServiceCollection` to obtain one. Maps are
  added as instances or as inline delegates. Duplicate pairs are rejected exactly as
  `MappingBuilder` rejects them, so a test cannot pass against a composition the container would
  refuse, and `Build` takes a snapshot so a dispatcher already handed to a test is unaffected by
  later additions. Substituting the dispatcher remains the worse option: it needs one substitute per
  pair, and each returns what the test told it to rather than what a map would.
- `MappedPairRegistry` and `IServiceCollection.GetMappedPairs()`, exposing the registered source and
  destination pairs so a service can assert its expectations while the container is still being
  composed, rather than discovering a missing pair on the first code path that needs it. One
  registry spans repeated `AddHostLoomMapping` calls and every registration overload.
- `MappingNotFoundException` now reports the destinations the requested source type *is* registered
  to map to, through a new constructor overload and a `RegisteredDestinations` property. The near
  miss is usually the diagnosis — a destination named one letter differently, or the pair registered
  in the other direction — so listing them turns reading the message into the fix.
- `AddHostLoomMapping` takes the dispatcher's `ServiceLifetime`. Scoped remains the default and the
  safe choice. Singleton lets an `IHostedService` take the dispatcher directly, and then every map
  must be registered singleton too — anything else is rejected at registration, because a singleton
  dispatcher resolves from the root provider and would retain each disposable map for the life of
  the process. Injecting a closed `IMapper<TSource, TDestination>` is still preferable, provided
  that map's own graph is singleton-safe. A singleton dispatcher does not silence `HLM0006`, which
  reports the shape rather than the lifetime it was registered with.
- `HLM0004` and `HLM0005`, completeness analysis for explicit maps, closing the one axis on which
  an explicit map is weaker than the convention mapping it replaces: a destination member that is
  simply never assigned compiles, passes review, and ships as silent data loss. `HLM0004` reports
  members a map never assigns; a member supplied through the destination's constructor counts as
  assigned, so positional records need nothing extra. `HLM0005` reports a map whose body the
  analysis cannot read, so that no diagnostic always means checked rather than skipped — every
  `Map` implementation lands in verified, not verifiable, or not applicable, the last being a
  destination with no settable public instance members or one that is itself a sequence. Two body
  shapes are verified: a destination constructed and returned directly, and one local constructed,
  assigned into across any number of statements and branches, then returned. Conditional assignment
  counts as assigned, because the rule targets forgotten members rather than conditional ones. A
  map whose destination is a type parameter cannot have its members enumerated and is skipped
  silently; that blind spot is documented in the analyzer README rather than left to be discovered.
- `HLM0006`, reporting a type that takes the scoped `IMapper` dispatcher through its constructor
  and is then registered as a singleton or a hosted service. The failure this replaces is the
  asymmetric one: a captured scoped service throws at host build where scope validation is enabled,
  the generic host's default in Development, and succeeds where it is not — so it fails in the
  environment least like production and hides in the one that matters. Only the non-generic
  dispatcher is reported; the closed `IMapper<TSource, TDestination>` the rule points at is
  transient and never flagged.
- `UnmappedMembersAttribute` in `HostLoom.Mapping`, naming the destination members a map leaves
  unset on purpose. Naming each one rather than marking the map incomplete is what keeps a member
  added to the contract later from being excused along with the deliberate omissions.
- `MappingBuilder.Add<TSource, TDestination>(Func<IServiceProvider, IMapper<TSource, TDestination>>)`
  registers a pair through a factory, which is how a generic map class is closed. A map generic in
  more than its source and destination — `EntityMapper<TEntity, TModel, TTranslation>` implementing
  `IMapper<TEntity, TModel>` — cannot be registered as an open generic, because the container
  requires the open service type and open implementation type to have equal arity and this shape
  never does. Closing the map at the call site instead keeps every type argument visible to the
  compiler, so the registration needs no `MakeGenericType` and both packages keep their trimming
  and Native AOT analyzers clean. Called from a generic helper, one map class registers many pairs
  in both directions; each registration remains a single closed descriptor, so the registered pairs
  stay enumerable and duplicate detection still spans it. A factory that returns null is reported
  as the factory's fault rather than surfacing as `MappingNotFoundException`, which would blame the
  registration for a pair that is in fact registered.
- Sequence and null-tolerant mapping in the core package, as extension methods on the closed
  mapper: `MapMany` and `MapManyOrEmpty` return `IReadOnlyList<TDestination>` sized in one
  allocation when the source reports a count, `MapManyDeferred` maps lazily for scans too large to
  materialize, and `MapOrNull` maps one value through null. The null policy is carried by the
  method name instead of by configuration, so unlike a convention mapper's global switch every
  call site that depends on null tolerance stays greppable. `MapManyDeferred` validates its
  arguments eagerly and only the mapping is deferred, so a null argument is reported at the call
  that passed it rather than at the first enumeration. The `HostLoom.Mapping` README documents the
  three ways AutoMapper's defaults differ, verified against AutoMapper 14 — including that
  `AllowNullCollections = false` rewrites null collection *members* to empty during ordinary object
  mapping, not only top-level collection maps, which is the case most likely to change behaviour
  silently during a migration.
- `MappingBuilder.Add<TMapper>()` registers a map class from the pair it already declares, read
  from the single closed `IMapper<TSource, TDestination>` it implements. A registration no longer
  restates a type triple the class carries on its interface, and the registering file needs no
  `using` for the contracts being mapped between — only for the map class. Inference reads type
  metadata once per registration and composes nothing, so both packages keep their trimming and
  Native AOT analyzers clean and the map dispatch path stays free of reflection. A class that
  implements no pair, or more than one, fails at registration with every candidate pair named and
  points at `Add<TSource, TDestination, TMapper>()`, which remains for choosing a pair explicitly
  and for closing an open generic map. Duplicate detection spans both overloads, since an inferred
  registration produces the same closed service type as an explicit one.
- Mapping benchmarks against AutoMapper across four suites: one map in steady state for a flat
  eight-scalar contract and a nested one with a child object and child collection; a batch of 100
  and 1000; a scope-resolve-map unit of work through the container; and cold start through the
  first mapped object, where a convention mapper's expression compilation lands. The flat shape is
  deliberately AutoMapper's best case — names match on both sides, so it is pure convention with no
  `ForMember` — and AutoMapper's execution plans are compiled during setup so the steady-state
  suites measure per-call cost rather than the cost of getting there. `GlobalSetup` asserts that
  both libraries produce equivalent destination values, so an incomplete map on either side fails
  the run instead of being reported as a faster one. AutoMapper is referenced by the benchmark
  project only; nothing under `src/` depends on it and the project is not packable.

### Changed

- An inferred mapping pair is resolved once per map class and read from a static field afterwards,
  instead of walking `GetInterfaces()` on every registration. Registering four maps went from 111 ns
  and 680 B to 63 ns and 552 B — identical to restating the type triple, so choosing the shorter
  registration form no longer costs anything. Inference failures are stored rather than thrown from
  the static constructor, which would otherwise reach the caller as a `TypeInitializationException`
  wrapping the real diagnostic, and would cache that wrapping for every later attempt.
- Benchmark suites that settle an implementation choice rather than compare libraries:
  `MapManyStrategyBenchmarks` measured a span fast path over `T[]` and `List<T>` at 2% against the
  `IReadOnlyList<T>` indexing that ships, which does not pay for two extra type checks and a span
  over `List<T>`'s internals — the map calls are roughly 94% of the work at 1000 elements.
  `MappingLifetimeBenchmarks` established that the dispatcher's extra 24 B is the transient map
  class constructed per dispatch, and that registering a stateless map as a singleton returns its
  allocation to exactly that of an injected closed map.

### Fixed

- A singleton mapping dispatcher now requires every map to be a singleton, and rejects anything
  else at registration. A singleton dispatcher resolves from the root provider, which never goes
  out of scope: a disposable map, or any disposable in its graph, was retained for the life of the
  process instead of released per unit of work, and a scoped dependency reached through a transient
  map was captured. Both were invisible at the call site. The documentation claiming a closed
  transient map "carries no restriction" in a singleton was wrong for the same reason — such a map
  is promoted to a singleton, so a scoped service inside it reproduces the Development-throws,
  Production-succeeds asymmetry `HLM0006` exists to prevent. `HLM0006`'s wording is corrected to
  say so.
- The factory registration overload is documented as the exception rather than the rule for closing
  a generic map. `Add<TEntity, TModel, EntityMapper<TEntity, TModel, TTranslation>>()` already
  closes a constructed generic from a generic helper, and being an implementation type it stays
  covered by `ValidateOnBuild`, where a factory body is opaque until the map is first resolved.
  Factories are for construction the container cannot perform.
- `MappedPairRegistry.Pairs` returns a read-only wrapper rather than the backing list, which could
  be downcast and appended to with pairs the container would never resolve.
- Inferring a mapping pair no longer re-reads `Type.GenericTypeArguments`, which allocates a fresh
  array on every access and was read twice per registration when recording the pair. Registering
  four maps by inference had become 3.6× the explicit form at 392 ns and 1128 B; the source and
  destination are now cached alongside the pair, and both forms cost 107 ns and 808 B. Inference
  being free is the only thing that justifies preferring the shorter registration.
- RabbitMQ no longer replaces a connection that automatic recovery owns. Observing `IsOpen: false`
  during a broker drop used to dispose the connection and build a new one, which cancelled the
  recovery that would have restored the channels, queues, and consumers created on it — leaving
  every listener and subscription permanently dead and silent while publishing carried on against
  the replacement. Only an application-initiated close is now treated as final; a peer or library
  shutdown is left to recover. The visible trade is deliberate: a publish attempted during the
  outage now fails loudly and transiently instead of succeeding at the cost of the consumers.
  `TopologyRecoveryEnabled` is stated explicitly rather than relied on as a default, because the
  listeners depend on it. Keeping the connection makes a second case reachable that replacing it
  had hidden: the reply queue is server-named and exclusive, so recovery re-declares it under a new
  name while the recovered channel reports itself open — nothing re-declared the reply path, and
  the cached name would have addressed a queue that no longer existed, timing out every later
  request on a connection that looked healthy. The broker now follows the rename.
- A WebSocket `Cancel` frame no longer escapes as `ObjectDisposedException` when it races the
  request it cancels. The request could complete and dispose its own cancellation source between
  the lookup and the cancel, and unlike the shutdown path — which already guarded this exact race —
  the frame path did not, so the exception passed the session's graceful-close handling and left
  the connection through ASP.NET. A client sending request/cancel pairs could provoke it.
- Kafka classifies both ways a required header can be absent as a malformed envelope. A record
  produced without headers carries a null collection, and `Headers.GetLastBytes` throws
  `KeyNotFoundException` rather than returning null for a missing key — so the existing null-check
  never fired and neither case reached the malformed path. Both were treated as transient faults
  and cost the record its full redelivery and backoff budget before being discarded, where a
  malformed envelope is committed and skipped immediately.
- Pipeline startup validation constructs only the filters a run would construct. A filter switched
  off for the environment through `EnabledWhen` was still built by the validator, so a host refused
  to start over a filter that would never execute — defeating the switch precisely when its
  dependencies were absent, which is the case it exists for. The validator also wraps any
  constructor failure in its guidance now, not only the container's `InvalidOperationException`.
- Kafka now skips only records classified as malformed HostLoom envelopes; an application handler
  that throws `InvalidDataException` follows the configured broker redelivery policy instead of
  being committed immediately as poison data.
- Usage analyzers identify framework assemblies through generated assembly metadata, preventing a
  consumer such as `HostLoom.OrderService` from being analyzed as framework code while ensuring
  future packages under `src/` are included automatically.

## [0.2.0] - 2026-08-27

### Added

- `HostLoom.Diagnostics`, a composition ledger that records what each registration decided —
  which branch activated, why, and what was deliberately skipped — and reports the whole plan
  once at host start under the `HostLoom.Diagnostics.Composition` category: one `Information`
  manifest line for the composition, one `Debug` line per decision carrying its reason and the
  registration method that recorded it, and a `Warning` naming both choices whenever one
  component was recorded with choices that disagree. Collection is unconditional and passive, so
  a library can record without imposing anything: nothing is written until an application calls
  `AddCompositionDiagnostics`, and that opt-in may come after the decisions it reports. The
  package is a standalone leaf that nothing else depends on, so an application that does not
  reference it carries nothing.
- `HostLoom.Mapping` and `HostLoom.Mapping.DependencyInjection`, explicit object mapping that keeps
  renames, required members, constructor changes, conversions, and nullability visible to the C#
  compiler and to code review, instead of deferring them to startup or to production. A map is an
  ordinary class implementing the closed generic `IMapper<TSource, TDestination>` and is invoked
  directly: nothing scans assemblies, inspects members for mapping, compiles expressions, emits
  code, or dispatches from `object` to `Type`, so both projects opt into the .NET SDK trimming and
  Native AOT analyzers without reflection annotations on mapped members.
- `AddHostLoomMapping` registers one map class per source and destination pair, transient by
  default so a map can take scoped dependencies through its constructor, with an overload that
  registers a prebuilt stateless map as a singleton. The non-generic `IMapper` dispatcher is
  scoped, so a scoped dependency cannot be promoted through the normal registration path;
  orchestration code coordinating several pairs writes `mapper.From(source).To<Destination>()`
  through an allocation-free source wrapper. A duplicate pair fails at registration rather than
  resolving ambiguously, and a missing pair throws `MappingNotFoundException` naming both types.
  Mapping stays synchronous and performs no I/O, and this first release deliberately omits
  reverse-map inference, flattening conventions, lifecycle callbacks, runtime dictionaries,
  polymorphic `object` dispatch, and provider-specific projections — query projections remain
  explicit `IQueryable.Select` expressions so a provider still receives the whole expression tree.
- `HostLoom.Analyzers`, an opt-in Roslyn analyzer package that reports omitted available
  cancellation tokens on HostLoom async calls (`HLM0001`), synchronous blocking over HostLoom
  `Task` and `ValueTask` operations (`HLM0002`), and singleton dependency-injection registration
  of request handlers, event handlers, or request behaviors (`HLM0003`).
- Logging pipeline health metrics on the `HostLoom.Logging` meter: dropped records with reasons,
  queue depth, blocked enqueues and their duration, component failures, and writer state.
- A bounded logging shutdown deadline (`ShutdownTimeout`, default 5 seconds) and an optional
  bound on blocking enqueues (`EnqueueTimeout`), both counted rather than silent when they expire.
- A configurable `TimeProvider` for log timestamps, read per event on the calling thread.
- A deterministic field-name collision policy: duplicate names collapse by source precedence
  (event holes over scopes over enrichers over static fields, last occurrence wins within a
  source), user names beginning with `@` are escaped CLEF-style to `@@`, names a formatter
  reserves for its own schema are dropped rather than duplicated, and configurable name-length
  and per-record field caps apply — every dropped field counted in
  `hostloom.logging.fields.dropped`.

- Structured fields from the standard `ILogger` path: template holes from `LogInformation`-style
  calls, `LoggerMessage.Define`, source-generated logger methods, and custom key/value state are
  captured as typed fields with no call-site changes; `{OriginalFormat}` is preserved as the
  message template for template-aware formatters, and Serilog-style `@`/`$` operator prefixes
  are stripped from emitted names.
- Bounded `{@...}` destructuring: object graphs serialize into nested typed JSON on the calling
  thread, cycle-safe, with configurable caps on depth, collection items, object members, string
  length, and encoded bytes per record — every cap cut marked with an explicit sentinel, and any
  getter or serializer failure emitting `"[DestructuringFailed]"` instead of ever falling back
  to `ToString()`.
- Logging benchmarks against the deployed Serilog shape (`CompactJsonFormatter` on the calling
  thread): template with two scalar holes on the fast and interface paths, a destructured
  mid-size contract, an event enriched with eleven trace properties, a scoped event, a disabled
  level, and formatter-only throughput for the background half.
- Configuration binding for the logging provider: an `AddHostLoomLogging` overload binds
  `HostLoomLoggerOptions` from a configuration section (canonically `HostLoom:Logging`), with
  the code callback applying after configuration and unknown or invalid keys failing at host
  startup. Level filtering remains standard MEL `Logging` configuration.
- `HostLoomBootstrapLogger`, a synchronous pre-DI logger emitting the same event shape as the
  hosted provider — same formatter, typed fields, masking policy, timestamps, static fields,
  and enrichers — written to stdout on the calling thread with a construction-time minimum
  level, explicit flush/disposal, and failures swallowed unless fail-fast is opted into.
- A `ClefLogFormatter` emitting Compact Log Event Format shaped after Serilog's
  `CompactJsonFormatter`: `@t`, `@mt` when a template exists (`@m` only when none does), `@r`
  renderings for formatted fast-path holes, `@l` omitted for Information with Serilog level
  names otherwise, `@x` as the complete exception chain, `@tr`/`@sp`, `SourceContext`,
  `ThreadId`, `EventId` in the Serilog provider's property shape, and every captured field as a
  top-level typed property. `@i` is deliberately not emitted.
- Full exception chains in both formatters: `Exception.ToString()` semantics (inner exceptions
  and aggregate children included), bounded by a configurable cap with an explicit truncation
  marker, and guarded so an exception whose own `ToString` throws cannot fault the writer.
- MEL scope support: the provider implements `ISupportExternalScope` (with a standalone fallback
  so `BeginScope` works without a logger factory), scopes are snapshotted on the calling thread,
  structured scope pairs flatten into typed fields where inner scopes override outer ones and
  event holes override both, and templated or non-structured scopes keep their rendered text in
  a `Scope` array in outer-to-inner order. A throwing scope is counted, never thrown.
- Producer-side enrichment: `ILogEnricher` implementations registered on the options run on the
  calling thread before queueing — where `AsyncLocal` ambient context is still visible — writing
  typed fields through `LogEntryWriter` (no raw JSON injection possible). Enrichers run in
  registration order, a throwing enricher is counted and skipped without costing the event, and
  event holes outrank enricher fields.
- Static enrichment fields: `Environment.MachineName` attached by default (Serilog
  `WithMachineName` parity, opt-out via `AttachMachineName`) and a configurable `ServiceName`,
  both UTF-8-encoded once at provider start and carrying the lowest collision precedence.
- Fail-closed PII protection: `[NotLogged]` omits a property or field entirely at every nesting
  level (never read, including inherited members), `[LogMasked]` replaces or deterministically
  part-reveals values, a registration-time per-type policy covers unannotatable types, and
  legacy `Destructurama.Attributed` annotations are honored by name without the dependency.
  Events with `@` holes render their message from the protected representations, so a record
  type's generated `ToString()` cannot leak excluded members through the message text.

### Changed

- Structured log fields keep their JSON value kinds: numeric and boolean interpolation holes are
  emitted as JSON numbers and booleans, a formatted hole keeps its rendering in the message while
  the field stays typed, and `DateTimeOffset` holes default to ISO-8601.
- `LogFast` through a wrapped (dependency-injected) logger now hands structured key/value state
  to the standard interface, so captured hole names survive into any structured provider instead
  of being flattened away with the rendered string.
- `ILogSink.Write` now receives a `CancellationToken` so shutdown can abandon a stalled sink.
- An unexpected formatter or sink failure faults the logging pipeline instead of silently killing
  the writer: queued and later records are counted as dropped, and no caller can block on a dead
  writer.
- Log timestamps follow operating-system clock corrections instead of drifting from a Stopwatch
  anchor captured at startup.

### Fixed

- Destructuring a `{@...}` hole no longer allocates a fresh buffer and `Utf8JsonWriter` per
  event: the writer demands multi-kilobyte chunks from its buffer writer, which had cost about
  5 KB of garbage per destructured event. Thread-local pooled scratch (with a reentrancy guard
  and a retention cap) cuts that to the unavoidable reflection boxing — roughly 15× less
  allocation and measurably faster than Serilog's capture on the same contract.

- A stray `OperationCanceledException` from a formatter or sink faults the logging pipeline
  instead of silently stopping the writer while producers still see it as running.
- Sink disposal is bounded by the logging shutdown timeout, so a sink that hangs inside its own
  flush-on-dispose cannot hang application shutdown.
- The logging writer runs on a dedicated background thread, so a stalled synchronous sink write
  no longer occupies a thread-pool worker.
- Records formatted but not yet accepted by the sink are counted when the pipeline faults or
  shutdown abandons the writer.
- Logging options are validated at provider construction: queue capacity, batch size, queue-full
  policy, time provider, and both timeouts fail fast with actionable errors.

## [0.1.0] - 2026-08-25

### Added

- A transport-neutral request/response runtime with typed contracts, behaviors, scoped handlers,
  correlation, remote faults, JSON wire envelopes, host lifecycle integration, diagnostics, health
  checks, and an execution-free pipeline probe.
- Typed event publishing and named subscriptions, with fan-out semantics implemented by the
  in-memory, RabbitMQ, and Kafka transports.
- A composable asynchronous pipeline package with conditional branches, retry, circuit breaking,
  rate and concurrency limits, cooperative timeouts, short-circuits, immutable probes, metrics,
  and tracing.
- Dependency-injection pipeline registration with named stages, private transient filter
  registrations, per-run scopes and feature toggles, startup validation, topology reporting, and
  per-filter instrumentation.
- Deterministic pipeline testing helpers and harnesses, including asynchronous validation and
  disposal support, plus a runnable document-indexing example.
- An authenticated raw WebSocket RPC and live-subscription gateway with JSON, MessagePack, and
  Protocol Buffers subprotocols, bounded per-connection memory, and explicit subscription credit.
- Allocation-free UTF-8 logging compatible with `Microsoft.Extensions.Logging`, with bounded
  buffering, drop accounting, trace capture, and benchmarks.

### Compatibility

- Targets .NET 10 and C# 14. This is the first public release; the API is experimental and may
  change before 1.0.
- RabbitMQ and Kafka are optional transport packages. Core pipelines and the in-memory transport
  do not require an external broker.

[Unreleased]: https://github.com/kidoz/hostloom/compare/v0.9.0...HEAD
[0.9.0]: https://github.com/kidoz/hostloom/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/kidoz/hostloom/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/kidoz/hostloom/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/kidoz/hostloom/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/kidoz/hostloom/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/kidoz/hostloom/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/kidoz/hostloom/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/kidoz/hostloom/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/kidoz/hostloom/releases/tag/v0.1.0
