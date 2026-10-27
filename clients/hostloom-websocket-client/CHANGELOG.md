# Changelog

All notable changes to `@hostloom/websocket-client` are documented in this file. The package uses
independent [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-10-27

### Added

- The `maximumConcurrentRequests` connection option, 8 by default, bounds active requests locally.
  A welcome can lower the effective limit but cannot raise it above this value.
- The `maximumCreditPerSubscription` connection option, 1,024 by default, bounds each
  subscription's credit locally. It applies to the initial subscription and to resubscription
  after a reconnect; a welcome can lower the effective limit but cannot raise it.

### Changed

- Request concurrency is the smaller of the local option and the welcome's advertised limit, and
  subscription credit is the smaller of the local option and the welcome's advertised credit.
  Applications that relied on a gateway advertising more than 8 concurrent requests or more than
  1,024 credit must raise the corresponding option; otherwise `request()` rejects with
  `HostLoomRequestCapacityError` and `subscribe()` rejects with `RangeError`.
- Both options must be integers from 1 to 2147483647; any other value throws `RangeError` when the
  connection is constructed.

## [0.3.1] - 2026-10-26

### Added

- The `welcomeTimeoutMilliseconds` connection option, 10,000 ms by default, bounds each attempt to
  open the socket and receive its `welcome`, including a silent initial socket and replacement
  attempts during recovery. An attempt that misses the deadline closes the connection with code
  `1006` and reason `welcome_timeout`.
- The `closeTimeoutMilliseconds` connection option, 5,000 ms by default, bounds close teardown. A
  socket that never reports a close is detached, pending operations settle locally with code `1006`
  and reason `close_timeout`, and a queued manual connect resumes.

### Changed

- Automatic resubscription draws on the control-frame budget, so a large subscription set recovers
  over multiple budget windows, and unsubscribing while a restart is queued discards it before a
  subscribe frame is sent.
- Malformed JSON-v1 fixtures are shared with the server tests, and development dependencies are
  updated to clear the dependency audit.

### Fixed

- Server frames whose kind conflicts with the ownership of a known request or subscription stream
  are rejected as protocol failures before orphan subscription cleanup.
- `decodeJsonPayload` enforces the frame decoder's Base64 syntax, rejecting missing padding and
  whitespace.
- Callbacks captured from a replaced WebSocket are ignored once the replacement takes ownership.

## [0.3.0] - 2026-09-25

### Added

- The `maximumControlFramesPerSecond` connection option, 50 by default to match the gateway's
  default control-frame budget.

### Changed

- Credit and acknowledgement frames are sent after the current task and coalesced per
  subscription. `acknowledge()` skips sequences already covered, and a failed send fails the
  subscription instead of throwing from `acknowledge()`.

### Fixed

- Automatic credit and acknowledgements no longer get a connection closed with 1008
  `rate_limited`. They are paced by a connection-wide budget shared with every control frame, at
  most half the gateway's limit in any second (25 by default: a burst of 5, then 20 per second). A
  credit-2 subscription on a busy topic used to send one frame per event.
- A `close()` during an earlier close rejects the `connect()` queued behind it with
  `HostLoomConnectionClosedError` instead of opening a socket nobody owns.

## [0.2.0] - 2026-09-20

### Changed

- Automatic reconnect starts only after a session that received its `welcome` is lost. A first
  `connect()` that never reaches a welcome now rejects and leaves the connection `disconnected`
  instead of also retrying in the background; attempts that fail inside a running retry cycle
  still keep it alive. The `refreshCredentials` documentation states that the callback runs for
  every `1008` close and can branch on `close.reason`.

## [0.1.0] - 2026-09-03

### Added

- Dependency-free ESM output with TypeScript declarations.
- Strict `hostloom.json.v1` frame encoding, decoding, and shared fixture conformance. `streamId`,
  `sessionId`, and `eventId` are identifiers of 32 lowercase hexadecimal digits; the dashed and
  uppercase spellings are rejected. `newStreamId()` allocates one, `HOSTLOOM_SESSION_STREAM` names
  the all-zero identifier that only `welcome` may carry, and `streamIdFactory` replaces the random
  allocator when an application or test supplies its own.
- Validated connection negotiation, correlated requests, cancellation, and typed remote faults.
- Credit-managed subscriptions, acknowledgements, unsubscription, and terminal fault handling.
- Opt-in jittered exponential reconnect, credential refresh for expired sessions, and logical
  subscription resubscription without request or event replay.
- Transition-safe subscription routing, idempotent terminal cleanup, non-string request validation,
  and independent duplicate topic/key subscription lifetimes.

### Fixed

- Ignore credential-refresh results after their reconnect cycle has ended, and preserve manual
  closes made by connected-state observers without resubscribing on a closing socket.
- Enforce the JSON-v1 integer bounds and Base64 payload syntax before sending client frames or
  delivering server frames. Payload bytes remain opaque and need not contain JSON.
- Enforce the welcome-advertised UTF-8 message-size limit before sending, with a typed error that
  reports both sizes. Acknowledgements are safe no-ops during reconnect and invalid lifecycle uses
  now throw a typed subscription-state error. Unowned subscription frames trigger one cleanup
  `unsubscribe`, while subscriptions created through the low-level API remain caller-owned.
- `connect()` called during a caller-requested close now waits for the close event and opens one
  replacement socket; repeated calls share the same pending promise. Protocol-failure closes remain
  terminal.
