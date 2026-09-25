# Changelog

All notable changes to `@hostloom/websocket-client` are documented in this file. The package uses
independent [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
