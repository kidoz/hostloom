# Changelog

All notable changes to `@hostloom/websocket-client` are documented in this file. The package uses
independent [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Enforce the welcome-advertised UTF-8 message-size limit before sending, with a typed error that
  reports both sizes. Acknowledgements are safe no-ops during reconnect and invalid lifecycle uses
  now throw a typed subscription-state error. Unowned subscription frames trigger one cleanup
  `unsubscribe`, while subscriptions created through the low-level API remain caller-owned.
- `connect()` called during a caller-requested close now waits for the close event and opens one
  replacement socket; repeated calls share the same pending promise. Protocol-failure closes remain
  terminal.

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
