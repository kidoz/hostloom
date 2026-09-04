# `@hostloom/websocket-client`

Dependency-free ESM browser primitives for the HostLoom `hostloom.json.v1` WebSocket protocol.

This package currently provides:

- discriminated TypeScript types for every version-one frame;
- direction-aware client encoding and server decoding;
- runtime validation of untrusted server frames;
- Base64 application-JSON payload helpers;
- an injectable `HostLoomConnection` that negotiates JSON-v1, waits for a valid `welcome`, exposes
  connection-state and server-frame observers, supports explicit close and manual reconnect, and
  can opt into jittered exponential reconnect;
- a request API with automatic stream identifiers, response correlation, typed remote faults,
  welcome-advertised concurrency and message-size enforcement, gateway timeouts, and `AbortSignal`
  cancellation;
- a subscription API with shared stream allocation, confirmation gating, bounded pre-listener event
  buffering, automatic low-watermark credit replenishment, acknowledgements, typed terminal faults,
  `AbortSignal` unsubscription, and automatic resubscription on a replacement socket;
- client `ping` and server `pong` frames for application-level liveness and round-trip time;
- conformance tests against the schema and exact fixtures shipped by
  `HostLoom.AspNetCore.WebSockets`.

## Install

After the first package release:

```text
npm install @hostloom/websocket-client
```

The published package has no runtime dependencies. TypeScript is used only to build its ESM and
declaration output.

## Connect and exchange frames

```typescript
import {
    decodeJsonPayload,
    encodeJsonPayload,
    HostLoomConnection,
} from "@hostloom/websocket-client";

const connection = new HostLoomConnection("wss://inventory.example.com/realtime", {
    reconnect: {
        // Required only when the gateway uses 1008 to report an expired session.
        refreshCredentials: async () => {
            await fetch("/api/session/refresh", { method: "POST" });
        },
    },
});

connection.onStateChange(({ state, close }) => {
    console.log(state, close?.code);
});

const welcome = await connection.connect();
console.log(welcome.maximumMessageSize);

const responsePayload = await connection.request(
    "inventory.get",
    encodeJsonPayload({ itemId: "item-42" }),
    { timeoutMilliseconds: 3_000 },
);
const response = decodeJsonPayload<{ itemId: string; available: number }>(responsePayload);
console.log(response.available);

const subscription = await connection.subscribe("inventory.level.changed", {
    key: "item-42",
    credit: 32,
});

subscription.onEvent((event) => {
    const value = decodeJsonPayload<{ itemId: string; available: number }>(event.payload);
    console.log(value);
});

// When this view no longer needs updates:
await subscription.unsubscribe();
```

`connect()` is idempotent while an attempt is in progress and reports `connected` only after the
server selects `hostloom.json.v1` and sends a valid `welcome`. Without the `reconnect` option, a
close returns the connection to `disconnected`; call `connect()` again when the application chooses
to retry. When `connect()` is called while a caller-requested close is still in progress, repeated
calls share one promise: the client waits for the close event before opening exactly one replacement
socket. A close caused by a protocol failure remains terminal and rejects `connect()` until teardown
finishes.

Providing `reconnect` enables automatic retry after unexpected connection loss. The delay starts at
1 second, doubles after each failed attempt, is capped at 30 seconds, and applies ±20% jitter by
default. Each value is configurable. A valid `welcome` resets the delay. Calling `close()` or
encountering a protocol error is terminal and never retries. Close code `1008` is also terminal
unless `refreshCredentials` is provided; the client awaits that callback before creating the next
socket. A rejected refresh ends reconnection. Browser handshake failures remain generic
`HostLoomConnectionError` values because browsers do not expose the HTTP response details.

`request()` allocates a new stream identifier for each request and resolves with the opaque Base64
response payload. It rejects with `HostLoomRemoteFaultError` when the gateway sends a fault, and
with `HostLoomRequestCapacityError` before sending when the welcome-advertised concurrency limit is
already occupied. Every client frame is measured as encoded UTF-8 before it reaches the socket;
`HostLoomMessageSizeError` reports the actual and advertised maximum sizes when it is too large.
Pass an `AbortSignal` to send one `cancel` frame and reject locally with
`HostLoomRequestCanceledError`; the slot remains reserved until the gateway sends the terminal
response or fault because the request is still active on the server until then.

`subscribe()` shares stream allocation with `request()` and resolves only
after the matching `subscribed` frame. Events received before the first `onEvent` listener are
buffered within the initial credit; credit does not replenish until a listener exists. After that,
the client restores the requested credit whenever the remaining amount reaches `lowWatermark`
(half the initial credit by default). `unsubscribe()` sends one frame and resolves after the
gateway replies `complete`. An `AbortSignal` follows the same wire shutdown; aborting before
confirmation rejects the pending call with `HostLoomSubscriptionCanceledError`.

Several streams may subscribe to the same `(topic, key)` pair. They remain independent: each
receives its own event frame and unsubscribing one leaves its siblings registered. `unsubscribe()`
is idempotent after a subscription has already reached `closed`, including after a remote fault or
connection loss. An unsubscribe that was already waiting for `complete` still rejects if a terminal
failure interrupts that in-flight operation.

`acknowledge(sequence)` records positive live-event progress in the current gateway session; it
does not enable replay. An acknowledgement made while the subscription is `reconnecting` is a
no-op because an old session's sequence cannot be applied to its replacement. Other invalid
lifecycle states throw `HostLoomSubscriptionStateError` with the current public state. With
automatic reconnect enabled, a logical subscription and its listeners enter `reconnecting`,
discard buffered events and session credit, and resubscribe with a new session-scoped stream
identifier after the replacement socket receives `welcome`. It becomes `active` only after the
matching `subscribed` frame. Missed events are not replayed, so applications must still use their
snapshot/version contract; a gateway snapshot provider may supply a fresh sequence-zero snapshot
during resubscription. Pending requests fail on connection loss and are never replayed, avoiding
duplicate side effects. Without automatic reconnect, connection loss ends subscriptions as before.

The low-level `send()` API remains available; callers using it own their stream identifiers and
must keep them distinct from automatically allocated request and subscription streams;
`newStreamId()` is exported for that purpose. The connection tracks a low-level `subscribe` until
its terminal `complete` or `fault`, so its server frames remain caller-owned and visible through
`onFrame`. A `subscribed` or `event` frame owned by neither API is treated as an orphan: the client
sends exactly one `unsubscribe` for that stream so a desynchronized gateway subscription cannot
leak for the session lifetime.

Tests can supply `webSocketFactory` in the constructor options. The returned object implements the
small exported `HostLoomWebSocket` interface, so lifecycle behavior is testable without a browser
or network listener. `streamIdFactory` replaces the random allocator, which makes stream routing
deterministic in a test and lets an application derive a stream from its own trace identifier.

The payload helper assumes the application's HostLoom serializer produces UTF-8 JSON. Do not use it
when the application serializer produces another byte representation; frame encode/decode still
works with an opaque Base64 payload.

`streamId`, `sessionId`, and `eventId` are identifiers, not numbers: each is a string of 32
lowercase hexadecimal digits, and the decoder rejects any other spelling, including the dashed and
uppercase forms. The all-zero identifier, exported as `HOSTLOOM_SESSION_STREAM`, addresses the
session itself and is valid only on `welcome`.

JavaScript cannot represent every .NET 64-bit integer exactly. The decoder therefore rejects
`sequence` and numeric limit values outside JavaScript's safe-integer range rather than silently
rounding ordering values.

## Develop

From this directory:

```text
npm ci
npm run verify
```

`verify` checks Prettier formatting, runs ESLint, compiles with TypeScript 7, runs the Vitest suite,
and performs an npm package dry-run. ESLint uses Babel's TypeScript parser because the stable
`typescript-eslint` release does not yet declare TypeScript 7 compatibility; `tsc` remains the
type-aware symbol and unused-code gate. Development requires Node 22.18 or a compatible newer
release, while the published browser package retains no runtime dependencies.

The Vitest tests load the canonical schema and fixtures directly from
`src/HostLoom.AspNetCore.WebSockets/protocol`, so changes to either implementation must remain
compatible with the same files. They use the injectable fake WebSocket boundary rather than a DOM
emulator for deterministic protocol and lifecycle coverage.

## Release

The package has an independent `websocket-client-vX.Y.Z` tag stream. Before publishing, set the
same strict semantic version in `package.json` and add its section to this package's `CHANGELOG.md`.
Publishing a GitHub release for that tag runs the package gate, uploads the exact `.tgz` artifact,
and publishes that artifact through the protected `npm` environment. The ordinary .NET `vX.Y.Z`
release stream does not publish this package.

For the first publication only, add a short-lived granular npm token with bypass-2FA permission as
the `NPM_BOOTSTRAP_TOKEN` environment secret. npm cannot configure a trusted publisher until the
package exists. After that first publication:

1. Configure the package's npm trusted publisher for repository `kidoz/hostloom`, workflow
   `websocket-client-release.yml`, environment `npm`, and the `npm publish` action.
2. Delete `NPM_BOOTSTRAP_TOKEN` from GitHub.
3. Require two-factor authentication and disallow token publication in the npm package settings.

Subsequent releases use GitHub OIDC without a long-lived write credential and publish provenance
automatically. A manual run packages and uploads the current version without publishing by default;
select a `websocket-client-vX.Y.Z` tag and explicitly enable its `publish` input only for controlled
recovery or the bootstrap release.
