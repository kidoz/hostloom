import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "vitest";

import {
    HOSTLOOM_JSON_V1_SUBPROTOCOL,
    HostLoomConnection,
    HostLoomConnectionClosedError,
    HostLoomConnectionError,
    HostLoomMessageSizeError,
    HostLoomProtocolError,
    HostLoomRemoteFaultError,
    HostLoomRequestCanceledError,
    HostLoomRequestCapacityError,
    type HostLoomConnectionState,
    type HostLoomConnectionStateChange,
    type HostLoomWebSocket,
    type HostLoomWebSocketEventMap,
    type ServerFrame,
    type WelcomeFrame,
} from "../dist/index.js";

type EventListener = (event: never) => void;
type PostWelcomeFrame = Exclude<ServerFrame, WelcomeFrame>;

interface CloseCall {
    readonly code: number | undefined;
    readonly reason: string | undefined;
}

const packageDirectory = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const welcomeJson = (
    await readFile(
        resolve(
            packageDirectory,
            "../../src/HostLoom.AspNetCore.WebSockets/protocol/fixtures/json-v1/welcome.json",
        ),
        "utf8",
    )
).trim();

class FakeWebSocket implements HostLoomWebSocket {
    public protocol = "";
    public readonly sent: string[] = [];
    public readonly closeCalls: CloseCall[] = [];
    public closeError: unknown;
    readonly #listeners = new Map<keyof HostLoomWebSocketEventMap, Set<EventListener>>();

    public send(data: string): void {
        this.sent.push(data);
    }

    public close(code?: number, reason?: string): void {
        if (this.closeError !== undefined) {
            throw this.closeError;
        }

        this.closeCalls.push({ code, reason });
    }

    public addEventListener<TKey extends keyof HostLoomWebSocketEventMap>(
        type: TKey,
        listener: (event: HostLoomWebSocketEventMap[TKey]) => void,
    ): void {
        const listeners = this.#listeners.get(type) ?? new Set<EventListener>();
        listeners.add(listener);
        this.#listeners.set(type, listeners);
    }

    public removeEventListener<TKey extends keyof HostLoomWebSocketEventMap>(
        type: TKey,
        listener: (event: HostLoomWebSocketEventMap[TKey]) => void,
    ): void {
        this.#listeners.get(type)?.delete(listener);
    }

    public open(protocol = HOSTLOOM_JSON_V1_SUBPROTOCOL): void {
        this.protocol = protocol;
        this.#emit("open", {} as Event);
    }

    public message(data: unknown): void {
        this.#emit("message", { data } as MessageEvent<unknown>);
    }

    public error(): void {
        this.#emit("error", {} as Event);
    }

    public closed(code: number, reason = "", wasClean = true): void {
        this.#emit("close", { code, reason, wasClean } as CloseEvent);
    }

    #emit<TKey extends keyof HostLoomWebSocketEventMap>(
        type: TKey,
        event: HostLoomWebSocketEventMap[TKey],
    ): void {
        for (const listener of [...(this.#listeners.get(type) ?? [])]) {
            (listener as (event: HostLoomWebSocketEventMap[TKey]) => void)(event);
        }
    }
}

function createHarness(socket = new FakeWebSocket()) {
    const calls: { url: string | URL; protocols: string[] }[] = [];
    const connection = new HostLoomConnection("wss://inventory.example.com/realtime", {
        webSocketFactory: (url, protocols) => {
            calls.push({ url, protocols: [...protocols] });
            return socket;
        },
    });

    return { calls, connection, socket };
}

async function connectHarness(
    connection: HostLoomConnection,
    socket: FakeWebSocket,
    welcome = welcomeJson,
): Promise<WelcomeFrame> {
    const pending = connection.connect();
    socket.open();
    socket.message(welcome);
    return pending;
}

function welcomeWithRequestLimit(limit: number): string {
    return JSON.stringify({
        ...(JSON.parse(welcomeJson) as WelcomeFrame),
        maximumConcurrentRequests: limit,
    });
}

function welcomeWithMessageSize(maximumMessageSize: number): string {
    return JSON.stringify({
        ...(JSON.parse(welcomeJson) as WelcomeFrame),
        maximumMessageSize,
    });
}

/** A stream the caller picks itself, rather than one the connection allocated. */
const CALLER_STREAM = "99999999999999999999999999999999";

/** Reads the identifier the connection allocated for the frame it sent at `index`. */
function sentStream(socket: FakeWebSocket, index: number): string {
    return (JSON.parse(sentFrame(socket, index)) as { readonly streamId: string }).streamId;
}

/** Reads one sent frame positionally, so an index mistake fails the test rather than the types. */
function sentFrame(socket: FakeWebSocket, index: number): string {
    const frame = socket.sent.at(index);
    if (frame === undefined) {
        throw new Error(`The fake socket has not sent a frame at index ${index}.`);
    }

    return frame;
}

function nextSocket(sockets: FakeWebSocket[]): FakeWebSocket {
    const socket = sockets.shift();
    if (socket === undefined) {
        throw new Error("The test requested more sockets than the harness provides.");
    }

    return socket;
}

test("connect negotiates JSON-v1 and waits for a valid welcome frame", async () => {
    const { calls, connection, socket } = createHarness();
    const changes: HostLoomConnectionStateChange[] = [];
    connection.onStateChange((change) => changes.push(change));

    const pending = connection.connect();

    assert.equal(connection.state, "connecting");
    assert.equal(connection.connect(), pending);
    assert.deepEqual(calls, [
        {
            url: "wss://inventory.example.com/realtime",
            protocols: [HOSTLOOM_JSON_V1_SUBPROTOCOL],
        },
    ]);

    socket.open();
    assert.equal(connection.state, "connecting");
    socket.message(welcomeJson);

    const welcome = await pending;
    assert.equal(welcome.kind, "welcome");
    assert.equal(connection.welcome, welcome);
    assert.equal(connection.state, "connected");
    assert.deepEqual(
        changes.map(({ previousState, state }) => [previousState, state]),
        [
            ["disconnected", "connecting"],
            ["connecting", "connected"],
        ],
    );
    assert.equal(await connection.connect(), welcome);
});

test("a connecting observer can synchronously complete the fake handshake", async () => {
    const { connection, socket } = createHarness();
    connection.onStateChange(({ state }) => {
        if (state === "connecting") {
            socket.open();
            socket.message(welcomeJson);
        }
    });

    const pending = connection.connect();

    assert.ok(pending instanceof Promise);
    assert.equal((await pending).kind, "welcome");
    assert.equal(connection.state, "connected");
});

test("send encodes client frames and onFrame observes validated post-welcome frames", async () => {
    const { connection, socket } = createHarness();
    const frames: PostWelcomeFrame[] = [];
    const stop = connection.onFrame((frame) => frames.push(frame));
    const pending = connection.connect();
    socket.open();
    socket.message(welcomeJson);
    await pending;

    connection.send({ kind: "cancel", streamId: CALLER_STREAM });
    assert.deepEqual(socket.sent, [`{"kind":"cancel","streamId":"${CALLER_STREAM}"}`]);

    socket.message(`{"kind":"response","streamId":"${CALLER_STREAM}","payload":"e30="}`);
    assert.deepEqual(frames, [{ kind: "response", streamId: CALLER_STREAM, payload: "e30=" }]);

    stop();
    socket.message(`{"kind":"complete","streamId":"${CALLER_STREAM}"}`);
    assert.equal(frames.length, 1);
});

test("request correlates concurrent responses and preserves opaque payloads", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);
    const observed: PostWelcomeFrame[] = [];
    connection.onFrame((frame) => observed.push(frame));

    const first = connection.request("inventory.get", "eyJpdGVtSWQiOiJpdGVtLTEifQ==", {
        timeoutMilliseconds: 3_000,
    });
    const second = connection.request("inventory.get", "eyJpdGVtSWQiOiJpdGVtLTIifQ==");

    const firstStream = sentStream(socket, 0);
    const secondStream = sentStream(socket, 1);
    assert.notEqual(firstStream, secondStream);
    assert.deepEqual(socket.sent, [
        `{"kind":"request","streamId":"${firstStream}","operation":"inventory.get","timeoutMilliseconds":3000,"payload":"eyJpdGVtSWQiOiJpdGVtLTEifQ=="}`,
        `{"kind":"request","streamId":"${secondStream}","operation":"inventory.get","payload":"eyJpdGVtSWQiOiJpdGVtLTIifQ=="}`,
    ]);

    socket.message(
        `{"kind":"response","streamId":"${secondStream}","payload":"eyJhdmFpbGFibGUiOjJ9"}`,
    );
    socket.message(
        `{"kind":"response","streamId":"${firstStream}","payload":"eyJhdmFpbGFibGUiOjF9"}`,
    );

    assert.equal(await first, "eyJhdmFpbGFibGUiOjF9");
    assert.equal(await second, "eyJhdmFpbGFibGUiOjJ9");
    assert.deepEqual(
        observed.map(({ kind, streamId }) => [kind, streamId]),
        [
            ["response", secondStream],
            ["response", firstStream],
        ],
    );
});

test("request rejects a server fault with its typed public fields", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);
    const pending = connection.request("inventory.get", "e30=");
    const stream = sentStream(socket, 0);

    socket.message(
        `{"kind":"fault","streamId":"${stream}","code":"operation_not_found","message":"The requested operation is not registered."}`,
    );

    await assert.rejects(
        pending,
        (error) =>
            error instanceof HostLoomRemoteFaultError &&
            error.streamId === stream &&
            error.code === "operation_not_found" &&
            error.message === "The requested operation is not registered.",
    );
});

test("request enforces the welcome concurrency limit and releases capacity on completion", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket, welcomeWithRequestLimit(1));

    const first = connection.request("inventory.get", "e30=");
    await assert.rejects(
        connection.request("inventory.get", "e30="),
        (error) => error instanceof HostLoomRequestCapacityError && error.limit === 1,
    );
    assert.equal(socket.sent.length, 1);

    const firstStream = sentStream(socket, 0);
    socket.message(`{"kind":"response","streamId":"${firstStream}","payload":"e30="}`);
    await first;

    const second = connection.request("inventory.get", "e30=");
    const secondStream = sentStream(socket, 1);
    assert.notEqual(secondStream, firstStream);
    socket.message(`{"kind":"response","streamId":"${secondStream}","payload":"e30="}`);
    await second;
});

test("AbortSignal sends cancel once and a late response cannot complete a newer request", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);

    const alreadyCanceled = new AbortController();
    alreadyCanceled.abort(new Error("caller stopped"));
    await assert.rejects(
        connection.request("inventory.get", "e30=", { signal: alreadyCanceled.signal }),
        HostLoomRequestCanceledError,
    );
    assert.deepEqual(socket.sent, []);

    const controller = new AbortController();
    const canceled = connection.request("inventory.get", "e30=", { signal: controller.signal });
    controller.abort(new Error("caller stopped"));
    controller.abort();

    await assert.rejects(canceled, HostLoomRequestCanceledError);
    const canceledStream = sentStream(socket, 0);
    assert.deepEqual(socket.sent, [
        `{"kind":"request","streamId":"${canceledStream}","operation":"inventory.get","payload":"e30="}`,
        `{"kind":"cancel","streamId":"${canceledStream}"}`,
    ]);

    const next = connection.request("inventory.get", "e30=");
    const nextStream = sentStream(socket, 2);
    socket.message(`{"kind":"response","streamId":"${canceledStream}","payload":"bGF0ZQ=="}`);
    socket.message(`{"kind":"response","streamId":"${nextStream}","payload":"e30="}`);
    assert.equal(await next, "e30=");
});

test("a canceled request keeps its concurrency reservation until the terminal server frame", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket, welcomeWithRequestLimit(1));
    const controller = new AbortController();
    const canceled = connection.request("inventory.get", "e30=", { signal: controller.signal });
    controller.abort();
    await assert.rejects(canceled, HostLoomRequestCanceledError);

    await assert.rejects(connection.request("inventory.get", "e30="), HostLoomRequestCapacityError);

    const canceledStream = sentStream(socket, 0);
    socket.message(
        `{"kind":"fault","streamId":"${canceledStream}","code":"canceled","message":"The request was canceled."}`,
    );
    const next = connection.request("inventory.get", "e30=");
    const nextStream = sentStream(socket, -1);
    assert.notEqual(nextStream, canceledStream);
    socket.message(`{"kind":"response","streamId":"${nextStream}","payload":"e30="}`);
    await next;
});

test("request removes its abort listener after a terminal response", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);
    const controller = new AbortController();
    const pending = connection.request("inventory.get", "e30=", { signal: controller.signal });

    socket.message(`{"kind":"response","streamId":"${sentStream(socket, 0)}","payload":"e30="}`);
    await pending;
    controller.abort();

    assert.equal(socket.sent.length, 1);
});

test("disconnect rejects every pending request and a new session allocates unused streams", async () => {
    const firstSocket = new FakeWebSocket();
    const secondSocket = new FakeWebSocket();
    const sockets = [firstSocket, secondSocket];
    const connection = new HostLoomConnection("wss://inventory.example.com/realtime", {
        webSocketFactory: () => nextSocket(sockets),
    });
    await connectHarness(connection, firstSocket);

    const first = connection.request("inventory.get", "e30=");
    const second = connection.request("inventory.get", "e30=");
    const abandoned = [sentStream(firstSocket, 0), sentStream(firstSocket, 1)];
    firstSocket.closed(1001, "server_shutdown", true);

    await assert.rejects(first, HostLoomConnectionClosedError);
    await assert.rejects(second, HostLoomConnectionClosedError);

    await connectHarness(connection, secondSocket);
    const afterReconnect = connection.request("inventory.get", "e30=");
    const reconnectStream = sentStream(secondSocket, 0);
    assert.ok(!abandoned.includes(reconnectStream));
    secondSocket.message(`{"kind":"response","streamId":"${reconnectStream}","payload":"e30="}`);
    await afterReconnect;
});

test("request validates local preconditions without leaking concurrency", async () => {
    const { connection, socket } = createHarness();
    await assert.rejects(connection.request("inventory.get", "e30="), HostLoomConnectionError);
    await connectHarness(connection, socket, welcomeWithRequestLimit(1));

    await assert.rejects(connection.request("   ", "e30="), TypeError);
    await assert.rejects(
        connection.request("inventory.get", "e30=", { timeoutMilliseconds: 0 }),
        HostLoomProtocolError,
    );

    const valid = connection.request("inventory.get", "e30=");
    assert.equal(socket.sent.length, 1);
    socket.message(`{"kind":"response","streamId":"${sentStream(socket, 0)}","payload":"e30="}`);
    await valid;
});

test("request rejects an oversized frame locally without consuming request capacity", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket, welcomeWithMessageSize(160));

    await assert.rejects(
        connection.request("inventory.get", "A".repeat(4_096)),
        (error) =>
            error instanceof HostLoomMessageSizeError &&
            error.actualSize > error.maximumSize &&
            error.maximumSize === 160,
    );
    assert.deepEqual(socket.sent, []);
    assert.equal(connection.state, "connected");

    const valid = connection.request("inventory.get", "e30=");
    assert.equal(socket.sent.length, 1);
    socket.message(`{"kind":"response","streamId":"${sentStream(socket, 0)}","payload":"e30="}`);
    await valid;
});

test("unowned subscription traffic triggers one cleanup frame per stream", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);

    socket.message(
        `{"kind":"subscribed","streamId":"${CALLER_STREAM}","topic":"inventory.changed","credit":2}`,
    );
    socket.message(
        `{"kind":"event","streamId":"${CALLER_STREAM}","topic":"inventory.changed","sequence":1,"eventId":"11111111111111111111111111111111","payload":"e30="}`,
    );

    assert.deepEqual(socket.sent, [`{"kind":"unsubscribe","streamId":"${CALLER_STREAM}"}`]);
    assert.equal(connection.state, "connected");
});

test("low-level subscriptions remain caller-owned instead of being treated as orphans", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);

    connection.send({
        kind: "subscribe",
        streamId: CALLER_STREAM,
        topic: "inventory.changed",
        credit: 2,
    });
    socket.message(
        `{"kind":"subscribed","streamId":"${CALLER_STREAM}","topic":"inventory.changed","credit":2}`,
    );
    socket.message(
        `{"kind":"event","streamId":"${CALLER_STREAM}","topic":"inventory.changed","sequence":1,"eventId":"11111111111111111111111111111111","payload":"e30="}`,
    );

    assert.deepEqual(socket.sent, [
        `{"kind":"subscribe","streamId":"${CALLER_STREAM}","topic":"inventory.changed","credit":2}`,
    ]);

    connection.send({ kind: "unsubscribe", streamId: CALLER_STREAM });
    socket.message(`{"kind":"complete","streamId":"${CALLER_STREAM}"}`);
    assert.equal(connection.state, "connected");
});

test("close is observable and a closed connection can reconnect manually", async () => {
    const firstSocket = new FakeWebSocket();
    const secondSocket = new FakeWebSocket();
    const sockets = [firstSocket, secondSocket];
    const changes: HostLoomConnectionStateChange[] = [];
    const connection = new HostLoomConnection("wss://inventory.example.com/realtime", {
        webSocketFactory: () => nextSocket(sockets),
    });
    connection.onStateChange((change) => changes.push(change));

    const firstConnect = connection.connect();
    firstSocket.open();
    firstSocket.message(welcomeJson);
    await firstConnect;

    connection.close(3001, "client_shutdown");
    assert.equal(connection.state, "closing");
    assert.deepEqual(firstSocket.closeCalls, [{ code: 3001, reason: "client_shutdown" }]);
    firstSocket.closed(1000, "closed", true);

    assert.equal(connection.state, "disconnected");
    assert.equal(connection.welcome, undefined);
    assert.deepEqual(connection.lastClose, { code: 1000, reason: "closed", wasClean: true });
    assert.equal(sockets.length, 1, "the connection must not reconnect automatically");

    const secondConnect = connection.connect();
    secondSocket.open();
    secondSocket.message(welcomeJson);
    await secondConnect;
    assert.equal(connection.state, "connected");
    assert.deepEqual(
        changes.map(({ state }) => state),
        ["connecting", "connected", "closing", "disconnected", "connecting", "connected"],
    );
});

test("connect during a manual close waits for teardown before opening one replacement", async () => {
    const firstSocket = new FakeWebSocket();
    const secondSocket = new FakeWebSocket();
    const sockets = [firstSocket, secondSocket];
    const states: HostLoomConnectionState[] = [];
    const connection = new HostLoomConnection("wss://inventory.example.com/realtime", {
        webSocketFactory: () => nextSocket(sockets),
    });
    connection.onStateChange(({ state }) => states.push(state));
    await connectHarness(connection, firstSocket);

    connection.close(3001, "client_shutdown");
    const queued = connection.connect();
    assert.equal(connection.connect(), queued);
    assert.equal(connection.state, "closing");
    assert.equal(sockets.length, 1, "the replacement must wait for the close event");

    firstSocket.closed(1000, "closed", true);
    assert.equal(connection.state, "connecting");
    assert.equal(sockets.length, 0, "exactly one replacement socket must be created");

    secondSocket.open();
    secondSocket.message(welcomeJson);
    assert.equal((await queued).kind, "welcome");
    assert.equal(connection.state, "connected");
    assert.deepEqual(states, [
        "connecting",
        "connected",
        "closing",
        "disconnected",
        "connecting",
        "connected",
    ]);
});

test("a queued reconnect rejects if the manual close cannot start", async () => {
    const closeFailure = new Error("close failed");
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);
    socket.closeError = closeFailure;
    let queued: Promise<WelcomeFrame> | undefined;
    connection.onStateChange(({ state }) => {
        if (state === "closing") {
            queued = connection.connect();
        }
    });

    assert.throws(
        () => connection.close(),
        (error) => error instanceof HostLoomConnectionError && error.cause === closeFailure,
    );
    await assert.rejects(
        queued as Promise<WelcomeFrame>,
        (error) => error instanceof HostLoomConnectionError && error.cause === closeFailure,
    );
    assert.equal(connection.state, "connected");
    assert.equal((await connection.connect()).kind, "welcome");
});

test("connect rejects a server-selected subprotocol mismatch", async () => {
    const { connection, socket } = createHarness();
    const pending = connection.connect();

    socket.open("another.protocol");

    await assert.rejects(pending, HostLoomProtocolError);
    assert.equal(connection.state, "closing");
    await assert.rejects(connection.connect(), HostLoomConnectionError);
    assert.deepEqual(socket.closeCalls, [{ code: undefined, reason: undefined }]);
    socket.closed(1006, "", false);
    assert.equal(connection.state, "disconnected");
});

test.each([
    { name: "malformed JSON", data: "not json" },
    { name: "a binary frame", data: new Uint8Array([1, 2, 3]) },
    {
        name: "a response frame",
        data: `{"kind":"response","streamId":"${CALLER_STREAM}","payload":"e30="}`,
    },
])("connect rejects $name before welcome", async ({ data }) => {
    const { connection, socket } = createHarness();
    const pending = connection.connect();
    socket.open();
    socket.message(data);

    await assert.rejects(pending, HostLoomProtocolError);
    assert.equal(connection.state, "closing");
    assert.equal(socket.closeCalls.length, 1);
});

test("a close before welcome rejects connect with close details", async () => {
    const { connection, socket } = createHarness();
    const pending = connection.connect();
    socket.open();
    socket.closed(1008, "session_expired", true);

    await assert.rejects(
        pending,
        (error) =>
            error instanceof HostLoomConnectionClosedError &&
            error.close.code === 1008 &&
            error.close.reason === "session_expired",
    );
    assert.equal(connection.state, "disconnected");
});

test("a WebSocket error rejects connect and begins closing", async () => {
    const { connection, socket } = createHarness();
    const pending = connection.connect();
    socket.error();

    await assert.rejects(pending, HostLoomConnectionError);
    assert.equal(connection.state, "closing");
    assert.equal(socket.closeCalls.length, 1);
});

test("factory and caller misuse failures are deterministic", async () => {
    const changes: HostLoomConnectionState[] = [];
    const connection = new HostLoomConnection("wss://inventory.example.com/realtime", {
        webSocketFactory: () => {
            throw new Error("construction failed");
        },
    });
    connection.onStateChange((change) => changes.push(change.state));

    await assert.rejects(connection.connect(), HostLoomConnectionError);
    assert.equal(connection.state, "disconnected");
    assert.deepEqual(changes, []);
    assert.throws(
        () => connection.send({ kind: "cancel", streamId: CALLER_STREAM }),
        HostLoomConnectionError,
    );
    assert.throws(() => connection.close(2000), RangeError);
    assert.throws(() => connection.close(3000, "☕".repeat(62)), RangeError);
});
