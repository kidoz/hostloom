import assert from "node:assert/strict";
import { afterEach, test, vi } from "vitest";

import {
    HOSTLOOM_JSON_V1_SUBPROTOCOL,
    HOSTLOOM_SESSION_STREAM,
    HostLoomConnection,
    HostLoomConnectionClosedError,
    HostLoomConnectionError,
    HostLoomProtocolError,
    type EventFrame,
    type HostLoomCloseInfo,
    type HostLoomReconnectOptions,
    type HostLoomSubscriptionClose,
    type HostLoomWebSocket,
    type HostLoomWebSocketEventMap,
    type ServerFrame,
    type WelcomeFrame,
} from "../dist/index.js";

type EventListener = (event: never) => void;

interface CloseCall {
    readonly code: number | undefined;
    readonly reason: string | undefined;
}

/** Allocates predictable stream identifiers so routing assertions stay readable. */
function sequentialStreamIds(): () => string {
    let next = 0;
    return () => (++next).toString(16).padStart(32, "0");
}

/** The nth identifier `sequentialStreamIds` hands out. */
function stream(index: number): string {
    return index.toString(16).padStart(32, "0");
}

const welcome: WelcomeFrame = {
    kind: "welcome",
    streamId: HOSTLOOM_SESSION_STREAM,
    sessionId: stream(0xf0f0),
    credit: 1_024,
    maximumMessageSize: 65_536,
    maximumConcurrentRequests: 8,
};

class FakeWebSocket implements HostLoomWebSocket {
    public protocol = "";
    public readonly sent: string[] = [];
    public readonly closeCalls: CloseCall[] = [];
    readonly #listeners = new Map<keyof HostLoomWebSocketEventMap, Set<EventListener>>();

    public send(data: string): void {
        this.sent.push(data);
    }

    public close(code?: number, reason?: string): void {
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

    public open(): void {
        this.protocol = HOSTLOOM_JSON_V1_SUBPROTOCOL;
        this.#emit("open", {} as Event);
    }

    public message(frame: ServerFrame): void {
        this.#emit("message", { data: JSON.stringify(frame) } as MessageEvent<unknown>);
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

function createHarness(reconnect: HostLoomReconnectOptions = { jitterRatio: 0 }) {
    const sockets: FakeWebSocket[] = [];
    const connection = new HostLoomConnection("wss://inventory.example.com/realtime", {
        reconnect,
        streamIdFactory: sequentialStreamIds(),
        webSocketFactory: () => {
            const socket = new FakeWebSocket();
            sockets.push(socket);
            return socket;
        },
    });
    return { connection, sockets };
}

/** Reads one created socket positionally, so a retry that never happened fails the test. */
function socketAt(sockets: readonly FakeWebSocket[], index: number): FakeWebSocket {
    const socket = sockets.at(index);
    if (socket === undefined) {
        throw new Error(`The harness has not created a socket at index ${index}.`);
    }

    return socket;
}

async function connect(
    connection: HostLoomConnection,
    sockets: readonly FakeWebSocket[],
    overrides: Partial<WelcomeFrame> = {},
): Promise<FakeWebSocket> {
    const pending = connection.connect();
    const socket = socketAt(sockets, -1);
    socket.open();
    socket.message({ ...welcome, ...overrides });
    await pending;
    return socket;
}

function sentFrames(socket: FakeWebSocket): Record<string, unknown>[] {
    return socket.sent.map((frame) => JSON.parse(frame) as Record<string, unknown>);
}

afterEach(() => {
    vi.clearAllTimers();
    vi.useRealTimers();
    vi.restoreAllMocks();
});

test("an unexpected close retries and restores a logical subscription without replaying requests", async () => {
    vi.useFakeTimers();
    const { connection, sockets } = createHarness();
    const firstSocket = await connect(connection, sockets);
    const request = connection.request("inventory.get", "e30=");
    const subscribing = connection.subscribe("inventory.changed", { credit: 4 });
    firstSocket.message({
        kind: "subscribed",
        streamId: stream(2),
        topic: "inventory.changed",
        credit: 4,
    });
    const subscription = await subscribing;
    const events: EventFrame[] = [];
    const closes: HostLoomSubscriptionClose[] = [];
    subscription.onEvent((event) => events.push(event));
    subscription.onClose((close) => closes.push(close));

    firstSocket.closed(1001, "server_shutdown", true);

    await assert.rejects(request, HostLoomConnectionClosedError);
    assert.equal(connection.state, "reconnecting");
    assert.equal(subscription.state, "reconnecting");
    assert.deepEqual(closes, []);
    const recovered = connection.connect();

    await vi.advanceTimersByTimeAsync(999);
    assert.equal(sockets.length, 1);
    await vi.advanceTimersByTimeAsync(1);
    assert.equal(sockets.length, 2);
    const secondSocket = socketAt(sockets, 1);
    secondSocket.open();
    secondSocket.message({ ...welcome, sessionId: stream(0xf0f1) });

    assert.equal((await recovered).sessionId, stream(0xf0f1));
    assert.equal(connection.state, "connected");
    assert.equal(subscription.state, "reconnecting");

    // Restoration takes a fresh identifier rather than reusing the abandoned one, so a late frame
    // from the closed session can never be mistaken for the restored stream.
    assert.deepEqual(sentFrames(secondSocket), [
        {
            kind: "subscribe",
            streamId: stream(3),
            topic: "inventory.changed",
            credit: 4,
        },
    ]);
    assert.equal(subscription.streamId, stream(3));

    secondSocket.message({
        kind: "subscribed",
        streamId: stream(3),
        topic: "inventory.changed",
        credit: 4,
    });
    assert.equal(subscription.state, "active");
    secondSocket.message({
        kind: "event",
        streamId: stream(3),
        topic: "inventory.changed",
        sequence: 1,
        eventId: stream(0x1001),
        payload: "e30=",
    });
    assert.equal(events.length, 1);
    assert.deepEqual(closes, []);
});

test("backoff doubles after failed attempts and resets after a welcome", async () => {
    vi.useFakeTimers();
    const { connection, sockets } = createHarness();
    const firstSocket = await connect(connection, sockets);

    firstSocket.closed(1006, "", false);
    await vi.advanceTimersByTimeAsync(1_000);
    assert.equal(sockets.length, 2);
    const recoveredAcrossAttempts = connection.connect();
    socketAt(sockets, 1).closed(1006, "", false);

    await vi.advanceTimersByTimeAsync(1_999);
    assert.equal(sockets.length, 2);
    await vi.advanceTimersByTimeAsync(1);
    assert.equal(sockets.length, 3);
    socketAt(sockets, 2).open();
    socketAt(sockets, 2).message({ ...welcome, sessionId: stream(0xf0f1) });
    assert.equal((await recoveredAcrossAttempts).sessionId, stream(0xf0f1));
    assert.equal(connection.state, "connected");

    socketAt(sockets, 2).closed(1001, "server_shutdown", true);
    await vi.advanceTimersByTimeAsync(999);
    assert.equal(sockets.length, 3);
    await vi.advanceTimersByTimeAsync(1);
    assert.equal(sockets.length, 4);
});

test("jitter varies each delay and never exceeds the configured maximum", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1);
    const { connection, sockets } = createHarness({
        initialDelayMilliseconds: 20_000,
        maximumDelayMilliseconds: 30_000,
        multiplier: 2,
        jitterRatio: 0.2,
    });
    const firstSocket = await connect(connection, sockets);

    firstSocket.closed(1006, "", false);
    await vi.advanceTimersByTimeAsync(23_999);
    assert.equal(sockets.length, 1);
    await vi.advanceTimersByTimeAsync(1);
    assert.equal(sockets.length, 2);

    socketAt(sockets, 1).closed(1006, "", false);
    await vi.advanceTimersByTimeAsync(29_999);
    assert.equal(sockets.length, 2);
    await vi.advanceTimersByTimeAsync(1);
    assert.equal(sockets.length, 3);
});

test("session expiry waits for credential refresh before creating a replacement socket", async () => {
    vi.useFakeTimers();
    let resolveRefresh: (() => void) | undefined;
    const refresh = vi.fn(
        (_close: HostLoomCloseInfo) =>
            new Promise<void>((resolve) => {
                resolveRefresh = resolve;
            }),
    );
    const { connection, sockets } = createHarness({ jitterRatio: 0, refreshCredentials: refresh });
    const firstSocket = await connect(connection, sockets);

    firstSocket.closed(1008, "session_expired", true);
    const recovered = connection.connect();
    vi.advanceTimersByTime(1_000);
    await Promise.resolve();

    assert.equal(refresh.mock.calls.length, 1);
    assert.deepEqual(refresh.mock.calls[0]?.[0], {
        code: 1008,
        reason: "session_expired",
        wasClean: true,
    });
    assert.equal(sockets.length, 1);

    if (resolveRefresh === undefined) {
        throw new Error("The reconnect policy has not requested a credential refresh.");
    }

    resolveRefresh();
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(sockets.length, 2);
    socketAt(sockets, 1).open();
    socketAt(sockets, 1).message({ ...welcome, sessionId: stream(0xf0f1) });
    await recovered;
});

test("session expiry is terminal without a credential refresh callback", async () => {
    vi.useFakeTimers();
    const { connection, sockets } = createHarness();
    const socket = await connect(connection, sockets);
    const subscribing = connection.subscribe("inventory.changed", { credit: 2 });
    socket.message({
        kind: "subscribed",
        streamId: stream(1),
        topic: "inventory.changed",
        credit: 2,
    });
    const subscription = await subscribing;
    const closed = new Promise<HostLoomSubscriptionClose>((resolve) =>
        subscription.onClose(resolve),
    );

    socket.closed(1008, "session_expired", true);

    assert.equal(connection.state, "disconnected");
    assert.equal((await closed).error instanceof HostLoomConnectionClosedError, true);
    await vi.advanceTimersByTimeAsync(60_000);
    assert.equal(sockets.length, 1);
});

test("a subscription awaiting its first confirmation survives reconnection", async () => {
    vi.useFakeTimers();
    const { connection, sockets } = createHarness();
    const firstSocket = await connect(connection, sockets);
    const subscribing = connection.subscribe("inventory.changed", { credit: 2 });
    let settled = false;
    void subscribing.then(
        () => {
            settled = true;
        },
        () => {
            settled = true;
        },
    );

    firstSocket.closed(1001, "server_shutdown", true);
    await Promise.resolve();
    assert.equal(settled, false);

    await vi.advanceTimersByTimeAsync(1_000);
    socketAt(sockets, 1).open();
    socketAt(sockets, 1).message({ ...welcome, sessionId: stream(0xf0f1) });
    assert.deepEqual(sentFrames(socketAt(sockets, 1)), [
        {
            kind: "subscribe",
            streamId: stream(2),
            topic: "inventory.changed",
            credit: 2,
        },
    ]);
    assert.equal(settled, false);

    socketAt(sockets, 1).message({
        kind: "subscribed",
        streamId: stream(2),
        topic: "inventory.changed",
        credit: 2,
    });
    assert.equal((await subscribing).state, "active");
});

test("a lower reconnected credit limit terminates only incompatible subscriptions", async () => {
    vi.useFakeTimers();
    const { connection, sockets } = createHarness();
    const firstSocket = await connect(connection, sockets);
    const subscribing = connection.subscribe("inventory.changed", { credit: 4 });
    firstSocket.message({
        kind: "subscribed",
        streamId: stream(1),
        topic: "inventory.changed",
        credit: 4,
    });
    const subscription = await subscribing;
    const closed = new Promise<HostLoomSubscriptionClose>((resolve) =>
        subscription.onClose(resolve),
    );

    firstSocket.closed(1001, "server_shutdown", true);
    await vi.advanceTimersByTimeAsync(1_000);
    socketAt(sockets, 1).open();
    socketAt(sockets, 1).message({ ...welcome, sessionId: stream(0xf0f1), credit: 2 });

    assert.equal((await closed).error instanceof HostLoomConnectionError, true);
    assert.equal(subscription.state, "closed");
    assert.deepEqual(socketAt(sockets, 1).sent, []);
    assert.equal(connection.state, "connected");
});

test("a rejected credential refresh terminates reconnection and retained subscriptions", async () => {
    vi.useFakeTimers();
    const refreshFailure = new Error("identity provider unavailable");
    const { connection, sockets } = createHarness({
        jitterRatio: 0,
        refreshCredentials: async () => Promise.reject(refreshFailure),
    });
    const socket = await connect(connection, sockets);
    const subscribing = connection.subscribe("inventory.changed", { credit: 2 });
    socket.message({
        kind: "subscribed",
        streamId: stream(1),
        topic: "inventory.changed",
        credit: 2,
    });
    const subscription = await subscribing;
    const closed = new Promise<HostLoomSubscriptionClose>((resolve) =>
        subscription.onClose(resolve),
    );

    socket.closed(1008, "session_expired", true);
    const recovered = connection.connect();
    await vi.advanceTimersByTimeAsync(1_000);

    await assert.rejects(
        recovered,
        (error) => error instanceof HostLoomConnectionError && error.cause === refreshFailure,
    );
    assert.equal(connection.state, "disconnected");
    assert.equal((await closed).error?.cause, refreshFailure);
    assert.equal(sockets.length, 1);
});

test("manual close and protocol failure never start automatic reconnect", async () => {
    vi.useFakeTimers();
    const manual = createHarness();
    const manualSocket = await connect(manual.connection, manual.sockets);
    manual.connection.close(3001, "client_shutdown");
    manualSocket.closed(1000, "closed", true);
    await vi.advanceTimersByTimeAsync(60_000);
    assert.equal(manual.connection.state, "disconnected");
    assert.equal(manual.sockets.length, 1);

    const protocol = createHarness();
    const pending = protocol.connection.connect();
    socketAt(protocol.sockets, 0).open();
    socketAt(protocol.sockets, 0).message({
        kind: "response",
        streamId: stream(1),
        payload: "e30=",
    });
    await assert.rejects(pending, HostLoomProtocolError);
    socketAt(protocol.sockets, 0).closed(1006, "", false);
    await vi.advanceTimersByTimeAsync(60_000);
    assert.equal(protocol.connection.state, "disconnected");
    assert.equal(protocol.sockets.length, 1);
});

test("manual close while a failed socket is closing cancels the pending retry", async () => {
    vi.useFakeTimers();
    const { connection, sockets } = createHarness();
    const socket = await connect(connection, sockets);

    socket.error();
    assert.equal(connection.state, "closing");
    connection.close();
    socket.closed(1006, "", false);

    assert.equal(connection.state, "disconnected");
    await vi.advanceTimersByTimeAsync(60_000);
    assert.equal(sockets.length, 1);
});

test("unsubscribe while reconnecting removes the logical subscription without a wire frame", async () => {
    vi.useFakeTimers();
    const { connection, sockets } = createHarness();
    const firstSocket = await connect(connection, sockets);
    const subscribing = connection.subscribe("inventory.changed", { credit: 2 });
    firstSocket.message({
        kind: "subscribed",
        streamId: stream(1),
        topic: "inventory.changed",
        credit: 2,
    });
    const subscription = await subscribing;

    firstSocket.closed(1001, "server_shutdown", true);
    await subscription.unsubscribe();
    assert.equal(subscription.state, "closed");

    await vi.advanceTimersByTimeAsync(1_000);
    socketAt(sockets, 1).open();
    socketAt(sockets, 1).message({ ...welcome, sessionId: stream(0xf0f1) });
    assert.deepEqual(socketAt(sockets, 1).sent, []);
});

test("reconnect option validation rejects unsafe timing policies", () => {
    const create = (reconnect: HostLoomReconnectOptions) =>
        new HostLoomConnection("wss://inventory.example.com/realtime", { reconnect });

    assert.throws(() => create({ initialDelayMilliseconds: 0 }), RangeError);
    assert.throws(
        () => create({ initialDelayMilliseconds: 2_000, maximumDelayMilliseconds: 1_000 }),
        RangeError,
    );
    assert.throws(() => create({ multiplier: 1 }), RangeError);
    assert.throws(() => create({ jitterRatio: 1 }), RangeError);
    assert.throws(
        // A JavaScript caller can still pass a non-callable refresh hook.
        () => create({ refreshCredentials: true } as unknown as HostLoomReconnectOptions),
        TypeError,
    );
});
