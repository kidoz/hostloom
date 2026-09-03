import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "vitest";

import {
    HOSTLOOM_JSON_V1_SUBPROTOCOL,
    HostLoomConnection,
    HostLoomConnectionClosedError,
    HostLoomProtocolError,
    HostLoomRemoteFaultError,
    HostLoomSubscriptionCanceledError,
    type EventFrame,
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

const packageDirectory = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const welcome = JSON.parse(
    await readFile(
        resolve(
            packageDirectory,
            "../../src/HostLoom.AspNetCore.WebSockets/protocol/fixtures/json-v1/welcome.json",
        ),
        "utf8",
    ),
) as WelcomeFrame;

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

/** Allocates predictable stream identifiers so routing assertions stay readable. */
function sequentialStreamIds(): () => string {
    let next = 0;
    return () => (++next).toString(16).padStart(32, "0");
}

/** The nth identifier `sequentialStreamIds` hands out. */
function stream(index: number): string {
    return index.toString(16).padStart(32, "0");
}

function createHarness() {
    const socket = new FakeWebSocket();
    const connection = new HostLoomConnection("wss://inventory.example.com/realtime", {
        webSocketFactory: () => socket,
        streamIdFactory: sequentialStreamIds(),
    });
    return { connection, socket };
}

async function connectHarness(
    connection: HostLoomConnection,
    socket: FakeWebSocket,
    credit = welcome.credit,
): Promise<void> {
    const pending = connection.connect();
    socket.open();
    socket.message({ ...welcome, credit });
    await pending;
}

function sentFrames(socket: FakeWebSocket): Record<string, unknown>[] {
    return socket.sent.map((frame) => JSON.parse(frame) as Record<string, unknown>);
}

function event(
    streamId: string,
    sequence: number,
    eventId = stream(0x1000 + sequence),
): EventFrame {
    return {
        kind: "event",
        streamId,
        topic: "inventory.changed",
        sequence,
        eventId,
        payload: "e30=",
    };
}

test("subscribe shares request stream allocation and waits for confirmation", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);

    const request = connection.request("inventory.get", "e30=");
    const pending = connection.subscribe("inventory.changed", {
        key: "item-1",
        credit: 4,
    });
    let confirmed = false;
    void pending.then(() => {
        confirmed = true;
    });

    assert.deepEqual(sentFrames(socket), [
        {
            kind: "request",
            streamId: stream(1),
            operation: "inventory.get",
            payload: "e30=",
        },
        {
            kind: "subscribe",
            streamId: stream(2),
            topic: "inventory.changed",
            key: "item-1",
            credit: 4,
        },
    ]);
    await Promise.resolve();
    assert.equal(confirmed, false);

    socket.message({
        kind: "subscribed",
        streamId: stream(2),
        topic: "inventory.changed",
        key: "item-1",
        credit: 4,
    });
    const subscription = await pending;
    assert.equal(subscription.streamId, stream(2));
    assert.equal(subscription.topic, "inventory.changed");
    assert.equal(subscription.key, "item-1");
    assert.equal(subscription.state, "active");

    socket.message({ kind: "response", streamId: stream(1), payload: "e30=" });
    await request;
});

test("subscribe rejects a gateway denial with a typed remote fault", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);
    const pending = connection.subscribe("inventory.changed", { credit: 2 });

    socket.message({
        kind: "fault",
        streamId: stream(1),
        code: "forbidden",
        message: "The caller is not authorized for this topic.",
    });

    await assert.rejects(
        pending,
        (error) =>
            error instanceof HostLoomRemoteFaultError &&
            error.streamId === stream(1) &&
            error.code === "forbidden",
    );
});

test("subscribe treats a mismatched confirmation as a protocol failure", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);
    const pending = connection.subscribe("inventory.changed", { credit: 2 });

    socket.message({
        kind: "subscribed",
        streamId: stream(1),
        topic: "inventory.changed",
        credit: 3,
    });

    await assert.rejects(pending, HostLoomProtocolError);
    assert.equal(connection.state, "closing");
    assert.equal(socket.closeCalls.length, 1);
});

test("events buffer until observed and replenish credit at the low watermark", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);
    const pending = connection.subscribe("inventory.changed", {
        credit: 3,
        lowWatermark: 1,
    });
    socket.message({
        kind: "subscribed",
        streamId: stream(1),
        topic: "inventory.changed",
        credit: 3,
    });
    const subscription = await pending;

    socket.message(event(stream(1), 0, stream(0xf001)));
    socket.message(event(stream(1), 1));
    assert.equal(socket.sent.length, 1, "credit pauses until an event listener is attached");

    const received: EventFrame[] = [];
    subscription.onEvent((frame) => received.push(frame));
    assert.deepEqual(
        received.map(({ sequence, eventId }) => [sequence, eventId]),
        [
            [0, stream(0xf001)],
            [1, stream(0x1001)],
        ],
    );
    assert.deepEqual(sentFrames(socket).at(-1), {
        kind: "credit",
        streamId: stream(1),
        credit: 2,
    });

    socket.message(event(stream(1), 2));
    assert.equal(socket.sent.length, 2);
    socket.message(event(stream(1), 3));
    assert.deepEqual(sentFrames(socket).at(-1), {
        kind: "credit",
        streamId: stream(1),
        credit: 2,
    });
    assert.deepEqual(
        received.map(({ sequence }) => sequence),
        [0, 1, 2, 3],
    );
});

test("an event beyond locally available credit closes the connection", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);
    const pending = connection.subscribe("inventory.changed", { credit: 1 });
    socket.message({
        kind: "subscribed",
        streamId: stream(1),
        topic: "inventory.changed",
        credit: 1,
    });
    const subscription = await pending;
    const closed = new Promise<HostLoomSubscriptionClose>((resolve) =>
        subscription.onClose(resolve),
    );

    socket.message(event(stream(1), 1));
    socket.message(event(stream(1), 2));

    assert.equal(connection.state, "closing");
    assert.equal((await closed).error instanceof HostLoomProtocolError, true);
});

test("unsubscribe sends once, ignores later events, and resolves on complete", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);
    const pending = connection.subscribe("inventory.changed", { credit: 2 });
    socket.message({
        kind: "subscribed",
        streamId: stream(1),
        topic: "inventory.changed",
        credit: 2,
    });
    const subscription = await pending;
    const events: EventFrame[] = [];
    const closes: HostLoomSubscriptionClose[] = [];
    subscription.onEvent((frame) => events.push(frame));
    subscription.onClose((close) => closes.push(close));

    const first = subscription.unsubscribe();
    const second = subscription.unsubscribe();
    assert.equal(first, second);
    assert.equal(subscription.state, "unsubscribing");
    assert.deepEqual(sentFrames(socket).at(-1), { kind: "unsubscribe", streamId: stream(1) });
    assert.equal(sentFrames(socket).filter(({ kind }) => kind === "unsubscribe").length, 1);

    socket.message(event(stream(1), 1));
    assert.deepEqual(events, []);
    socket.message({ kind: "complete", streamId: stream(1) });
    await first;
    assert.equal(subscription.state, "closed");
    assert.deepEqual(closes, [{}]);
    await subscription.unsubscribe();
});

test("AbortSignal maps to unsubscribe before and after confirmation", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);

    const alreadyCanceled = new AbortController();
    alreadyCanceled.abort(new Error("caller stopped"));
    await assert.rejects(
        connection.subscribe("inventory.changed", {
            credit: 2,
            signal: alreadyCanceled.signal,
        }),
        HostLoomSubscriptionCanceledError,
    );
    assert.deepEqual(socket.sent, []);

    const beforeConfirmation = new AbortController();
    const canceled = connection.subscribe("inventory.changed", {
        credit: 2,
        signal: beforeConfirmation.signal,
    });
    beforeConfirmation.abort(new Error("caller stopped"));
    await assert.rejects(canceled, HostLoomSubscriptionCanceledError);
    assert.deepEqual(sentFrames(socket), [
        {
            kind: "subscribe",
            streamId: stream(1),
            topic: "inventory.changed",
            credit: 2,
        },
        { kind: "unsubscribe", streamId: stream(1) },
    ]);
    socket.message({
        kind: "subscribed",
        streamId: stream(1),
        topic: "inventory.changed",
        credit: 2,
    });
    socket.message({ kind: "complete", streamId: stream(1) });

    const afterConfirmation = new AbortController();
    const active = connection.subscribe("inventory.changed", {
        credit: 2,
        signal: afterConfirmation.signal,
    });
    socket.message({
        kind: "subscribed",
        streamId: stream(2),
        topic: "inventory.changed",
        credit: 2,
    });
    const subscription = await active;
    afterConfirmation.abort();
    afterConfirmation.abort();
    assert.equal(subscription.state, "unsubscribing");
    assert.equal(sentFrames(socket).filter(({ kind }) => kind === "unsubscribe").length, 2);
    socket.message({ kind: "complete", streamId: stream(2) });
    assert.equal(subscription.state, "closed");
});

test("fault terminates an active subscription with a typed error", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);
    const pending = connection.subscribe("inventory.changed", { credit: 2 });
    socket.message({
        kind: "subscribed",
        streamId: stream(1),
        topic: "inventory.changed",
        credit: 2,
    });
    const subscription = await pending;
    const closed = new Promise<HostLoomSubscriptionClose>((resolve) =>
        subscription.onClose(resolve),
    );

    socket.message({
        kind: "fault",
        streamId: stream(1),
        code: "snapshot_failed",
        message: "The topic snapshot could not be loaded.",
    });

    const result = await closed;
    assert.equal(result.error instanceof HostLoomRemoteFaultError, true);
    await subscription.unsubscribe();
    assert.equal(subscription.state, "closed");
    assert.equal(sentFrames(socket).filter(({ kind }) => kind === "unsubscribe").length, 0);
});

test("disconnect terminates an active subscription and pending unsubscribe", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);
    const pending = connection.subscribe("inventory.changed", { credit: 2 });
    socket.message({
        kind: "subscribed",
        streamId: stream(1),
        topic: "inventory.changed",
        credit: 2,
    });
    const subscription = await pending;
    const closed = new Promise<HostLoomSubscriptionClose>((resolve) =>
        subscription.onClose(resolve),
    );
    const stopping = subscription.unsubscribe();

    socket.closed(1001, "server_shutdown", true);

    assert.equal((await closed).error instanceof HostLoomConnectionClosedError, true);
    await assert.rejects(stopping, HostLoomConnectionClosedError);
    assert.equal(subscription.state, "closed");
});

test("disconnect rejects a subscription still waiting for confirmation", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);
    const pending = connection.subscribe("inventory.changed", { credit: 2 });

    socket.closed(1001, "server_shutdown", true);

    await assert.rejects(pending, HostLoomConnectionClosedError);
});

test("acknowledge validates positive sequences and sends an ack while active", async () => {
    const { connection, socket } = createHarness();
    await connectHarness(connection, socket);
    const pending = connection.subscribe("inventory.changed", { credit: 2 });
    socket.message({
        kind: "subscribed",
        streamId: stream(1),
        topic: "inventory.changed",
        credit: 2,
    });
    const subscription = await pending;

    assert.throws(() => subscription.acknowledge(0), RangeError);
    subscription.acknowledge(7);
    assert.deepEqual(sentFrames(socket).at(-1), {
        kind: "ack",
        streamId: stream(1),
        sequence: 7,
    });

    const closing = subscription.unsubscribe();
    assert.throws(() => subscription.acknowledge(8), /only while the subscription is active/);
    socket.message({ kind: "complete", streamId: stream(1) });
    await closing;
});

test("subscribe validates credit, key, and watermark before allocating a stream", async () => {
    const { connection, socket } = createHarness();
    await assert.rejects(
        connection.subscribe("inventory.changed", { credit: 1 }),
        /only while connected/,
    );
    await connectHarness(connection, socket, 4);

    await assert.rejects(connection.subscribe(" ", { credit: 1 }), TypeError);
    await assert.rejects(connection.subscribe("inventory.changed", { credit: 0 }), RangeError);
    await assert.rejects(connection.subscribe("inventory.changed", { credit: 5 }), RangeError);
    await assert.rejects(
        connection.subscribe("inventory.changed", { credit: 2, lowWatermark: 2 }),
        RangeError,
    );
    await assert.rejects(
        connection.subscribe("inventory.changed", { credit: 2, key: "x".repeat(257) }),
        RangeError,
    );
    assert.deepEqual(socket.sent, []);

    const pending = connection.subscribe("inventory.changed", { credit: 2 });
    assert.equal(sentFrames(socket)[0]?.streamId, stream(1));
    socket.message({
        kind: "subscribed",
        streamId: stream(1),
        topic: "inventory.changed",
        credit: 2,
    });
    await pending;
});
