import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, test, vi } from "vitest";

import {
    HOSTLOOM_JSON_V1_SUBPROTOCOL,
    HostLoomConnection,
    HostLoomConnectionClosedError,
    type ClientFrame,
    type HostLoomConnectionOptions,
    type HostLoomSubscribeOptions,
    type HostLoomSubscription,
    type HostLoomSubscriptionClose,
    type HostLoomWebSocket,
    type HostLoomWebSocketEventMap,
    type ServerFrame,
    type WelcomeFrame,
} from "../dist/index.js";

type EventListener = (event: never) => void;

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

/** The gateway's default `MaximumControlFramesPerSecond`. */
const GATEWAY_CONTROL_BUDGET = 50;

class FakeWebSocket implements HostLoomWebSocket {
    public protocol = "";
    public onSend: ((data: string) => void) | undefined;
    readonly #listeners = new Map<keyof HostLoomWebSocketEventMap, Set<EventListener>>();

    public send(data: string): void {
        this.onSend?.(data);
    }

    public close(): void {}

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

interface GatewayStream {
    readonly topic: string;
    readonly limit: number;
    credit: number;
}

interface CreditRecord {
    readonly streamId: string;
    readonly credit: number;
}

interface AckRecord {
    readonly streamId: string;
    readonly sequence: number;
}

/**
 * Plays the gateway's side of the wire. It confirms subscriptions, delivers a published event
 * only while the stream has credit and drops it otherwise, as the gateway does, and counts
 * control frames in the gateway's fixed one-second windows.
 */
class GatewayModel {
    public readonly credits: CreditRecord[] = [];
    public readonly acknowledgements: AckRecord[] = [];
    public readonly controlFrames: number[] = [];
    public readonly violations: string[] = [];
    public delivered = 0;
    public dropped = 0;
    public busiestWindow = 0;
    readonly #socket: FakeWebSocket;
    readonly #streams = new Map<string, GatewayStream>();
    #sequence = 0;
    #windowStart = Date.now();
    #windowCount = 0;

    public constructor(socket: FakeWebSocket) {
        this.#socket = socket;
        socket.onSend = (data) => this.#receive(JSON.parse(data) as ClientFrame);
    }

    public creditOf(streamId: string): number {
        return this.#stream(streamId).credit;
    }

    public publish(streamId: string): void {
        const stream = this.#stream(streamId);
        if (stream.credit === 0) {
            this.dropped++;
            return;
        }

        stream.credit--;
        this.delivered++;
        this.#sequence++;
        this.#socket.message({
            kind: "event",
            streamId,
            topic: stream.topic,
            sequence: this.#sequence,
            eventId: this.#sequence.toString(16).padStart(32, "0"),
            payload: "e30=",
        });
    }

    #receive(frame: ClientFrame): void {
        if (frame.kind !== "request") {
            this.#countControlFrame();
        }

        if (frame.kind === "subscribe") {
            this.#streams.set(frame.streamId, {
                topic: frame.topic,
                limit: frame.credit,
                credit: frame.credit,
            });
            queueMicrotask(() =>
                this.#socket.message({
                    kind: "subscribed",
                    streamId: frame.streamId,
                    topic: frame.topic,
                    ...(frame.key === undefined ? {} : { key: frame.key }),
                    credit: frame.credit,
                }),
            );
        } else if (frame.kind === "credit") {
            const stream = this.#stream(frame.streamId);
            if (stream.credit + frame.credit > stream.limit) {
                this.violations.push(`credit ${frame.credit} above the window of ${stream.limit}`);
            }

            stream.credit += frame.credit;
            this.credits.push({ streamId: frame.streamId, credit: frame.credit });
        } else if (frame.kind === "ack") {
            this.acknowledgements.push({ streamId: frame.streamId, sequence: frame.sequence });
        }
    }

    /** Mirrors the gateway's fixed window: it restarts at the first frame a second after it began. */
    #countControlFrame(): void {
        const now = Date.now();
        if (now - this.#windowStart >= 1_000) {
            this.#windowStart = now;
            this.#windowCount = 0;
        }

        this.#windowCount++;
        this.busiestWindow = Math.max(this.busiestWindow, this.#windowCount);
        this.controlFrames.push(now);
    }

    #stream(streamId: string): GatewayStream {
        const stream = this.#streams.get(streamId);
        if (stream === undefined) {
            throw new Error(`The gateway has no stream ${streamId}.`);
        }

        return stream;
    }
}

/** The most frames sent in any one-second interval, whatever its alignment. */
function busiestSecond(times: readonly number[]): number {
    let busiest = 0;
    let first = 0;
    for (let last = 0; last < times.length; last++) {
        while ((times[last] as number) - (times[first] as number) >= 1_000) {
            first++;
        }
        busiest = Math.max(busiest, last - first + 1);
    }

    return busiest;
}

async function connectToGateway(options: HostLoomConnectionOptions = {}) {
    const socket = new FakeWebSocket();
    const gateway = new GatewayModel(socket);
    const connection = new HostLoomConnection("wss://inventory.example.com/realtime", {
        ...options,
        webSocketFactory: () => socket,
    });
    const connected = connection.connect();
    socket.open();
    socket.message(welcome);
    await connected;
    return { connection, gateway, socket };
}

async function subscribeAll(
    connection: HostLoomConnection,
    count: number,
    options: HostLoomSubscribeOptions,
): Promise<HostLoomSubscription[]> {
    return Promise.all(
        Array.from({ length: count }, (_, index) =>
            connection.subscribe("inventory.changed", { ...options, key: `item-${index}` }),
        ),
    );
}

async function subscribeOne(
    connection: HostLoomConnection,
    options: HostLoomSubscribeOptions,
): Promise<HostLoomSubscription> {
    const [subscription] = await subscribeAll(connection, 1, options);
    if (subscription === undefined) {
        throw new Error("The subscription was not created.");
    }

    return subscription;
}

afterEach(() => {
    vi.clearAllTimers();
    vi.useRealTimers();
});

test("a credit-2 subscription paces its credit through a burst of 200 events", async () => {
    vi.useFakeTimers();
    const { connection, gateway } = await connectToGateway();
    const subscription = await subscribeOne(connection, { credit: 2 });
    let received = 0;
    subscription.onEvent(() => received++);
    const started = Date.now();

    // One event per millisecond: far more than two credits can cover at any sane frame rate.
    for (let index = 0; index < 200; index++) {
        gateway.publish(subscription.streamId);
        await vi.advanceTimersByTimeAsync(1);
    }
    await vi.advanceTimersByTimeAsync(1_000);
    const elapsedSeconds = (Date.now() - started) / 1_000;

    assert.deepEqual(gateway.violations, []);
    assert.ok(
        busiestSecond(gateway.controlFrames) <= GATEWAY_CONTROL_BUDGET / 2,
        `${busiestSecond(gateway.controlFrames)} control frames in one second`,
    );
    assert.ok(gateway.busiestWindow <= GATEWAY_CONTROL_BUDGET);
    assert.ok(
        gateway.credits.length <= 5 + Math.ceil(20 * elapsedSeconds),
        `${gateway.credits.length} credit frames in ${elapsedSeconds} s`,
    );
    assert.equal(connection.state, "connected");
    assert.equal(subscription.state, "active");
    assert.equal(received, gateway.delivered);

    // Once the pacing holds a frame back, the next one returns everything consumed meanwhile,
    // and nothing is left owing when the burst is over.
    assert.ok(gateway.credits.filter(({ credit }) => credit === 2).length >= 3);
    assert.equal(gateway.creditOf(subscription.streamId), 2);
});

test("credit returns at the low watermark in the same tick while the budget allows", async () => {
    vi.useFakeTimers();
    const { connection, gateway } = await connectToGateway();
    const subscription = await subscribeOne(connection, { credit: 2 });
    subscription.onEvent(() => undefined);

    // Ten events per second is within the pacing, so the remaining credit never reaches zero.
    for (let index = 0; index < 50; index++) {
        gateway.publish(subscription.streamId);
        await vi.advanceTimersByTimeAsync(0);
        assert.equal(gateway.creditOf(subscription.streamId), 2, `after event ${index + 1}`);
        await vi.advanceTimersByTimeAsync(100);
    }

    assert.equal(gateway.dropped, 0);
    assert.equal(gateway.credits.length, 50);
    assert.equal(connection.state, "connected");
});

test("many acknowledging subscriptions stay under the gateway's control budget", async () => {
    vi.useFakeTimers();
    const { connection, gateway } = await connectToGateway();
    // The gateway's default MaximumSubscriptionsPerConnection.
    const subscriptions = await subscribeAll(connection, 32, { credit: 2 });
    for (const subscription of subscriptions) {
        subscription.onEvent((event) => subscription.acknowledge(event.sequence));
    }

    // Each subscription is offered 100 events per second for five seconds.
    for (let millisecond = 0; millisecond < 5_000; millisecond += 10) {
        for (const subscription of subscriptions) {
            gateway.publish(subscription.streamId);
        }
        await vi.advanceTimersByTimeAsync(10);
    }

    assert.deepEqual(gateway.violations, []);
    assert.ok(
        gateway.busiestWindow <= GATEWAY_CONTROL_BUDGET,
        `${gateway.busiestWindow} control frames in one gateway window`,
    );
    assert.equal(connection.state, "connected");
    for (const subscription of subscriptions) {
        assert.equal(subscription.state, "active");
        assert.ok(
            gateway.credits.some(({ streamId }) => streamId === subscription.streamId),
            "every subscription receives its share of the budget",
        );
    }

    // After the subscribe frames, only paced credit and acknowledgements are sent.
    const paced = gateway.controlFrames.slice(subscriptions.length);
    assert.ok(busiestSecond(paced) <= GATEWAY_CONTROL_BUDGET / 2);
});

test("acknowledgements made in one task leave as one frame with the highest sequence", async () => {
    vi.useFakeTimers();
    const { connection, gateway } = await connectToGateway();
    const subscription = await subscribeOne(connection, { credit: 32 });
    subscription.onEvent((event) => subscription.acknowledge(event.sequence));

    for (let index = 0; index < 5; index++) {
        gateway.publish(subscription.streamId);
    }
    assert.deepEqual(gateway.acknowledgements, [], "nothing leaves before the task ends");
    await vi.advanceTimersByTimeAsync(0);
    assert.deepEqual(gateway.acknowledgements, [{ streamId: subscription.streamId, sequence: 5 }]);

    // The gateway keeps the highest acknowledgement, so a lower one would change nothing.
    subscription.acknowledge(3);
    subscription.acknowledge(5);
    await vi.advanceTimersByTimeAsync(0);
    assert.equal(gateway.acknowledgements.length, 1);
    subscription.acknowledge(6);
    await vi.advanceTimersByTimeAsync(0);
    assert.deepEqual(gateway.acknowledgements.at(-1), {
        streamId: subscription.streamId,
        sequence: 6,
    });
    assert.equal(gateway.credits.length, 0, "27 of 32 credits remain above the watermark");
});

test("credit and acknowledgements still pending when the connection closes are discarded", async () => {
    vi.useFakeTimers();
    const { connection, gateway, socket } = await connectToGateway();
    const subscription = await subscribeOne(connection, { credit: 2 });
    const closes: HostLoomSubscriptionClose[] = [];
    subscription.onClose((close) => closes.push(close));
    subscription.onEvent((event) => subscription.acknowledge(event.sequence));

    gateway.publish(subscription.streamId);
    connection.close();
    await vi.advanceTimersByTimeAsync(1_000);

    assert.deepEqual(gateway.credits, []);
    assert.deepEqual(gateway.acknowledgements, []);
    assert.equal(closes.length, 0, "a discarded flush is not a subscription failure");
    socket.closed(1000, "", true);
    assert.equal(closes.length, 1);
    assert.ok(closes[0]?.error instanceof HostLoomConnectionClosedError);
});

test("a lower gateway control budget lowers the pacing with it", async () => {
    vi.useFakeTimers();
    const { connection, gateway } = await connectToGateway({ maximumControlFramesPerSecond: 10 });
    const subscription = await subscribeOne(connection, { credit: 2 });
    subscription.onEvent(() => undefined);

    for (let millisecond = 0; millisecond < 3_000; millisecond++) {
        gateway.publish(subscription.streamId);
        await vi.advanceTimersByTimeAsync(1);
    }

    assert.ok(busiestSecond(gateway.controlFrames) <= 5);
    assert.ok(gateway.busiestWindow <= 10);
    assert.equal(connection.state, "connected");
});

test("the control budget option must be a positive safe integer", () => {
    for (const maximumControlFramesPerSecond of [0, -1, 1.5, Number.NaN, 2 ** 53]) {
        assert.throws(
            () =>
                new HostLoomConnection("wss://inventory.example.com/realtime", {
                    maximumControlFramesPerSecond,
                }),
            RangeError,
            String(maximumControlFramesPerSecond),
        );
    }
});
