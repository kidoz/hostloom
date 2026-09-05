import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "vitest";

import {
    decodeJsonPayload,
    decodeServerFrame,
    encodeClientFrame,
    encodeJsonPayload,
    HOSTLOOM_JSON_V1_FRAME_KINDS,
    HOSTLOOM_JSON_V1_SUBPROTOCOL,
    HOSTLOOM_SESSION_STREAM,
    HostLoomProtocolError,
    newStreamId,
    type ClientFrame,
} from "../dist/index.js";

const packageDirectory = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const protocolDirectory = resolve(
    packageDirectory,
    "../../src/HostLoom.AspNetCore.WebSockets/protocol",
);
const fixtureDirectory = resolve(protocolDirectory, "fixtures/json-v1");

const fixtureNames = ["welcome", "subscribed", "event", "snapshot-event", "fault", "pong"];

const STREAM = "2222222222222222222222222222abcd";
const SESSION = "11111111111111111111111111111111";
const EVENT = "55555555555555555555555555555555";

interface NumericSchema {
    readonly minimum: number;
    readonly maximum: number;
}

interface FrameSchema {
    readonly $defs: {
        readonly identifier: { readonly pattern: string };
        readonly sessionStream: { readonly const: string };
        readonly clientStream: {
            readonly pattern: string;
            readonly not: { readonly const: string };
        };
    };
    readonly properties: {
        readonly kind: { readonly enum: readonly string[] };
        readonly streamId: { readonly $ref: string };
        readonly sessionId: { readonly $ref: string };
        readonly eventId: { readonly $ref: string };
        readonly timeoutMilliseconds: NumericSchema;
        readonly credit: NumericSchema;
        readonly sequence: NumericSchema;
        readonly maximumMessageSize: NumericSchema;
        readonly maximumConcurrentRequests: NumericSchema;
        readonly payload: { readonly pattern: string };
    };
    readonly additionalProperties: boolean;
    readonly required: readonly string[];
    readonly allOf: readonly {
        readonly if: { readonly properties: { readonly kind: { readonly const?: string } } };
        readonly then: {
            readonly properties?: { readonly streamId?: { readonly $ref?: string } };
        };
    }[];
}

async function readFrameSchema(): Promise<FrameSchema> {
    return JSON.parse(
        await readFile(
            resolve(protocolDirectory, "hostloom-websocket-json-v1.schema.json"),
            "utf8",
        ),
    ) as FrameSchema;
}

/** Frames the exported types reject. The codec must still reject them at runtime. */
function untypedClientFrame(frame: unknown): ClientFrame {
    return frame as ClientFrame;
}

test("uses the registered JSON-v1 subprotocol", () => {
    assert.equal(HOSTLOOM_JSON_V1_SUBPROTOCOL, "hostloom.json.v1");
});

test("frame kinds remain synchronized with the published schema", async () => {
    const schema = await readFrameSchema();

    assert.deepEqual(schema.properties.kind.enum, HOSTLOOM_JSON_V1_FRAME_KINDS);
    assert.equal(schema.additionalProperties, false);
    assert.deepEqual(schema.required, ["kind", "streamId"]);
});

test("numeric bounds remain synchronized with the published schema", async () => {
    const { properties } = await readFrameSchema();

    // The schema may never admit a value this client refuses: every bound stays inside the
    // safe-integer range `assertInteger` enforces.
    for (const [name, bounds] of Object.entries(properties).filter(
        (entry): entry is [string, NumericSchema] => "maximum" in entry[1],
    )) {
        assert.ok(
            Number.isSafeInteger(bounds.maximum),
            `${name} maximum ${bounds.maximum} is outside the safe-integer range.`,
        );
        assert.ok(bounds.minimum >= 0, `${name} minimum ${bounds.minimum} is negative.`);
    }

    assert.equal(properties.sequence.maximum, Number.MAX_SAFE_INTEGER);
    assert.equal(properties.credit.maximum, 2_147_483_647);
    assert.equal(properties.timeoutMilliseconds.maximum, 2_147_483_647);
    assert.equal(properties.maximumMessageSize.maximum, 2_147_483_647);
    assert.equal(properties.maximumConcurrentRequests.maximum, 2_147_483_647);
});

test("the codecs enforce the published numeric boundaries in both directions", async () => {
    const { properties } = await readFrameSchema();
    for (const property of [
        "timeoutMilliseconds",
        "credit",
        "sequence",
        "maximumMessageSize",
        "maximumConcurrentRequests",
    ] as const) {
        const { minimum, maximum } = properties[property];
        for (const value of [minimum, maximum]) {
            assert.doesNotThrow(() =>
                encodeClientFrame(
                    untypedClientFrame({ kind: "ping", streamId: STREAM, [property]: value }),
                ),
            );
            assert.doesNotThrow(() =>
                decodeServerFrame(
                    JSON.stringify({ kind: "pong", streamId: STREAM, [property]: value }),
                ),
            );
        }
        for (const value of [minimum - 1, maximum + 1, 1.5]) {
            assert.throws(
                () =>
                    encodeClientFrame(
                        untypedClientFrame({ kind: "ping", streamId: STREAM, [property]: value }),
                    ),
                HostLoomProtocolError,
                `${property}=${value} must not be encoded`,
            );
            assert.throws(
                () =>
                    decodeServerFrame(
                        JSON.stringify({ kind: "pong", streamId: STREAM, [property]: value }),
                    ),
                HostLoomProtocolError,
                `${property}=${value} must not be decoded`,
            );
        }
    }
});

test("frame payloads require Base64 syntax while preserving opaque bytes", () => {
    for (const payload of ["", "AA==", "//8=", "AQID", "AAAA".repeat(100_000)]) {
        const encoded = encodeClientFrame({
            kind: "request",
            streamId: STREAM,
            operation: "inventory.get",
            payload,
        });
        assert.equal((JSON.parse(encoded) as { payload: string }).payload, payload);
        for (const kind of ["response", "event"]) {
            const frame = decodeServerFrame(
                JSON.stringify({
                    kind,
                    streamId: STREAM,
                    sequence: 0,
                    eventId: EVENT,
                    payload,
                }),
            );
            assert.ok("payload" in frame);
            assert.equal(frame.payload, payload);
        }
    }
    for (const payload of [
        "not base64",
        "A",
        "AAA",
        "A===",
        "====",
        "AA=A",
        "AA-_",
        "AA==\n",
        "AAA\n",
        "AAA\r",
        "AAA\u2028",
        " AAA",
        "AA==AAAA",
    ]) {
        assert.throws(
            () =>
                encodeClientFrame({
                    kind: "request",
                    streamId: STREAM,
                    operation: "inventory.get",
                    payload,
                }),
            HostLoomProtocolError,
        );
        for (const kind of ["response", "event"]) {
            assert.throws(
                () =>
                    decodeServerFrame(
                        JSON.stringify({
                            kind,
                            streamId: STREAM,
                            sequence: 0,
                            eventId: EVENT,
                            payload,
                        }),
                    ),
                HostLoomProtocolError,
            );
        }
    }
});

test("the published schema pins the welcome stream to the session identifier", async () => {
    const schema = await readFrameSchema();
    const welcome = schema.allOf.find((branch) => branch.if.properties.kind.const === "welcome");

    assert.equal(welcome?.then.properties?.streamId?.$ref, "#/$defs/sessionStream");
    assert.equal(schema.$defs.sessionStream.const, HOSTLOOM_SESSION_STREAM);
    assert.throws(
        () =>
            decodeServerFrame(
                `{"kind":"welcome","streamId":"${STREAM}","sessionId":"${SESSION}","credit":1,"maximumMessageSize":64,"maximumConcurrentRequests":1}`,
            ),
        HostLoomProtocolError,
    );
});

test("identifiers remain synchronized with the published schema", async () => {
    const schema = await readFrameSchema();
    const identifier = new RegExp(schema.$defs.identifier.pattern);

    for (const property of ["streamId", "sessionId", "eventId"] as const) {
        assert.equal(schema.properties[property].$ref, "#/$defs/identifier");
    }

    // Whatever the allocator produces must satisfy the published contract, and the spellings the
    // contract excludes must not slip through either.
    for (let attempt = 0; attempt < 32; attempt++) {
        const allocated = newStreamId();
        assert.ok(identifier.test(allocated), `The schema rejected ${allocated}.`);
        assert.notEqual(allocated, HOSTLOOM_SESSION_STREAM);
    }

    assert.ok(!identifier.test(`${STREAM.slice(0, 8)}-${STREAM.slice(8)}`));
    assert.ok(!identifier.test(STREAM.toUpperCase()));
    assert.equal(schema.$defs.clientStream.not.const, HOSTLOOM_SESSION_STREAM);
});

test("the published payload pattern accepts encoded payloads and rejects malformed Base64", async () => {
    const { properties } = await readFrameSchema();
    const pattern = new RegExp(properties.payload.pattern);

    for (const value of ["", encodeJsonPayload({ itemId: "item-42" }), encodeJsonPayload([1, 2])]) {
        assert.ok(pattern.test(value), `The schema pattern rejected the encoded payload ${value}.`);
    }

    assert.ok(!pattern.test("not base64"));
    assert.throws(() => decodeJsonPayload("not base64"), HostLoomProtocolError);
});

for (const fixtureName of fixtureNames) {
    test(`decodes and canonically reproduces the ${fixtureName} fixture`, async () => {
        const fixture = (
            await readFile(resolve(fixtureDirectory, `${fixtureName}.json`), "utf8")
        ).trim();
        const frame = decodeServerFrame(fixture);

        assert.equal(JSON.stringify(frame), fixture);
    });
}

test("encodes compact client frames in protocol property order", () => {
    assert.equal(
        encodeClientFrame({
            kind: "request",
            streamId: STREAM,
            operation: "inventory.get",
            timeoutMilliseconds: 3_000,
            payload: "eyJpdGVtSWQiOiJpdGVtLTQyIn0=",
        }),
        `{"kind":"request","streamId":"${STREAM}","operation":"inventory.get","timeoutMilliseconds":3000,"payload":"eyJpdGVtSWQiOiJpdGVtLTQyIn0="}`,
    );

    assert.equal(
        encodeClientFrame({
            kind: "subscribe",
            streamId: STREAM,
            topic: "inventory.level.changed",
            key: "item-42",
            credit: 32,
        }),
        `{"kind":"subscribe","streamId":"${STREAM}","topic":"inventory.level.changed","key":"item-42","credit":32}`,
    );

    assert.equal(
        encodeClientFrame({ kind: "ping", streamId: STREAM }),
        `{"kind":"ping","streamId":"${STREAM}"}`,
    );
});

test("keeps ping client-only and pong server-only", () => {
    assert.throws(
        () => encodeClientFrame(untypedClientFrame({ kind: "pong", streamId: STREAM })),
        HostLoomProtocolError,
    );
    assert.throws(
        () => decodeServerFrame(`{"kind":"ping","streamId":"${STREAM}"}`),
        HostLoomProtocolError,
    );
    assert.throws(
        () => encodeClientFrame({ kind: "ping", streamId: HOSTLOOM_SESSION_STREAM }),
        HostLoomProtocolError,
    );
    assert.throws(
        () => decodeServerFrame(`{"kind":"pong","streamId":"${HOSTLOOM_SESSION_STREAM}"}`),
        HostLoomProtocolError,
    );
});

test("round-trips Unicode application JSON through Base64", () => {
    const payload = { itemId: "item-42", description: "Café ☕", available: 8 };
    const encoded = encodeJsonPayload(payload);

    assert.deepEqual(decodeJsonPayload<typeof payload>(encoded), payload);
});

test("rejects malformed or server-invalid frames", () => {
    const welcomeBody = `"sessionId":"${SESSION}","credit":1,"maximumMessageSize":64,"maximumConcurrentRequests":1`;
    const invalidFrames = [
        "not json",
        "[]",
        `{"kind":"Welcome","streamId":"${HOSTLOOM_SESSION_STREAM}"}`,
        `{"kind":"unknown","streamId":"${HOSTLOOM_SESSION_STREAM}"}`,
        `{"kind":"welcome","streamId":"${HOSTLOOM_SESSION_STREAM}",${welcomeBody},"extra":true}`,
        `{"kind":"welcome","streamId":"${STREAM}",${welcomeBody}}`,
        `{"kind":"event","streamId":"${STREAM}","sequence":7,"eventId":"${EVENT}"}`,
        `{"kind":"event","streamId":41,"sequence":7,"eventId":"${EVENT}","payload":"AQID"}`,
        `{"kind":"event","streamId":"${STREAM}","sequence":7,"eventId":"event-1","payload":"AQID"}`,
        `{"kind":"event","streamId":"${STREAM.toUpperCase()}","sequence":7,"eventId":"${EVENT}","payload":"AQID"}`,
        `{"kind":"request","streamId":"${STREAM}","operation":"inventory.get","payload":"e30="}`,
    ];

    for (const frame of invalidFrames) {
        assert.throws(() => decodeServerFrame(frame), HostLoomProtocolError);
    }
});

test("rejects malformed client frames and application payloads", () => {
    assert.throws(
        () => encodeClientFrame({ kind: "cancel", streamId: HOSTLOOM_SESSION_STREAM }),
        HostLoomProtocolError,
    );
    assert.throws(
        () => encodeClientFrame(untypedClientFrame({ kind: "cancel", streamId: "not-hex" })),
        HostLoomProtocolError,
    );
    assert.throws(
        () =>
            encodeClientFrame(
                untypedClientFrame({ kind: "response", streamId: STREAM, payload: "e30=" }),
            ),
        HostLoomProtocolError,
    );
    assert.throws(() => encodeJsonPayload(undefined), HostLoomProtocolError);
    assert.throws(() => decodeJsonPayload("not base64"), HostLoomProtocolError);
});
