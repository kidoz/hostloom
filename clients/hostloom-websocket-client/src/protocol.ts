export const HOSTLOOM_JSON_V1_SUBPROTOCOL = "hostloom.json.v1";

export const HOSTLOOM_JSON_V1_FRAME_KINDS = [
    "welcome",
    "request",
    "response",
    "fault",
    "cancel",
    "subscribe",
    "subscribed",
    "event",
    "credit",
    "ack",
    "unsubscribe",
    "complete",
    "ping",
    "pong",
] as const;

export type HostLoomFrameKind = (typeof HOSTLOOM_JSON_V1_FRAME_KINDS)[number];

/** The reserved identifier that addresses the session itself rather than one of its streams. */
export const HOSTLOOM_SESSION_STREAM = "00000000000000000000000000000000";

const IDENTIFIER_PATTERN = /^[0-9a-f]{32}$/;

/**
 * Allocates a random stream identifier in the 32 hex digit form the protocol carries. The bytes
 * are laid out as an RFC 4122 version 4 identifier so a gateway log and a browser log name the
 * same stream. `crypto.randomUUID` is not used because it requires a secure context.
 */
export function newStreamId(): string {
    const bytes = new Uint8Array(16);
    crypto.getRandomValues(bytes);
    bytes[6] = (bytes[6]! & 0x0f) | 0x40;
    bytes[8] = (bytes[8]! & 0x3f) | 0x80;

    let identifier = "";
    for (const byte of bytes) {
        identifier += byte.toString(16).padStart(2, "0");
    }

    return identifier;
}

interface FrameBase<TKind extends HostLoomFrameKind> {
    readonly kind: TKind;
    readonly streamId: string;
}

export interface WelcomeFrame extends FrameBase<"welcome"> {
    readonly streamId: typeof HOSTLOOM_SESSION_STREAM;
    readonly sessionId: string;
    readonly credit: number;
    readonly maximumMessageSize: number;
    readonly maximumConcurrentRequests: number;
}

export interface RequestFrame extends FrameBase<"request"> {
    readonly operation: string;
    readonly timeoutMilliseconds?: number;
    readonly payload: string;
}

export interface ResponseFrame extends FrameBase<"response"> {
    readonly payload: string;
}

export interface FaultFrame extends FrameBase<"fault"> {
    readonly code: string;
    readonly message: string;
}

export interface CancelFrame extends FrameBase<"cancel"> {}

export interface SubscribeFrame extends FrameBase<"subscribe"> {
    readonly topic: string;
    readonly key?: string;
    readonly credit: number;
}

export interface SubscribedFrame extends FrameBase<"subscribed"> {
    readonly topic: string;
    readonly key?: string;
    readonly credit: number;
}

export interface EventFrame extends FrameBase<"event"> {
    readonly topic?: string;
    readonly key?: string;
    readonly sequence: number;
    readonly eventId: string;
    readonly payload: string;
}

export interface CreditFrame extends FrameBase<"credit"> {
    readonly credit: number;
}

export interface AckFrame extends FrameBase<"ack"> {
    readonly sequence: number;
}

export interface UnsubscribeFrame extends FrameBase<"unsubscribe"> {}

export interface CompleteFrame extends FrameBase<"complete"> {}

export interface PingFrame extends FrameBase<"ping"> {}

export interface PongFrame extends FrameBase<"pong"> {}

export type ClientFrame =
    | RequestFrame
    | CancelFrame
    | SubscribeFrame
    | CreditFrame
    | AckFrame
    | UnsubscribeFrame
    | PingFrame;

export type ServerFrame =
    | WelcomeFrame
    | ResponseFrame
    | FaultFrame
    | SubscribedFrame
    | EventFrame
    | CompleteFrame
    | PongFrame;

export type HostLoomFrame = ClientFrame | ServerFrame;

export class HostLoomProtocolError extends Error {
    public constructor(message: string, options?: ErrorOptions) {
        super(message, options);
        this.name = "HostLoomProtocolError";
    }
}

const FRAME_KIND_SET: ReadonlySet<string> = new Set(HOSTLOOM_JSON_V1_FRAME_KINDS);
const CLIENT_KIND_SET: ReadonlySet<HostLoomFrameKind> = new Set([
    "request",
    "cancel",
    "subscribe",
    "credit",
    "ack",
    "unsubscribe",
    "ping",
]);
const SERVER_KIND_SET: ReadonlySet<HostLoomFrameKind> = new Set([
    "welcome",
    "response",
    "fault",
    "subscribed",
    "event",
    "complete",
    "pong",
]);

const FRAME_PROPERTY_ORDER = [
    "kind",
    "streamId",
    "sessionId",
    "operation",
    "topic",
    "key",
    "timeoutMilliseconds",
    "credit",
    "sequence",
    "eventId",
    "code",
    "message",
    "payload",
    "maximumMessageSize",
    "maximumConcurrentRequests",
] as const;

type FrameProperty = (typeof FRAME_PROPERTY_ORDER)[number];
type JsonObject = Record<string, unknown>;

const FRAME_PROPERTY_SET: ReadonlySet<string> = new Set(FRAME_PROPERTY_ORDER);
const STRING_PROPERTIES: ReadonlySet<FrameProperty> = new Set([
    "operation",
    "topic",
    "key",
    "code",
    "message",
    "payload",
]);
const IDENTIFIER_PROPERTIES: ReadonlySet<FrameProperty> = new Set([
    "streamId",
    "sessionId",
    "eventId",
]);

/** Decodes and validates one server-to-client `hostloom.json.v1` frame. */
export function decodeServerFrame(json: string): ServerFrame {
    if (typeof json !== "string") {
        throw new HostLoomProtocolError("The WebSocket frame must be a JSON string.");
    }

    let value: unknown;
    try {
        value = JSON.parse(json);
    } catch (error) {
        throw new HostLoomProtocolError("The WebSocket frame is not valid JSON.", {
            cause: error,
        });
    }

    const frame = validateFrame(value, "server");
    return canonicalize(frame) as unknown as ServerFrame;
}

/** Validates and encodes one client-to-server `hostloom.json.v1` frame. */
export function encodeClientFrame(frame: ClientFrame): string {
    const validated = validateFrame(frame, "client");
    return JSON.stringify(canonicalize(validated));
}

/** Serializes application JSON as the Base64 payload carried by JSON-v1 frames. */
export function encodeJsonPayload(value: unknown): string {
    let json: string | undefined;
    try {
        json = JSON.stringify(value);
    } catch (error) {
        throw new HostLoomProtocolError("The application payload is not JSON serializable.", {
            cause: error,
        });
    }

    if (json === undefined) {
        throw new HostLoomProtocolError("The application payload is not JSON serializable.");
    }

    const bytes = new TextEncoder().encode(json);
    let binary = "";
    const chunkSize = 0x8000;
    for (let offset = 0; offset < bytes.length; offset += chunkSize) {
        binary += String.fromCharCode(...bytes.subarray(offset, offset + chunkSize));
    }

    return btoa(binary);
}

/** Decodes a Base64 JSON-v1 application payload. The caller supplies its application type. */
export function decodeJsonPayload<T = unknown>(payload: string): T {
    if (typeof payload !== "string") {
        throw new HostLoomProtocolError("The application payload must be a Base64 string.");
    }

    try {
        const binary = atob(payload);
        const bytes = Uint8Array.from(binary, (character) => character.charCodeAt(0));
        const json = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
        return JSON.parse(json) as T;
    } catch (error) {
        throw new HostLoomProtocolError(
            "The application payload is not valid Base64-encoded UTF-8 JSON.",
            { cause: error },
        );
    }
}

function validateFrame(value: unknown, direction: "client" | "server"): JsonObject {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
        throw new HostLoomProtocolError("The WebSocket frame must be a JSON object.");
    }

    const frame = value as JsonObject;
    for (const property of Object.keys(frame)) {
        if (!FRAME_PROPERTY_SET.has(property)) {
            throw new HostLoomProtocolError(
                `The WebSocket frame property '${property}' is unknown.`,
            );
        }
    }

    const kind = requireString(frame, "kind");
    if (!FRAME_KIND_SET.has(kind)) {
        throw new HostLoomProtocolError(`The WebSocket frame kind '${kind}' is unknown.`);
    }

    const typedKind = kind as HostLoomFrameKind;
    const allowedKinds = direction === "client" ? CLIENT_KIND_SET : SERVER_KIND_SET;
    if (!allowedKinds.has(typedKind)) {
        throw new HostLoomProtocolError(
            `The WebSocket frame kind '${kind}' is not valid in the ${direction} direction.`,
        );
    }

    validateProvidedProperties(frame);
    const streamId = requireIdentifier(frame, "streamId");

    switch (typedKind) {
        case "welcome":
            if (streamId !== HOSTLOOM_SESSION_STREAM) {
                throw new HostLoomProtocolError(
                    "A welcome frame must use the reserved session stream identifier.",
                );
            }
            requireIdentifier(frame, "sessionId");
            requireInteger(frame, "credit", 0);
            requireInteger(frame, "maximumMessageSize", 1);
            requireInteger(frame, "maximumConcurrentRequests", 1);
            break;
        case "request":
            requireStream(streamId, typedKind);
            requireString(frame, "operation");
            requireString(frame, "payload");
            break;
        case "response":
            requireStream(streamId, typedKind);
            requireString(frame, "payload");
            break;
        case "fault":
            requireString(frame, "code");
            requireString(frame, "message");
            break;
        case "cancel":
        case "unsubscribe":
        case "complete":
        case "ping":
        case "pong":
            requireStream(streamId, typedKind);
            break;
        case "subscribe":
        case "subscribed":
            requireStream(streamId, typedKind);
            requireString(frame, "topic");
            requireInteger(frame, "credit", 0);
            break;
        case "event":
            requireStream(streamId, typedKind);
            requireInteger(frame, "sequence", 0);
            requireIdentifier(frame, "eventId");
            requireString(frame, "payload");
            break;
        case "credit":
            requireStream(streamId, typedKind);
            requireInteger(frame, "credit", 0);
            break;
        case "ack":
            requireStream(streamId, typedKind);
            requireInteger(frame, "sequence", 0);
            break;
    }

    return frame;
}

function validateProvidedProperties(frame: JsonObject): void {
    for (const property of STRING_PROPERTIES) {
        if (Object.hasOwn(frame, property) && typeof frame[property] !== "string") {
            throw new HostLoomProtocolError(
                `The WebSocket frame property '${property}' must be a string.`,
            );
        }
    }

    for (const property of IDENTIFIER_PROPERTIES) {
        if (Object.hasOwn(frame, property)) {
            requireIdentifier(frame, property);
        }
    }

    validateOptionalInteger(frame, "timeoutMilliseconds", 1);
    validateOptionalInteger(frame, "credit", 0);
    validateOptionalInteger(frame, "sequence", 0);
    validateOptionalInteger(frame, "maximumMessageSize", 1);
    validateOptionalInteger(frame, "maximumConcurrentRequests", 1);
}

function validateOptionalInteger(
    frame: JsonObject,
    property: FrameProperty,
    minimum: number,
): void {
    if (Object.hasOwn(frame, property)) {
        assertInteger(frame[property], property, minimum);
    }
}

function requireString(frame: JsonObject, property: FrameProperty): string {
    if (!Object.hasOwn(frame, property)) {
        throw new HostLoomProtocolError(`The WebSocket frame property '${property}' is required.`);
    }

    const value = frame[property];
    if (typeof value !== "string") {
        throw new HostLoomProtocolError(
            `The WebSocket frame property '${property}' must be a string.`,
        );
    }

    return value;
}

function requireInteger(frame: JsonObject, property: FrameProperty, minimum: number): number {
    if (!Object.hasOwn(frame, property)) {
        throw new HostLoomProtocolError(`The WebSocket frame property '${property}' is required.`);
    }

    return assertInteger(frame[property], property, minimum);
}

function assertInteger(value: unknown, property: FrameProperty, minimum: number): number {
    if (!Number.isSafeInteger(value) || (value as number) < minimum) {
        throw new HostLoomProtocolError(
            `The WebSocket frame property '${property}' must be a safe integer greater than or equal to ${minimum}.`,
        );
    }

    return value as number;
}

function requireIdentifier(frame: JsonObject, property: FrameProperty): string {
    const value = requireString(frame, property);
    if (!IDENTIFIER_PATTERN.test(value)) {
        throw new HostLoomProtocolError(
            `The WebSocket frame property '${property}' must be 32 lowercase hexadecimal digits.`,
        );
    }

    return value;
}

function requireStream(streamId: string, kind: HostLoomFrameKind): void {
    if (streamId === HOSTLOOM_SESSION_STREAM) {
        throw new HostLoomProtocolError(`A ${kind} frame must address a stream, not the session.`);
    }
}

function canonicalize(frame: JsonObject): JsonObject {
    const result: JsonObject = {};
    for (const property of FRAME_PROPERTY_ORDER) {
        if (Object.hasOwn(frame, property)) {
            result[property] = frame[property];
        }
    }

    return result;
}
