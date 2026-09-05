import {
    decodeServerFrame,
    encodeClientFrame,
    HOSTLOOM_JSON_V1_SUBPROTOCOL,
    HostLoomProtocolError,
    newStreamId,
    type ClientFrame,
    type FaultFrame,
    type ResponseFrame,
    type ServerFrame,
    type WelcomeFrame,
} from "./protocol.js";
import {
    HostLoomSubscriptionCanceledError,
    SubscriptionController,
    type HostLoomSubscribeOptions,
    type HostLoomSubscription,
} from "./subscription.js";

export type HostLoomConnectionState =
    "disconnected" | "connecting" | "connected" | "reconnecting" | "closing";

export interface HostLoomCloseInfo {
    readonly code: number;
    readonly reason: string;
    readonly wasClean: boolean;
}

export interface HostLoomConnectionStateChange {
    readonly previousState: HostLoomConnectionState;
    readonly state: HostLoomConnectionState;
    readonly close?: HostLoomCloseInfo;
    readonly error?: Error;
}

export type HostLoomConnectionStateListener = (change: HostLoomConnectionStateChange) => void;

export type HostLoomServerFrameListener = (frame: Exclude<ServerFrame, WelcomeFrame>) => void;

export interface HostLoomWebSocketEventMap {
    readonly open: Event;
    readonly message: MessageEvent<unknown>;
    readonly error: Event;
    readonly close: CloseEvent;
}

/** The browser WebSocket surface used by the connection and deterministic test doubles. */
export interface HostLoomWebSocket {
    readonly protocol: string;
    send(data: string): void;
    close(code?: number, reason?: string): void;
    addEventListener<TKey extends keyof HostLoomWebSocketEventMap>(
        type: TKey,
        listener: (event: HostLoomWebSocketEventMap[TKey]) => void,
    ): void;
    removeEventListener<TKey extends keyof HostLoomWebSocketEventMap>(
        type: TKey,
        listener: (event: HostLoomWebSocketEventMap[TKey]) => void,
    ): void;
}

export type HostLoomWebSocketFactory = (
    url: string | URL,
    protocols: readonly string[],
) => HostLoomWebSocket;

export interface HostLoomReconnectOptions {
    /** Initial retry delay. Defaults to 1,000 milliseconds. */
    readonly initialDelayMilliseconds?: number;
    /** Maximum jittered retry delay. Defaults to 30,000 milliseconds. */
    readonly maximumDelayMilliseconds?: number;
    /** Exponential delay multiplier. Defaults to 2. */
    readonly multiplier?: number;
    /** Symmetric random variation from 0 (none) to less than 1. Defaults to 0.2. */
    readonly jitterRatio?: number;
    /** Refreshes browser credentials after close code 1008 and before a replacement socket opens. */
    readonly refreshCredentials?: (close: HostLoomCloseInfo) => void | Promise<void>;
}

export interface HostLoomConnectionOptions {
    readonly webSocketFactory?: HostLoomWebSocketFactory;
    /**
     * Allocates the identifier for each request and subscription stream. It defaults to a random
     * one; supply it to derive a stream from an application trace identifier instead. Every value
     * must be 32 lowercase hex digits and must not repeat within a session.
     */
    readonly streamIdFactory?: () => string;
    /** Enables automatic reconnect when present. Omit it to retain manual reconnect behavior. */
    readonly reconnect?: HostLoomReconnectOptions;
}

export interface HostLoomRequestOptions {
    readonly signal?: AbortSignal;
    readonly timeoutMilliseconds?: number;
}

export class HostLoomConnectionError extends Error {
    public constructor(message: string, options?: ErrorOptions) {
        super(message, options);
        this.name = "HostLoomConnectionError";
    }
}

export class HostLoomConnectionClosedError extends HostLoomConnectionError {
    public readonly close: HostLoomCloseInfo;

    public constructor(close: HostLoomCloseInfo) {
        super(
            close.reason.length === 0
                ? `The WebSocket closed with code ${close.code}.`
                : `The WebSocket closed with code ${close.code}: ${close.reason}`,
        );
        this.name = "HostLoomConnectionClosedError";
        this.close = close;
    }
}

export class HostLoomRemoteFaultError extends Error {
    public readonly streamId: string;
    public readonly code: string;

    public constructor(frame: FaultFrame) {
        super(frame.message);
        this.name = "HostLoomRemoteFaultError";
        this.streamId = frame.streamId;
        this.code = frame.code;
    }
}

export class HostLoomRequestCanceledError extends Error {
    public constructor(options?: ErrorOptions) {
        super("The request was canceled by the caller.", options);
        this.name = "HostLoomRequestCanceledError";
    }
}

export class HostLoomRequestCapacityError extends HostLoomConnectionError {
    public readonly limit: number;

    public constructor(limit: number) {
        super(`The connection already has its maximum of ${limit} active requests.`);
        this.name = "HostLoomRequestCapacityError";
        this.limit = limit;
    }
}

export class HostLoomMessageSizeError extends HostLoomConnectionError {
    public readonly actualSize: number;
    public readonly maximumSize: number;

    public constructor(actualSize: number, maximumSize: number) {
        super(
            `The encoded client frame is ${actualSize} UTF-8 bytes, exceeding the gateway maximum of ${maximumSize}.`,
        );
        this.name = "HostLoomMessageSizeError";
        this.actualSize = actualSize;
        this.maximumSize = maximumSize;
    }
}

interface PendingRequest {
    readonly resolve: (payload: string) => void;
    readonly reject: (error: Error) => void;
    readonly signal: AbortSignal | undefined;
    readonly abortListener: (() => void) | undefined;
    settled: boolean;
}

interface QueuedConnect {
    readonly promise: Promise<WelcomeFrame>;
    readonly resolve: (welcome: WelcomeFrame) => void;
    readonly reject: (error: Error) => void;
}

interface ResolvedReconnectOptions {
    readonly initialDelayMilliseconds: number;
    readonly maximumDelayMilliseconds: number;
    readonly multiplier: number;
    readonly jitterRatio: number;
    readonly refreshCredentials: ((close: HostLoomCloseInfo) => void | Promise<void>) | undefined;
}

type CloseDisposition = "manual" | "protocol";

/**
 * Owns one JSON-v1 WebSocket connection with optional automatic reconnect and resubscription.
 *
 * A connection becomes `connected` only after the server selects the expected subprotocol and
 * sends a valid welcome frame. Requests are never replayed after a connection loss.
 */
export class HostLoomConnection {
    readonly #url: string | URL;
    readonly #webSocketFactory: HostLoomWebSocketFactory;
    readonly #streamIdFactory: () => string;
    readonly #reconnect: ResolvedReconnectOptions | undefined;
    readonly #stateListeners = new Set<HostLoomConnectionStateListener>();
    readonly #frameListeners = new Set<HostLoomServerFrameListener>();
    readonly #pendingRequests = new Map<string, PendingRequest>();
    readonly #subscriptions = new Map<string, SubscriptionController>();
    readonly #logicalSubscriptions = new Set<SubscriptionController>();
    readonly #manualSubscriptions = new Set<string>();
    readonly #orphanedSubscriptions = new Set<string>();

    #state: HostLoomConnectionState = "disconnected";
    #socket: HostLoomWebSocket | undefined;
    #opened = false;
    #welcome: WelcomeFrame | undefined;
    #lastClose: HostLoomCloseInfo | undefined;
    #connectPromise: Promise<WelcomeFrame> | undefined;
    #resolveConnect: ((welcome: WelcomeFrame) => void) | undefined;
    #rejectConnect: ((error: Error) => void) | undefined;
    #reconnectPromise: Promise<WelcomeFrame> | undefined;
    #resolveReconnect: ((welcome: WelcomeFrame) => void) | undefined;
    #rejectReconnect: ((error: Error) => void) | undefined;
    #queuedConnect: QueuedConnect | undefined;
    #reconnectTimer: number | undefined;
    #nextReconnectDelay: number;
    #closeDisposition: CloseDisposition | undefined;
    #reconnectClose: HostLoomCloseInfo | undefined;

    public constructor(url: string | URL, options: HostLoomConnectionOptions = {}) {
        this.#url = url;
        this.#webSocketFactory = options.webSocketFactory ?? createBrowserWebSocket;
        this.#streamIdFactory = options.streamIdFactory ?? newStreamId;
        this.#reconnect = resolveReconnectOptions(options.reconnect);
        this.#nextReconnectDelay =
            this.#reconnect?.initialDelayMilliseconds ?? DEFAULT_RECONNECT_INITIAL_DELAY;
    }

    public get state(): HostLoomConnectionState {
        return this.#state;
    }

    public get welcome(): WelcomeFrame | undefined {
        return this.#welcome;
    }

    public get lastClose(): HostLoomCloseInfo | undefined {
        return this.#lastClose;
    }

    public connect(): Promise<WelcomeFrame> {
        if (this.#state === "connected") {
            return Promise.resolve(this.#welcome as WelcomeFrame);
        }

        if (this.#state === "connecting") {
            return (this.#reconnectPromise ?? this.#connectPromise) as Promise<WelcomeFrame>;
        }

        if (this.#state === "reconnecting") {
            return this.#reconnectPromise as Promise<WelcomeFrame>;
        }

        if (this.#state === "closing") {
            if (this.#reconnectPromise !== undefined && this.#closeDisposition === undefined) {
                return this.#reconnectPromise;
            }

            if (this.#closeDisposition === "manual") {
                return this.#queueConnectAfterClose();
            }

            return Promise.reject(
                new HostLoomConnectionError("The WebSocket is still closing and cannot connect."),
            );
        }

        this.#closeDisposition = undefined;
        this.#resetReconnectDelay();
        return this.#beginConnect();
    }

    #beginConnect(): Promise<WelcomeFrame> {
        let socket: HostLoomWebSocket;
        try {
            socket = this.#webSocketFactory(this.#url, [HOSTLOOM_JSON_V1_SUBPROTOCOL]);
        } catch (error) {
            const connectionError = new HostLoomConnectionError(
                "The WebSocket could not be created.",
                {
                    cause: error,
                },
            );
            return Promise.reject(connectionError);
        }

        this.#lastClose = undefined;
        this.#socket = socket;
        this.#opened = false;
        const connectPromise = new Promise<WelcomeFrame>((resolve, reject) => {
            this.#resolveConnect = resolve;
            this.#rejectConnect = reject;
        });
        this.#connectPromise = connectPromise;
        this.#attach(socket);
        this.#transition("connecting");
        return connectPromise;
    }

    /** Sends one validated client frame. The connection must have received its welcome frame. */
    public send(frame: ClientFrame): void {
        this.#sendClientFrame(frame, true);
    }

    /**
     * Sends one request and resolves with its opaque Base64 response payload.
     *
     * The optional timeout is enforced by the gateway. Aborting sends a `cancel` frame and rejects
     * locally without waiting for the server's terminal fault.
     */
    public request(
        operation: string,
        payload: string,
        options: HostLoomRequestOptions = {},
    ): Promise<string> {
        if (this.#state !== "connected" || this.#welcome === undefined) {
            return Promise.reject(
                new HostLoomConnectionError("A request can be sent only while connected."),
            );
        }

        if (typeof operation !== "string" || operation.trim().length === 0) {
            return Promise.reject(
                new TypeError("The request operation must be a non-empty string."),
            );
        }

        if (options.signal?.aborted === true) {
            return Promise.reject(canceledBy(options.signal));
        }

        const limit = this.#welcome.maximumConcurrentRequests;
        if (this.#pendingRequests.size >= limit) {
            return Promise.reject(new HostLoomRequestCapacityError(limit));
        }

        let streamId: string;
        try {
            streamId = this.#allocateStreamId();
        } catch (error) {
            return Promise.reject(asError(error));
        }

        let resolveRequest: (payload: string) => void;
        let rejectRequest: (error: Error) => void;
        const requestPromise = new Promise<string>((resolve, reject) => {
            resolveRequest = resolve;
            rejectRequest = reject;
        });
        const abortListener =
            options.signal === undefined
                ? undefined
                : () => this.#cancelRequest(streamId, options.signal as AbortSignal);
        const pending: PendingRequest = {
            resolve: resolveRequest!,
            reject: rejectRequest!,
            signal: options.signal,
            abortListener,
            settled: false,
        };

        this.#pendingRequests.set(streamId, pending);
        options.signal?.addEventListener("abort", abortListener as () => void, { once: true });

        try {
            this.#sendClientFrame({
                kind: "request",
                streamId,
                operation,
                payload,
                ...(options.timeoutMilliseconds === undefined
                    ? {}
                    : { timeoutMilliseconds: options.timeoutMilliseconds }),
            });
        } catch (error) {
            this.#rejectRequest(streamId, asError(error));
        }

        return requestPromise;
    }

    /**
     * Subscribes to one topic and resolves after the gateway confirms it.
     *
     * Event credit is replenished to the requested amount whenever the remaining credit reaches the
     * low watermark. Replenishment pauses until at least one event listener is attached.
     */
    public subscribe(
        topic: string,
        options: HostLoomSubscribeOptions,
    ): Promise<HostLoomSubscription> {
        if (this.#state !== "connected" || this.#welcome === undefined) {
            return Promise.reject(
                new HostLoomConnectionError("A subscription can be started only while connected."),
            );
        }

        if (typeof topic !== "string" || topic.trim().length === 0) {
            return Promise.reject(new TypeError("The subscription topic must not be empty."));
        }

        if (options.signal?.aborted === true) {
            return Promise.reject(subscriptionCanceledBy(options.signal));
        }

        if (
            !Number.isSafeInteger(options.credit) ||
            options.credit <= 0 ||
            options.credit > this.#welcome.credit
        ) {
            return Promise.reject(
                new RangeError(
                    "The subscription credit must be a positive safe integer no greater than " +
                        `${this.#welcome.credit}.`,
                ),
            );
        }

        if (options.key !== undefined && typeof options.key !== "string") {
            return Promise.reject(
                new TypeError("The subscription key must be a string when provided."),
            );
        }

        if (options.key !== undefined && options.key.length > 256) {
            return Promise.reject(
                new RangeError("The subscription key must not exceed 256 characters."),
            );
        }

        const lowWatermark = options.lowWatermark ?? Math.floor(options.credit / 2);
        if (
            !Number.isSafeInteger(lowWatermark) ||
            lowWatermark < 0 ||
            lowWatermark >= options.credit
        ) {
            return Promise.reject(
                new RangeError(
                    "The subscription low watermark must be a non-negative safe integer less than " +
                        "its credit.",
                ),
            );
        }

        let streamId: string;
        try {
            streamId = this.#allocateStreamId();
        } catch (error) {
            return Promise.reject(asError(error));
        }

        let controller: SubscriptionController;
        controller = new SubscriptionController({
            streamId,
            topic,
            key: options.key,
            credit: options.credit,
            lowWatermark,
            signal: options.signal,
            send: (frame) => this.#sendClientFrame(frame),
            onProtocolError: (error) => this.#failProtocol(error),
            onTerminal: () => this.#removeSubscription(controller),
        });
        this.#logicalSubscriptions.add(controller);
        this.#subscriptions.set(streamId, controller);
        controller.start();
        return controller.ready;
    }

    /** Starts a caller-requested close. A later close event transitions to `disconnected`. */
    public close(code = 1000, reason = ""): void {
        validateClose(code, reason);

        if (this.#state === "disconnected") {
            return;
        }

        if (this.#state === "closing") {
            if (this.#closeDisposition !== "protocol") {
                this.#closeDisposition = "manual";
            }
            return;
        }

        this.#closeDisposition = "manual";
        if (this.#state === "reconnecting") {
            const close: HostLoomCloseInfo = { code, reason, wasClean: true };
            const error = new HostLoomConnectionClosedError(close);
            this.#cancelReconnectTimer();
            this.#rejectReconnectPromise(error);
            this.#rejectAllSubscriptions(error);
            this.#lastClose = close;
            this.#transition("disconnected", { close });
            this.#closeDisposition = undefined;
            return;
        }

        const previousState = this.#state;
        const socket = this.#socket;
        this.#transition("closing");
        if (!this.#ownsClosingSocket(socket)) {
            return;
        }

        try {
            socket.close(code, reason);
        } catch (error) {
            const connectionError = new HostLoomConnectionError(
                "The WebSocket close could not start.",
                {
                    cause: error,
                },
            );
            this.#closeDisposition = undefined;
            this.#rejectQueuedConnect(connectionError);
            this.#transition(previousState, { error: connectionError });
            throw connectionError;
        }
    }

    public onStateChange(listener: HostLoomConnectionStateListener): () => void {
        this.#stateListeners.add(listener);
        return () => this.#stateListeners.delete(listener);
    }

    /** Observes validated server frames received after the welcome frame. */
    public onFrame(listener: HostLoomServerFrameListener): () => void {
        this.#frameListeners.add(listener);
        return () => this.#frameListeners.delete(listener);
    }

    readonly #handleOpen = (event: Event): void => {
        void event;
        const socket = this.#socket;
        if (socket === undefined || this.#state !== "connecting") {
            return;
        }

        this.#opened = true;
        if (socket.protocol !== HOSTLOOM_JSON_V1_SUBPROTOCOL) {
            this.#failProtocol(
                new HostLoomProtocolError(
                    `The server selected '${socket.protocol || "no subprotocol"}' instead of '${HOSTLOOM_JSON_V1_SUBPROTOCOL}'.`,
                ),
            );
        }
    };

    readonly #handleMessage = (event: MessageEvent<unknown>): void => {
        if (this.#socket === undefined || this.#state === "closing") {
            return;
        }

        if (!this.#opened) {
            this.#failProtocol(
                new HostLoomProtocolError("A frame arrived before the WebSocket opened."),
            );
            return;
        }

        if (typeof event.data !== "string") {
            this.#failProtocol(
                new HostLoomProtocolError("The server WebSocket frame must be text."),
            );
            return;
        }

        let frame: ServerFrame;
        try {
            frame = decodeServerFrame(event.data);
        } catch (error) {
            this.#failProtocol(asError(error));
            return;
        }

        if (this.#state === "connecting") {
            if (frame.kind !== "welcome") {
                this.#failProtocol(
                    new HostLoomProtocolError("The first server frame must be a welcome frame."),
                );
                return;
            }

            this.#welcome = frame;
            const subscriptionsToRestart = [...this.#logicalSubscriptions].filter(
                (subscription) => subscription.canRestart,
            );
            const resolve = this.#resolveConnect;
            this.#clearConnectPromise();
            this.#closeDisposition = undefined;
            this.#resetReconnectDelay();
            resolve?.(frame);
            this.#resolveReconnectPromise(frame);
            const socket = this.#socket;
            this.#transition("connected");
            if (this.state === "connected" && this.#socket === socket) {
                this.#resubscribeAll(frame, subscriptionsToRestart);
            }
            return;
        }

        if (frame.kind === "welcome") {
            this.#failProtocol(
                new HostLoomProtocolError("The server sent more than one welcome frame."),
            );
            return;
        }

        if (frame.kind === "response" || frame.kind === "fault") {
            this.#settleRequest(frame);
        }

        const subscription = this.#subscriptions.get(frame.streamId);
        if (subscription !== undefined) {
            if (frame.kind === "subscribed") {
                subscription.acceptSubscribed(frame);
            } else if (frame.kind === "event") {
                subscription.acceptEvent(frame);
            } else if (frame.kind === "complete") {
                subscription.complete();
            } else if (frame.kind === "fault") {
                subscription.fail(new HostLoomRemoteFaultError(frame));
            }
        } else {
            this.#handleUnroutedSubscriptionFrame(frame);
        }

        if (this.#isClosing()) {
            return;
        }

        for (const listener of this.#frameListeners) {
            notifyListener(listener, frame);
        }
    };

    readonly #handleError = (event: Event): void => {
        void event;
        if (this.#socket === undefined || this.#state === "closing") {
            return;
        }

        const error = new HostLoomConnectionError("The WebSocket reported a connection error.");
        this.#rejectPendingConnect(error);
        this.#rejectAllRequests(error);
        if (this.#shouldPreserveSubscriptions()) {
            this.#suspendAllSubscriptions(error);
        } else {
            this.#rejectAllSubscriptions(error);
        }
        this.#transition("closing", { error });
        this.#closeAfterFailure();
    };

    readonly #handleClose = (event: CloseEvent): void => {
        const socket = this.#socket;
        if (socket === undefined) {
            return;
        }

        const close: HostLoomCloseInfo = {
            code: event.code,
            reason: event.reason,
            wasClean: event.wasClean,
        };

        this.#detach(socket);
        const closeError = new HostLoomConnectionClosedError(close);
        const disposition = this.#closeDisposition;
        this.#closeDisposition = undefined;
        this.#rejectPendingConnect(closeError);
        this.#rejectAllRequests(closeError);
        this.#socket = undefined;
        this.#opened = false;
        this.#welcome = undefined;
        this.#manualSubscriptions.clear();
        this.#orphanedSubscriptions.clear();
        this.#lastClose = close;
        if (this.#canReconnect(close, disposition)) {
            this.#suspendAllSubscriptions(closeError);
            this.#ensureReconnectPromise();
            this.#transition("reconnecting", { close });
            this.#scheduleReconnect(close);
        } else {
            this.#cancelReconnectTimer();
            this.#rejectReconnectPromise(closeError);
            this.#rejectAllSubscriptions(closeError);
            this.#transition("disconnected", { close });
            this.#resumeQueuedConnect();
        }
    };

    #attach(socket: HostLoomWebSocket): void {
        socket.addEventListener("open", this.#handleOpen);
        socket.addEventListener("message", this.#handleMessage);
        socket.addEventListener("error", this.#handleError);
        socket.addEventListener("close", this.#handleClose);
    }

    #ownsClosingSocket(socket: HostLoomWebSocket | undefined): socket is HostLoomWebSocket {
        return socket !== undefined && this.#state === "closing" && this.#socket === socket;
    }

    #isClosing(): boolean {
        return this.#state === "closing";
    }

    #detach(socket: HostLoomWebSocket): void {
        socket.removeEventListener("open", this.#handleOpen);
        socket.removeEventListener("message", this.#handleMessage);
        socket.removeEventListener("error", this.#handleError);
        socket.removeEventListener("close", this.#handleClose);
    }

    #failProtocol(error: Error): void {
        this.#closeDisposition = "protocol";
        this.#rejectPendingConnect(error);
        this.#rejectReconnectPromise(error);
        this.#rejectAllRequests(error);
        this.#rejectAllSubscriptions(error);
        this.#transition("closing", { error });
        this.#closeAfterFailure();
    }

    #closeAfterFailure(): void {
        try {
            this.#socket?.close();
        } catch {
            // A browser may already have moved the native socket to a terminal state.
        }
    }

    #rejectPendingConnect(error: Error): void {
        const reject = this.#rejectConnect;
        this.#clearConnectPromise();
        reject?.(error);
    }

    #clearConnectPromise(): void {
        this.#connectPromise = undefined;
        this.#resolveConnect = undefined;
        this.#rejectConnect = undefined;
    }

    #queueConnectAfterClose(): Promise<WelcomeFrame> {
        if (this.#queuedConnect !== undefined) {
            return this.#queuedConnect.promise;
        }

        let resolveConnect: (welcome: WelcomeFrame) => void;
        let rejectConnect: (error: Error) => void;
        const promise = new Promise<WelcomeFrame>((resolve, reject) => {
            resolveConnect = resolve;
            rejectConnect = reject;
        });
        this.#queuedConnect = {
            promise,
            resolve: resolveConnect!,
            reject: rejectConnect!,
        };
        return promise;
    }

    #resumeQueuedConnect(): void {
        const queued = this.#queuedConnect;
        if (queued === undefined) {
            return;
        }

        this.#queuedConnect = undefined;
        void this.connect().then(queued.resolve, queued.reject);
    }

    #rejectQueuedConnect(error: Error): void {
        const queued = this.#queuedConnect;
        this.#queuedConnect = undefined;
        queued?.reject(error);
    }

    #ensureReconnectPromise(): void {
        if (this.#reconnectPromise !== undefined) {
            return;
        }

        this.#reconnectPromise = new Promise<WelcomeFrame>((resolve, reject) => {
            this.#resolveReconnect = resolve;
            this.#rejectReconnect = reject;
        });
        void this.#reconnectPromise.catch(() => undefined);
    }

    #resolveReconnectPromise(welcome: WelcomeFrame): void {
        const resolve = this.#resolveReconnect;
        this.#clearReconnectPromise();
        resolve?.(welcome);
    }

    #rejectReconnectPromise(error: Error): void {
        const reject = this.#rejectReconnect;
        this.#clearReconnectPromise();
        reject?.(error);
    }

    #clearReconnectPromise(): void {
        this.#reconnectPromise = undefined;
        this.#resolveReconnect = undefined;
        this.#rejectReconnect = undefined;
        this.#reconnectClose = undefined;
    }

    #scheduleReconnect(close: HostLoomCloseInfo): void {
        const reconnect = this.#reconnect;
        if (reconnect === undefined || this.#state !== "reconnecting") {
            return;
        }

        this.#cancelReconnectTimer();
        this.#reconnectClose = close;
        const factor = 1 - reconnect.jitterRatio + 2 * reconnect.jitterRatio * Math.random();
        const delay = Math.min(
            reconnect.maximumDelayMilliseconds,
            Math.round(this.#nextReconnectDelay * factor),
        );
        this.#nextReconnectDelay = Math.min(
            reconnect.maximumDelayMilliseconds,
            this.#nextReconnectDelay * reconnect.multiplier,
        );
        this.#reconnectTimer = globalThis.setTimeout(() => {
            this.#reconnectTimer = undefined;
            void this.#attemptReconnect(close);
        }, delay);
    }

    async #attemptReconnect(close: HostLoomCloseInfo): Promise<void> {
        if (this.#state !== "reconnecting" || this.#reconnect === undefined) {
            return;
        }

        const reconnectPromise = this.#reconnectPromise;
        if (close.code === 1008) {
            try {
                await this.#reconnect.refreshCredentials?.(close);
            } catch (error) {
                if (this.#state !== "reconnecting" || this.#reconnectPromise !== reconnectPromise) {
                    return;
                }

                const refreshError = new HostLoomConnectionError(
                    "Credentials could not be refreshed after the session expired.",
                    { cause: error },
                );
                this.#rejectReconnectPromise(refreshError);
                this.#rejectAllSubscriptions(refreshError);
                this.#transition("disconnected", { close, error: refreshError });
                return;
            }

            if (this.#state !== "reconnecting" || this.#reconnectPromise !== reconnectPromise) {
                return;
            }
        }

        try {
            await this.#beginConnect();
        } catch {
            if (
                this.#state === "reconnecting" &&
                this.#reconnectPromise === reconnectPromise &&
                this.#socket === undefined &&
                this.#reconnectTimer === undefined
            ) {
                this.#scheduleReconnect(this.#reconnectClose ?? close);
            }
        }
    }

    #cancelReconnectTimer(): void {
        if (this.#reconnectTimer !== undefined) {
            globalThis.clearTimeout(this.#reconnectTimer);
            this.#reconnectTimer = undefined;
        }
    }

    #resetReconnectDelay(): void {
        this.#nextReconnectDelay =
            this.#reconnect?.initialDelayMilliseconds ?? DEFAULT_RECONNECT_INITIAL_DELAY;
    }

    #canReconnect(close: HostLoomCloseInfo, disposition: CloseDisposition | undefined): boolean {
        return (
            this.#reconnect !== undefined &&
            disposition === undefined &&
            (close.code !== 1008 || this.#reconnect.refreshCredentials !== undefined)
        );
    }

    #shouldPreserveSubscriptions(): boolean {
        return this.#reconnect !== undefined && this.#closeDisposition === undefined;
    }

    #allocateStreamId(): string {
        // Random identifiers are never reused, so a late frame from a closed stream cannot be
        // mistaken for a fresh one after a reconnect.
        for (let attempt = 0; attempt < 4; attempt++) {
            const streamId = this.#streamIdFactory();
            if (
                !this.#pendingRequests.has(streamId) &&
                !this.#subscriptions.has(streamId) &&
                !this.#manualSubscriptions.has(streamId) &&
                !this.#orphanedSubscriptions.has(streamId)
            ) {
                return streamId;
            }
        }

        throw new HostLoomConnectionError(
            "The connection could not allocate an unused stream identifier.",
        );
    }

    #cancelRequest(streamId: string, signal: AbortSignal): void {
        const pending = this.#pendingRequests.get(streamId);
        if (pending === undefined || pending.settled) {
            return;
        }

        pending.settled = true;
        this.#removeAbortListener(pending);
        if (this.#state === "connected") {
            try {
                this.#sendClientFrame({ kind: "cancel", streamId });
            } catch {
                // The request is already canceled locally; the connection lifecycle owns send failures.
            }
        }

        pending.reject(canceledBy(signal));
    }

    #settleRequest(frame: ResponseFrame | FaultFrame): void {
        const pending = this.#takeRequest(frame.streamId);
        if (pending === undefined || pending.settled) {
            return;
        }

        if (frame.kind === "response") {
            pending.resolve(frame.payload);
        } else {
            pending.reject(new HostLoomRemoteFaultError(frame));
        }
    }

    #sendClientFrame(frame: ClientFrame, trackManualSubscription = false): void {
        const socket = this.#socket;
        const welcome = this.#welcome;
        if (this.#state !== "connected" || socket === undefined || welcome === undefined) {
            throw new HostLoomConnectionError("A client frame can be sent only while connected.");
        }

        const encoded = encodeClientFrame(frame);
        const actualSize = UTF8_ENCODER.encode(encoded).byteLength;
        if (actualSize > welcome.maximumMessageSize) {
            throw new HostLoomMessageSizeError(actualSize, welcome.maximumMessageSize);
        }

        const newlyTracked =
            trackManualSubscription &&
            frame.kind === "subscribe" &&
            !this.#manualSubscriptions.has(frame.streamId);
        if (newlyTracked) {
            this.#manualSubscriptions.add(frame.streamId);
        }

        try {
            socket.send(encoded);
        } catch (error) {
            if (newlyTracked) {
                this.#manualSubscriptions.delete(frame.streamId);
            }
            throw new HostLoomConnectionError("The client frame could not be sent.", {
                cause: error,
            });
        }
    }

    #handleUnroutedSubscriptionFrame(frame: Exclude<ServerFrame, WelcomeFrame>): void {
        if (frame.kind === "complete" || frame.kind === "fault") {
            this.#manualSubscriptions.delete(frame.streamId);
            this.#orphanedSubscriptions.delete(frame.streamId);
            return;
        }

        if (
            (frame.kind !== "subscribed" && frame.kind !== "event") ||
            this.#manualSubscriptions.has(frame.streamId) ||
            this.#orphanedSubscriptions.has(frame.streamId)
        ) {
            return;
        }

        this.#orphanedSubscriptions.add(frame.streamId);
        try {
            this.#sendClientFrame({ kind: "unsubscribe", streamId: frame.streamId });
        } catch (error) {
            this.#orphanedSubscriptions.delete(frame.streamId);
            this.#failProtocol(
                new HostLoomProtocolError(
                    "The client could not stop an unowned subscription stream.",
                    { cause: error },
                ),
            );
        }
    }

    #rejectRequest(streamId: string, error: Error): void {
        const pending = this.#takeRequest(streamId);
        if (pending?.settled === false) {
            pending.reject(error);
        }
    }

    #rejectAllRequests(error: Error): void {
        for (const streamId of [...this.#pendingRequests.keys()]) {
            this.#rejectRequest(streamId, error);
        }
    }

    #rejectAllSubscriptions(error: Error): void {
        for (const subscription of [...this.#logicalSubscriptions]) {
            subscription.fail(error);
        }
    }

    #suspendAllSubscriptions(error: Error): void {
        for (const subscription of [...this.#logicalSubscriptions]) {
            subscription.suspend(error);
        }
        this.#subscriptions.clear();
    }

    #resubscribeAll(welcome: WelcomeFrame, subscriptions: readonly SubscriptionController[]): void {
        for (const subscription of subscriptions) {
            if (subscription.credit > welcome.credit) {
                subscription.fail(
                    new HostLoomConnectionError(
                        `The reconnected gateway permits at most ${welcome.credit} subscription credit.`,
                    ),
                );
                continue;
            }

            let streamId: string;
            try {
                streamId = this.#allocateStreamId();
            } catch (error) {
                subscription.fail(asError(error));
                continue;
            }

            this.#subscriptions.set(streamId, subscription);
            if (!subscription.restart(streamId)) {
                this.#subscriptions.delete(streamId);
            }
        }
    }

    #removeSubscription(subscription: SubscriptionController): void {
        if (this.#subscriptions.get(subscription.streamId) === subscription) {
            this.#subscriptions.delete(subscription.streamId);
        }
        this.#logicalSubscriptions.delete(subscription);
    }

    #takeRequest(streamId: string): PendingRequest | undefined {
        const pending = this.#pendingRequests.get(streamId);
        if (pending === undefined) {
            return undefined;
        }

        this.#pendingRequests.delete(streamId);
        this.#removeAbortListener(pending);
        return pending;
    }

    #removeAbortListener(pending: PendingRequest): void {
        if (pending.signal !== undefined && pending.abortListener !== undefined) {
            pending.signal.removeEventListener("abort", pending.abortListener);
        }
    }

    #transition(
        state: HostLoomConnectionState,
        details: { readonly close?: HostLoomCloseInfo; readonly error?: Error } = {},
    ): void {
        if (state === this.#state) {
            return;
        }

        const change: HostLoomConnectionStateChange = {
            previousState: this.#state,
            state,
            ...details,
        };
        this.#state = state;

        for (const listener of this.#stateListeners) {
            notifyListener(listener, change);
        }
    }
}

const DEFAULT_RECONNECT_INITIAL_DELAY = 1_000;
const DEFAULT_RECONNECT_MAXIMUM_DELAY = 30_000;
const DEFAULT_RECONNECT_MULTIPLIER = 2;
const DEFAULT_RECONNECT_JITTER_RATIO = 0.2;
const UTF8_ENCODER = new TextEncoder();

function resolveReconnectOptions(
    options: HostLoomReconnectOptions | undefined,
): ResolvedReconnectOptions | undefined {
    if (options === undefined) {
        return undefined;
    }

    const initialDelayMilliseconds =
        options.initialDelayMilliseconds ?? DEFAULT_RECONNECT_INITIAL_DELAY;
    const maximumDelayMilliseconds =
        options.maximumDelayMilliseconds ?? DEFAULT_RECONNECT_MAXIMUM_DELAY;
    const multiplier = options.multiplier ?? DEFAULT_RECONNECT_MULTIPLIER;
    const jitterRatio = options.jitterRatio ?? DEFAULT_RECONNECT_JITTER_RATIO;

    if (!Number.isSafeInteger(initialDelayMilliseconds) || initialDelayMilliseconds <= 0) {
        throw new RangeError("The reconnect initial delay must be a positive safe integer.");
    }

    if (
        !Number.isSafeInteger(maximumDelayMilliseconds) ||
        maximumDelayMilliseconds < initialDelayMilliseconds
    ) {
        throw new RangeError(
            "The reconnect maximum delay must be a safe integer no smaller than its initial delay.",
        );
    }

    if (!Number.isFinite(multiplier) || multiplier <= 1) {
        throw new RangeError("The reconnect multiplier must be a finite number greater than one.");
    }

    if (!Number.isFinite(jitterRatio) || jitterRatio < 0 || jitterRatio >= 1) {
        throw new RangeError(
            "The reconnect jitter ratio must be a finite number from zero up to, but not including, one.",
        );
    }

    if (
        options.refreshCredentials !== undefined &&
        typeof options.refreshCredentials !== "function"
    ) {
        throw new TypeError("The credential refresh callback must be a function when provided.");
    }

    return {
        initialDelayMilliseconds,
        maximumDelayMilliseconds,
        multiplier,
        jitterRatio,
        refreshCredentials: options.refreshCredentials,
    };
}

function createBrowserWebSocket(
    url: string | URL,
    protocols: readonly string[],
): HostLoomWebSocket {
    return new WebSocket(url, [...protocols]) as HostLoomWebSocket;
}

function validateClose(code: number, reason: string): void {
    if (code !== 1000 && (code < 3000 || code > 4999 || !Number.isInteger(code))) {
        throw new RangeError(
            "The WebSocket close code must be 1000 or an integer from 3000 to 4999.",
        );
    }

    if (new TextEncoder().encode(reason).byteLength > 123) {
        throw new RangeError("The WebSocket close reason must not exceed 123 UTF-8 bytes.");
    }
}

function asError(error: unknown): Error {
    return error instanceof Error ? error : new Error(String(error));
}

function canceledBy(signal: AbortSignal): HostLoomRequestCanceledError {
    return new HostLoomRequestCanceledError(
        signal.reason instanceof Error ? { cause: signal.reason } : undefined,
    );
}

function subscriptionCanceledBy(signal: AbortSignal): HostLoomSubscriptionCanceledError {
    return new HostLoomSubscriptionCanceledError(
        signal.reason instanceof Error ? { cause: signal.reason } : undefined,
    );
}

function notifyListener<T>(listener: (value: T) => void, value: T): void {
    try {
        listener(value);
    } catch (error) {
        queueMicrotask(() => {
            throw error;
        });
    }
}
