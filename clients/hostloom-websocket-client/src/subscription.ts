import {
    HostLoomProtocolError,
    type ClientFrame,
    type EventFrame,
    type SubscribedFrame,
} from "./protocol.js";

export interface HostLoomSubscribeOptions {
    readonly credit: number;
    readonly key?: string;
    readonly lowWatermark?: number;
    readonly signal?: AbortSignal;
}

export type HostLoomSubscriptionState =
    "subscribing" | "active" | "reconnecting" | "unsubscribing" | "closed";

export interface HostLoomSubscriptionClose {
    readonly error?: Error;
}

export type HostLoomSubscriptionEventListener = (event: EventFrame) => void;
export type HostLoomSubscriptionCloseListener = (close: HostLoomSubscriptionClose) => void;

export interface HostLoomSubscription {
    readonly streamId: string;
    readonly topic: string;
    readonly key: string | undefined;
    readonly state: HostLoomSubscriptionState;

    onEvent(listener: HostLoomSubscriptionEventListener): () => void;
    onClose(listener: HostLoomSubscriptionCloseListener): () => void;
    acknowledge(sequence: number): void;
    unsubscribe(): Promise<void>;
}

export class HostLoomSubscriptionCanceledError extends Error {
    public constructor(options?: ErrorOptions) {
        super("The subscription was canceled by the caller.", options);
        this.name = "HostLoomSubscriptionCanceledError";
    }
}

export class HostLoomSubscriptionStateError extends Error {
    public readonly state: HostLoomSubscriptionState;

    public constructor(state: HostLoomSubscriptionState, message: string) {
        super(message);
        this.name = "HostLoomSubscriptionStateError";
        this.state = state;
    }
}

interface SubscriptionControllerOptions {
    readonly streamId: string;
    readonly topic: string;
    readonly key: string | undefined;
    readonly credit: number;
    readonly lowWatermark: number;
    readonly signal: AbortSignal | undefined;
    readonly send: (frame: ClientFrame) => void;
    readonly onProtocolError: (error: HostLoomProtocolError) => void;
    readonly onTerminal: () => void;
}

type InternalSubscriptionState = "subscribing" | "resubscribing" | HostLoomSubscriptionState;

/** @internal */
export class SubscriptionController {
    #streamId: string;
    readonly #topic: string;
    readonly #key: string | undefined;
    readonly #credit: number;
    readonly #lowWatermark: number;
    readonly #signal: AbortSignal | undefined;
    readonly #send: (frame: ClientFrame) => void;
    readonly #onProtocolError: (error: HostLoomProtocolError) => void;
    readonly #onTerminal: () => void;
    readonly #eventListeners = new Set<HostLoomSubscriptionEventListener>();
    readonly #closeListeners = new Set<HostLoomSubscriptionCloseListener>();
    readonly #bufferedEvents: EventFrame[] = [];
    readonly #handle: HostLoomSubscription;
    readonly #readyPromise: Promise<HostLoomSubscription>;
    readonly #abortListener: () => void;

    #state: InternalSubscriptionState = "subscribing";
    #remainingCredit: number;
    #readySettled = false;
    #resolveReady: (subscription: HostLoomSubscription) => void;
    #rejectReady: (error: Error) => void;
    #terminalError: Error | undefined;
    #unsubscribePromise: Promise<void> | undefined;
    #resolveUnsubscribe: (() => void) | undefined;
    #rejectUnsubscribe: ((error: Error) => void) | undefined;

    public constructor(options: SubscriptionControllerOptions) {
        this.#streamId = options.streamId;
        this.#topic = options.topic;
        this.#key = options.key;
        this.#credit = options.credit;
        this.#remainingCredit = options.credit;
        this.#lowWatermark = options.lowWatermark;
        this.#signal = options.signal;
        this.#send = options.send;
        this.#onProtocolError = options.onProtocolError;
        this.#onTerminal = options.onTerminal;

        let resolveReady: (subscription: HostLoomSubscription) => void;
        let rejectReady: (error: Error) => void;
        this.#readyPromise = new Promise<HostLoomSubscription>((resolve, reject) => {
            resolveReady = resolve;
            rejectReady = reject;
        });
        this.#resolveReady = resolveReady!;
        this.#rejectReady = rejectReady!;
        this.#abortListener = () => this.#cancelBySignal();

        const controller = this;
        this.#handle = {
            get streamId() {
                return controller.#streamId;
            },
            get topic() {
                return controller.#topic;
            },
            get key() {
                return controller.#key;
            },
            get state() {
                return controller.#publicState;
            },
            onEvent(listener) {
                return controller.#onEvent(listener);
            },
            onClose(listener) {
                return controller.#onClose(listener);
            },
            acknowledge(sequence) {
                controller.#acknowledge(sequence);
            },
            unsubscribe() {
                return controller.#unsubscribe();
            },
        };
    }

    public get ready(): Promise<HostLoomSubscription> {
        return this.#readyPromise;
    }

    public get streamId(): string {
        return this.#streamId;
    }

    public get credit(): number {
        return this.#credit;
    }

    public get canRestart(): boolean {
        return this.#state === "reconnecting";
    }

    public start(): void {
        this.#signal?.addEventListener("abort", this.#abortListener, { once: true });
        this.#sendSubscribe();
    }

    public suspend(error: Error): void {
        if (this.#state === "closed" || this.#state === "reconnecting") {
            return;
        }

        if (this.#state === "unsubscribing") {
            this.#finish(error);
            return;
        }

        this.#state = "reconnecting";
        this.#remainingCredit = this.#credit;
        this.#bufferedEvents.length = 0;
    }

    public restart(streamId: string): boolean {
        if (this.#state !== "reconnecting") {
            return false;
        }

        this.#streamId = streamId;
        this.#remainingCredit = this.#credit;
        this.#state = this.#readySettled ? "resubscribing" : "subscribing";
        return this.#sendSubscribe();
    }

    #sendSubscribe(): boolean {
        try {
            this.#send({
                kind: "subscribe",
                streamId: this.#streamId,
                topic: this.#topic,
                credit: this.#credit,
                ...(this.#key === undefined ? {} : { key: this.#key }),
            });
            return true;
        } catch (error) {
            this.fail(asError(error));
            return false;
        }
    }

    public acceptSubscribed(frame: SubscribedFrame): void {
        if (this.#state === "closed") {
            return;
        }

        if (
            frame.topic !== this.#topic ||
            frame.key !== this.#key ||
            frame.credit !== this.#credit
        ) {
            this.#onProtocolError(
                new HostLoomProtocolError(
                    "The subscribed frame does not match the requested subscription.",
                ),
            );
            return;
        }

        if (this.#state === "subscribing" || this.#state === "resubscribing") {
            this.#state = "active";
            if (!this.#readySettled) {
                this.#readySettled = true;
                this.#resolveReady(this.#handle);
            }
            return;
        }

        if (this.#state === "active") {
            this.#onProtocolError(
                new HostLoomProtocolError(
                    "The server confirmed the same subscription more than once.",
                ),
            );
        }
    }

    public acceptEvent(event: EventFrame): void {
        if (this.#state === "closed") {
            return;
        }

        if (this.#state === "subscribing" || this.#state === "resubscribing") {
            this.#onProtocolError(
                new HostLoomProtocolError(
                    "A subscription event arrived before the subscribed frame.",
                ),
            );
            return;
        }

        if (this.#remainingCredit <= 0) {
            this.#onProtocolError(
                new HostLoomProtocolError(
                    "The server sent a subscription event without available credit.",
                ),
            );
            return;
        }

        this.#remainingCredit--;
        if (this.#state !== "active") {
            return;
        }

        if (this.#eventListeners.size === 0) {
            this.#bufferedEvents.push(event);
            return;
        }

        this.#notifyEvent(event);
        this.#replenishCredit();
    }

    public complete(): void {
        if (this.#state === "closed") {
            return;
        }

        if (this.#state === "subscribing" || this.#state === "resubscribing") {
            this.#onProtocolError(
                new HostLoomProtocolError("The subscription completed before it was confirmed."),
            );
            return;
        }

        this.#finish();
    }

    public fail(error: Error): void {
        if (this.#state === "closed") {
            return;
        }

        this.#finish(error);
    }

    #onEvent(listener: HostLoomSubscriptionEventListener): () => void {
        if (this.#state === "closed") {
            return () => false;
        }

        this.#eventListeners.add(listener);
        if (this.#state === "active" && this.#bufferedEvents.length > 0) {
            const buffered = this.#bufferedEvents.splice(0);
            for (const event of buffered) {
                if (this.#state !== "active") {
                    break;
                }
                this.#notifyEvent(event);
            }
            this.#replenishCredit();
        }

        return () => this.#eventListeners.delete(listener);
    }

    #onClose(listener: HostLoomSubscriptionCloseListener): () => void {
        if (this.#state === "closed") {
            notifyListener(listener, closeWith(this.#terminalError));
            return () => false;
        }

        this.#closeListeners.add(listener);
        return () => this.#closeListeners.delete(listener);
    }

    #acknowledge(sequence: number): void {
        if (!Number.isSafeInteger(sequence) || sequence <= 0) {
            throw new RangeError("The acknowledged sequence must be a positive safe integer.");
        }

        if (this.#state === "reconnecting" || this.#state === "resubscribing") {
            return;
        }

        if (this.#state !== "active") {
            throw new HostLoomSubscriptionStateError(
                this.#publicState,
                "A sequence can be acknowledged only while the subscription is active.",
            );
        }

        this.#send({ kind: "ack", streamId: this.#streamId, sequence });
    }

    #unsubscribe(): Promise<void> {
        if (this.#state === "closed") {
            return Promise.resolve();
        }

        const unsubscribePromise = this.#getUnsubscribePromise();
        this.#beginUnsubscribe();
        return unsubscribePromise;
    }

    #getUnsubscribePromise(): Promise<void> {
        if (this.#unsubscribePromise !== undefined) {
            return this.#unsubscribePromise;
        }

        this.#unsubscribePromise = new Promise<void>((resolve, reject) => {
            this.#resolveUnsubscribe = resolve;
            this.#rejectUnsubscribe = reject;
        });
        return this.#unsubscribePromise;
    }

    #cancelBySignal(): void {
        if (this.#state === "closed" || this.#state === "unsubscribing") {
            return;
        }

        if (!this.#readySettled) {
            this.#readySettled = true;
            this.#rejectReady(canceledBy(this.#signal as AbortSignal));
        }

        if (this.#state === "reconnecting") {
            this.#finish();
            return;
        }

        this.#beginUnsubscribe();
    }

    #beginUnsubscribe(): void {
        if (this.#state === "closed" || this.#state === "unsubscribing") {
            return;
        }

        if (this.#state === "reconnecting") {
            this.#finish();
            return;
        }

        this.#state = "unsubscribing";
        this.#bufferedEvents.length = 0;
        this.#removeAbortListener();
        try {
            this.#send({ kind: "unsubscribe", streamId: this.#streamId });
        } catch (error) {
            this.fail(asError(error));
        }
    }

    #replenishCredit(): void {
        if (
            this.#state !== "active" ||
            this.#eventListeners.size === 0 ||
            this.#remainingCredit > this.#lowWatermark
        ) {
            return;
        }

        const amount = this.#credit - this.#remainingCredit;
        const previousCredit = this.#remainingCredit;
        this.#remainingCredit = this.#credit;
        try {
            this.#send({ kind: "credit", streamId: this.#streamId, credit: amount });
        } catch (error) {
            this.#remainingCredit = previousCredit;
            this.fail(asError(error));
        }
    }

    #notifyEvent(event: EventFrame): void {
        for (const listener of this.#eventListeners) {
            notifyListener(listener, event);
        }
    }

    #finish(error?: Error): void {
        if (this.#state === "closed") {
            return;
        }

        this.#state = "closed";
        this.#terminalError = error;
        this.#bufferedEvents.length = 0;
        this.#removeAbortListener();
        this.#onTerminal();

        if (!this.#readySettled) {
            this.#readySettled = true;
            if (error === undefined) {
                this.#rejectReady(
                    new HostLoomSubscriptionStateError(
                        "closed",
                        "The subscription ended before it was confirmed.",
                    ),
                );
            } else {
                this.#rejectReady(error);
            }
        }

        if (error === undefined) {
            this.#resolveUnsubscribe?.();
        } else {
            this.#rejectUnsubscribe?.(error);
        }

        const close = closeWith(error);
        for (const listener of this.#closeListeners) {
            notifyListener(listener, close);
        }
        this.#eventListeners.clear();
        this.#closeListeners.clear();
    }

    #removeAbortListener(): void {
        this.#signal?.removeEventListener("abort", this.#abortListener);
    }

    get #publicState(): HostLoomSubscriptionState {
        return this.#state === "resubscribing" ? "reconnecting" : this.#state;
    }
}

function canceledBy(signal: AbortSignal): HostLoomSubscriptionCanceledError {
    return new HostLoomSubscriptionCanceledError(
        signal.reason instanceof Error ? { cause: signal.reason } : undefined,
    );
}

function closeWith(error: Error | undefined): HostLoomSubscriptionClose {
    return error === undefined ? {} : { error };
}

function asError(error: unknown): Error {
    return error instanceof Error ? error : new Error(String(error));
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
