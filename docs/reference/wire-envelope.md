# Wire envelope

Every message HostLoom puts on a transport travels inside an explicit
envelope. The envelope type itself is internal — this page documents the
format, which is the actual compatibility contract between services.

## Fields

| Field | Type | Meaning |
| --- | --- | --- |
| `MessageId` | GUID | Unique per message |
| `CorrelationId` | GUID, optional | Ties a response or fault back to its request |
| `Kind` | `Request` \| `Response` \| `Fault` \| `Event` | What this envelope carries |
| `MessageType` | string | Logical type name of the body |
| `ResponseType` | string | Logical type name of the expected response |
| `SentAt` | timestamp | When the sender serialized the envelope |
| `Body` | bytes | The serialized message payload |
| `Fault` | `{ ErrorType, Message }` | Present on `Kind = Fault` |

## Logical type names

A logical type name has the form:

```text
{AssemblyName}:{Type.FullName}
```

produced by `MessageTypeName.For<T>()`. Handler registration and response
validation compare these exact strings, so **both sides must produce the
same logical name** — same assembly name *and* same full type name. Two
services that separately define an identical-looking record in differently
named assemblies will not interoperate. The normal way to satisfy this is
a shared contracts assembly referenced by both sides.

The name is an identifier, not a load instruction: HostLoom never
dynamically loads the sender's assembly. The receiver deserializes the
body into the type *it* registered under that logical name.

## Encoding

The envelope is encoded with `System.Text.Json` using web defaults
(camel-case names) and the `Kind` enum as a string. The **body** passes
through the configurable serialization boundary: `IMessageSerializer`,
whose default is `SystemTextJsonMessageSerializer`, replaceable through
dependency injection because it is registered with `TryAddSingleton`.

## Identifier and name validation

Decoding rejects an envelope whose `MessageId` is missing or the all-zero
GUID, one whose `CorrelationId` is present but all-zero, and a `Response`
or `Fault` without a `CorrelationId`. Each is a `MalformedEnvelopeException`
raised before any handler runs: the message id is the sender's, and every
inbox key and reply correlation is built on it, so an empty one would let
unrelated deliveries collide.

Decoding also rejects, with the same exception, an envelope of any kind whose
`MessageType` is `null` or empty, a `Request` whose `ResponseType` is `null`
or empty, and a `Fault` whose `Fault` object has a `null` or empty
`ErrorType` or a `null` `Message`. The names are what the receiver resolves
registrations by, and the web defaults do not enforce nullable annotations, so
an explicit `null` would otherwise reach that lookup. An `Event` carries an
empty `ResponseType` by design, and neither a `Response`'s nor an `Event`'s
`ResponseType` is read. A `Fault` with no `Fault` object, or with an empty
`Message`, is still accepted; the caller sees the former as `ErrorType`
`Unknown`. Transports treat the exception as a poison frame rather than a
handler failure: Kafka commits past the record, and RabbitMQ rejects it
without requeue.

## Faults

A remote fault carries a **type and a message only — no stack trace**
crosses the wire. By default the type is `HandlerFault` and the message a
fixed "The request handler failed."; the real exception stays in the
handling side's log. The exception's own type name and message are sent
only when it is a `RemoteFaultException` (thrown by the handler for the
caller to read) or when `HostLoomOptions.IncludeFaultDetails` is on.
Requests the endpoint cannot route are answered with the stable types
`HandlerNotFound` and `ResponseTypeMismatch`, and a request a receive filter
completed without running its handler with `HandlerNotRun`, because there is
no response to send. On the caller's side a fault
surfaces as `RemoteRequestException`, whose `ErrorType` is that type. An
envelope that cannot be decoded raises `MalformedEnvelopeException`; a
reply that never arrives within the request timeout raises
`RequestTimeoutException`.

## What rides where

The envelope is transport-neutral; correlation additionally uses each
broker's native machinery:

- **RabbitMQ** — AMQP `CorrelationId` and `ReplyTo` properties.
- **Kafka** — correlation in record headers; replies on the configured
  response topic.
- **In-memory** — direct dispatch, same envelope, no wire.
