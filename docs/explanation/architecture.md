# Architecture and package layering

HostLoom is an experiment in building a Spring-like application framework
for .NET as a set of small, honest layers rather than one large runtime.
This page explains the layering and the reasoning behind it.

## Two borrowed ideas

HostLoom is deliberately not a MassTransit reimplementation. It borrows two
durable ideas and stops there:

1. **GreenPipes' composable asynchronous pipeline** becomes
   `HostLoom.Pipelines` — a transport-neutral middleware foundation with no
   dependency on messaging or a broker.
2. **MassTransit's typed contracts** — handlers, behaviors, clients,
   correlation, faults, and hosted transport lifecycle — become a compact
   request runtime in the `HostLoom` package.

## The layers

```text
HostLoom.Pipelines            ← no dependencies; pure middleware
        ▲
HostLoom                      ← contracts + runtime, builds on pipelines
        ▲
HostLoom.Transport.*          ← one adapter per broker
```

Alongside, three families stand **independent of the messaging core** and
are usable on their own: `HostLoom.Mapping.*`, `HostLoom.Logging`, and
`HostLoom.Diagnostics`. Independence is a design rule, not an accident — a
library should not drag a message broker into an application that only
wanted explicit object mapping.

## Contracts before machinery

The public surface is contracts-first: `IRequest<TResponse>`,
`IRequestHandler<,>`, `IRequestBehavior<,>`, `IRequestClient<,>`, `IEvent`,
`IEventHandler<>`, `IPublishEndpoint`. The machinery that moves envelopes —
dispatchers, executors, endpoints, the receive pipeline — is `internal`.
That keeps the compatibility promise small: applications depend on
contracts, and the runtime is free to change shape underneath them.

The transport SPI is equally small: a transport is an `IRequestBroker`
(listen and request over byte frames), optionally an `IEventBroker`
(publish/subscribe), and optionally an `IBrokerHealthProbe`. Everything a
transport does not implement is an explicit, early failure rather than a
silent degradation — publishing through a transport without pub/sub
throws, and registering a subscription against one fails at startup.

## Scoped by delivery attempt

One dependency-injection scope per delivery attempt is a foundational
choice, not a detail. It means a handler takes scoped dependencies through
its constructor like an ASP.NET Core controller, a retry never observes
scoped state leaked by the failed attempt before it, and the `HLM0003`
analyzer can flag singleton handler registrations as the bug they are.

## Explicit over convention

A recurring stance across the packages: make the decision visible in code
rather than in a convention engine.

- Object maps are ordinary classes; the compiler and code review see every
  member assignment (`HostLoom.Mapping`).
- Null tolerance is in the method name (`MapMany` vs `MapManyOrEmpty`),
  not in configuration.
- Conditional registration is recorded in a ledger and reported at startup
  (`HostLoom.Diagnostics`).
- The wire envelope's fields are explicit and documented, not an
  implementation detail of a serializer.

**Why explicit mapping.** A convention mapper decides member matching,
null handling, and conversions once, centrally, and invisibly — the cost
appears later, as silent data loss when a renamed member stops matching.
Explicit maps trade that convenience for compiler visibility, and the
analyzers close the remaining gap (`HLM0004`/`HLM0005` catch a
destination member no map assigns). Mapping is also deliberately
synchronous and I/O-free: fetching data belongs outside the map, distinct
semantic views deserve distinct destination types, and database
projections are better written directly as `IQueryable.Select`
expressions. The trade-off is real — more files, some repetition — and
accepted on the view that a mapping bug you can see costs less than one
you cannot.

**Why a composition ledger.** Registration builds a plan and executes
nothing, so the moment a conditional branch is taken there is no logger,
no bound options, no sinks — which is why composition logging so often
ends up as a static `Log.Debug` that fires before the real logging
exists, at a level filtered out in production. The ledger inverts this:
record at registration time, report once at host startup through the
application's own logging stack. Recording *skipped* components is the
part that pays: a log of what was registered cannot answer "what is
missing", because the branch that did nothing wrote nothing. The cost is
discipline — nothing enforces the calls, and a stale entry misreports —
which is why the guidance is to record selectively and keep each entry
beside the registration it describes.

## Where it is heading

The roadmap runs toward the Spring analogy: hardened delivery policies,
outbox/inbox, starter packages with conditional auto-configuration over
`Microsoft.Extensions.*`, validation and observability starters, and
AOT-safe compile-time registration. The current slice is intentionally a
small foundation laid carefully.
