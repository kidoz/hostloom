# HostLoom.NodeLeasing.Testing

This package tests node-ID consumers of `HostLoom.NodeLeasing` without an elector or a lock
backend. It references the kernel only.

`ManualNodeLeasing` is an `INodeLeasing` a test flips by hand: `Assign(id)` gives the instance a
node ID for a new term and releases `WaitForAssignmentAsync` waiters, `Lose()` ends the
assignment as an expired lease would, cancelling `AssignmentToken` and raising `Lost`, and
`Release()` ends it as a release on request. `Changes` lists every change raised, so a
consumer's reaction to each transition is asserted in order. `IsCoordinated` is settable, `true`
by default, to stand in for an elector over a disabled lock.

```csharp
using var leasing = new ManualNodeLeasing("orders-workers");
var source = new ShipmentIdSource(leasing);
await source.StartAsync(token);

leasing.Assign(7);      // the consumer sees node 7 and starts issuing identifiers
leasing.Lose();         // its token cancels; issuing must stop
```

See the `HostLoom.NodeLeasing` package README for the guarantee statement the scripted
transitions mirror.
