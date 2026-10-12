; Shipped analyzer releases. Rules first shipped with each tagged release, oldest first.
; Composition rules HLM0009-HLM0016 live in HostLoom.Composition.Generators and have not shipped yet.

## Release 0.2.0

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|------
HLM0001 | Usage | Warning | Pass an available cancellation token to HostLoom async calls.
HLM0002 | Usage | Warning | Avoid blocking on HostLoom async operations.
HLM0003 | Usage | Warning | Register HostLoom handlers and behaviors as scoped services.

## Release 0.3.0

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|------
HLM0004 | Usage | Warning | Assign every settable member of a mapped destination.
HLM0005 | Usage | Warning | Keep a map body in a shape completeness can be verified in.
HLM0006 | Usage | Warning | Do not capture the scoped mapping dispatcher in a singleton.

## Release 0.4.0

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|------
HLM0007 | Usage | Warning | Hash a secret before it becomes part of a cache or lock key.
HLM0008 | Usage | Warning | Forward the cancellation token a get-or-create factory receives.

## Release 0.8.0

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|------
HLM0017 | Usage | Info | Completeness of a generic map is not checked; test its closed pairs.
