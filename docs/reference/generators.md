# Generators

The `HostLoom.Generators` package: secure random strings, fixed-width
numeric codes, human-readable coupon codes, opaque tokens, prefixed
identifiers, GUID string identifiers, distinct batches, and unbiased random
choice. The package is dependency-free (pure BCL) and trimming/Native AOT
compatible; every type lives in the `HostLoom.Generators` namespace. The
companion `HostLoom.Generators.Testing` package ships the deterministic
entropy fakes described under [the entropy seam](#the-entropy-seam).

```text
dotnet add package HostLoom.Generators
dotnet add package HostLoom.Generators.Testing   # deterministic test fakes
```

## Guarantees and limits

- Every generator is backed by a cryptographic random number generator with
  unbiased selection.
- **No uniqueness guarantees anywhere.** Two calls can return the same
  value. Durable uniqueness stays an atomic constraint in the owning
  service; use `DistinctBatch` for batch-local de-duplication only.
- **Generated secrets must never be logged.** Tokens, nonces, and codes are
  written to diagnostics only as lengths and counts, and validation
  exceptions identify the offending position without echoing the input.
- Output formatting uses the invariant culture only; the current culture
  never affects output.
- Profiles and preset alphabets are immutable and thread-safe, and can be
  shared across generators and threads. The preset alphabets are versioned
  compatibility contracts: their exact characters and order are pinned by
  tests and never change within a major version.
- This is **not a drop-in Powell.CouponCode replacement**: the pinned
  1.0.3 artifact's alphabet, checksums, and grouping behavior are
  unverified, and codes issued by an older generator or library are never
  regenerated or rewritten.

## The entropy seam

Every generator accepts an optional `IRandomSource` and defaults to
`SecureRandomSource.Instance`, a process-wide, thread-safe source backed by
`RandomNumberGenerator` with unbiased rejection sampling on both integer
widths. `NextInt64` returns `minInclusive` when the bounds are equal and
draws nothing, matching `Random.NextInt64`; `NextInt32` rejects an empty
range.

```csharp
public interface IRandomSource
{
    void Fill(Span<byte> destination);
    int NextInt32(int minInclusive, int maxExclusive);
    long NextInt64(long minInclusive, long maxExclusive);
}
```

`IRandomSource` is the explicit, documented test seam. The
`HostLoom.Generators.Testing` package ships the deterministic fakes —
`SeededRandomSource` for reproducible seeded values, `ScriptedRandomSource`
for replaying a fixed script of values, and `AsyncNumericSequence` for a
deterministic numeric sequence — so a test can force characters, bounds,
and collisions. Production code must not silently switch to seeded
pseudorandomness; pass a fake only from test code.

## Random strings

`RandomStringGenerator` produces exact-length strings under an immutable
`RandomStringProfile`, each character selected independently over the whole
alphabet with equal weight:

```csharp
var profile = new RandomStringProfile(RandomAlphabet.AlphaNumeric, length: 10);
var generator = new RandomStringGenerator();
string reference = generator.Generate(profile);          // exactly 10 characters
generator.TryGenerate(profile, buffer);                  // false if the span is too short
```

`RandomStringProfile` accepts lengths in [1, 4096]. The presets — `Digits`,
`HexLower`, `HexUpper`, `AlphaNumeric`, `UrlSafe`, and `HumanReadable`
(thirty-two characters excluding the visually ambiguous `0`/`O` and
`1`/`I`) — are the versioned contracts named above; a custom
`RandomAlphabet` rejects duplicate and non-ASCII characters at construction
so every character keeps equal selection weight.

## Numeric codes (confirmation codes)

`NumericCodeGenerator` with a `NumericCodeProfile` draws a value from
[0, `MaxExclusive`) and formats it zero-padded to exactly `Digits`
characters (1–18) with the invariant culture. Leading zeros are part of the
contract, so `00042` is a valid five-digit code:

```csharp
var profile = new NumericCodeProfile(digits: 5);
var generator = new NumericCodeGenerator();
string code = generator.Generate(profile);               // "00000"–"99999", leading zeros kept

// Legacy adapter excluding the final value: 00000–99998
var legacy = new NumericCodeProfile(5, maxExclusive: 99999);
```

`MaxExclusive` defaults to 10^digits, covering every `digits`-digit value
including the final one; supply a smaller bound to reproduce a legacy range
that excludes the final value, as the adapter above does. For one-time
passwords, expiry, attempt limits, and delivery remain caller
responsibilities.

## Numeric identifiers (invoice identifier)

With a `NumericIdProfile`, values are drawn from
[`MinInclusive`, `MaxExclusive`) and formatted without padding; pick
`MinInclusive` = 10^(d−1) so identifiers never start with a zero:

```csharp
// A ten-digit invoice ID that never starts with zero.
var profile = new NumericIdProfile(1_000_000_000L, 10_000_000_000L);
string invoiceId = new NumericCodeGenerator().Generate(profile);
```

## Coupon codes (order coupon codes)

`CouponCodeGenerator` generates and validates human-readable codes of one
to six parts of 2–16 characters each, joined by a separator (`-` by
default) and drawn from `RandomAlphabet.HumanReadable` unless another
alphabet is supplied:

```csharp
var profile = new CouponCodeProfile(partLength: 6, partCount: 1, includeCheckDigit: true);
var generator = new CouponCodeGenerator();
string coupon = generator.Generate(profile);             // e.g. "K7MPQX"

if (generator.TryValidate(typed, profile, out string? normalized))
{
    // normalized is Trim + ToUpperInvariant of the accepted code
}
```

An eight-character single-part code is the same profile with
`partLength: 8`. When `includeCheckDigit` is true the final character of
every part is a check digit computed from the rest of the part, so a part
contributes `partLength − 1` random characters: a six-character part with a
check digit carries five random characters, an eight-character part seven.
Collision budgets must use that random search space, not the formatted
length.

Check digits are position-aware — the same characters moved to a different
part position fail validation — and detect transcription typos before a
code is submitted. They are not authentication and add no entropy.
`TryValidate` normalizes the input, splits on the separator, and verifies
the part count, the exact length of every part, alphabet membership of
every character, and every check digit when the profile enables them; on
success `normalized` receives the normalized code. `Normalize` is
normalization only — Trim plus `ToUpperInvariant` — and never validation:
it makes a typed-in code comparable but accepts any characters.

The separator must be a printable ASCII character that is not a letter or
digit and does not occur in the alphabet; an unacceptable separator is
rejected at profile construction.

## Opaque tokens and nonces

`OpaqueTokenGenerator` fills a fresh buffer on every call, so two calls
never share state:

```csharp
var generator = new OpaqueTokenGenerator();
string token = generator.GenerateToken(32);              // 43 unpadded Base64Url characters
string nonce = generator.GenerateHexNonce(8);            // 16 lowercase hex characters
```

Both members accept byte lengths in [1, 1024]; `GenerateHexNonce` takes an
optional `uppercase: true`. Tokens are opaque secrets or identifiers —
never log them, and never use them where an ordered identifier is expected:
tokens are not ordered.

## Prefixed identifiers (order IDs)

`PrefixedIdentifierGenerator` combines a caller-supplied prefix with a
random suffix for provider or wire formats that constrain total length.
Output length is always exactly the configured total length:

```csharp
var generator = new PrefixedIdentifierGenerator(totalLength: 20);
int suffixLength = generator.RemainingRandomLength("EU"); // 18
string orderId = generator.Generate("EU");               // exactly 20 characters
```

The suffix is drawn from `RandomAlphabet.AlphaNumeric` unless another
alphabet is supplied, and at least `minRandomSuffixLength` characters
(four by default) remain random. Prefixes are rejected, never sanitized:
a prefix that is null or empty, contains a character that is not an ASCII
letter or digit, or would leave fewer random characters than the configured
minimum fails the call rather than being silently rewritten. Total length
is bounded to [minRandomSuffixLength + 1, 512].

## GUID string identifiers

`GuidStringIdGenerator` is an `IStringIdGenerator` over BCL `Guid`
generation only — no bespoke GUID algorithm — for formats that already
store GUID strings:

```csharp
IStringIdGenerator ids = GuidStringIdGenerator.Dashed;      // 36 characters, "D" format
IStringIdGenerator compact = GuidStringIdGenerator.Compact; // 32 characters, "N" format
string id = ids.CreateStringId();
```

These are compatibility identifiers; they are never secrets.

## Distinct batches

`DistinctBatch.Generate` draws candidates until `count` distinct values are
collected or the attempt budget runs out:

```csharp
var result = DistinctBatch.Generate(
    candidate: _ => new CouponCodeGenerator().Generate(profile),
    count: 100,
    canonicalize: CouponCodeGenerator.Normalize);

if (result.Status == DistinctBatchStatus.Success)
{
    // result.Values holds exactly 100 distinct codes
}
```

Duplicate detection happens after canonicalization and uses one in-memory
`HashSet<T>` entry per accepted value, so memory grows with the batch size.
`count` is bounded to [1, 100000]; `maxAttempts` defaults to `count` × 10
computed with checked arithmetic and must be at least `count` when
supplied. An optional `exclude` predicate skips values before deduplication,
and the caller's cancellation token is passed to every candidate draw.

Always inspect `Status` before using `Values`: an exhausted budget returns
`DistinctBatchStatus.Exhausted` with the distinct values gathered so far in
`Values` and the draws made in `Attempts` — never duplicates and never a
silent partial success. There is no storage coordination: durable
uniqueness stays an atomic constraint in the owning service, which must
independently retry persistence conflicts.

## Random choice

`SecureChoice.Choose` selects one element of an indexed collection through
the same secure, unbiased `IRandomSource`, so every element has equal
weight; an empty collection is rejected:

```csharp
string region = SecureChoice.Choose(new[] { "eu", "us" });
```

## Native AOT

The package enables the .NET SDK Native AOT and trimming analyzers
(`IsAotCompatible=true`) and is verified under Native AOT rather than only
analyzed: `examples/HostLoom.Examples.GeneratorsAot` publishes with
`PublishAot=true`, resolves every generator from a service collection, and
checks its own results — a five-digit confirmation code, a coupon round
trip through `TryValidate` and `Normalize`, a ten-digit numeric ID, token
and nonce shapes, a prefixed order ID, a compact GUID ID, a distinct batch
of invoice codes, and a secure choice — exiting non-zero on a mismatch.
