# HostLoom.Generators

Secure random strings, fixed-width numeric codes, human-readable coupon codes, opaque tokens,
and constrained identifier generators for HostLoom. The package is dependency-free (pure BCL)
and trimming/Native AOT compatible.

## Guarantees and limits

- Every generator is backed by a cryptographic random number generator with unbiased selection.
- **No uniqueness guarantees anywhere.** Two calls can return the same value. Durable
  uniqueness stays an atomic constraint in the owning service; use `DistinctBatch` for
  batch-local de-duplication only.
- **Generated secrets must never be logged.** Tokens, nonces, and codes are written to
  diagnostics only as lengths and counts.
- Output formatting uses the invariant culture only.
- This is **not a drop-in Powell.CouponCode replacement**: the pinned 1.0.3 artifact's alphabet,
  checksums, and grouping behavior are unverified, and old codes are never regenerated.

## The entropy seam

Every generator accepts an optional `IRandomSource` and defaults to
`SecureRandomSource.Instance`. `IRandomSource` is the explicit, documented test seam — substitute
a deterministic fake (shipped in the HostLoom.Generators.Testing package) to force characters,
bounds, and collisions in tests. Production code must not silently switch to seeded
pseudorandomness.

## Random strings

```csharp
var profile = new RandomStringProfile(RandomAlphabet.AlphaNumeric, length: 10);
var generator = new RandomStringGenerator();
string reference = generator.Generate(profile);          // exactly 10 characters
generator.TryGenerate(profile, buffer);                  // false if the span is too short
```

Presets (`Digits`, `HexLower`, `HexUpper`, `AlphaNumeric`, `UrlSafe`, `HumanReadable`) are
versioned compatibility contracts; their exact characters are pinned by tests.

## Numeric codes (confirmation code)

```csharp
var profile = new NumericCodeProfile(digits: 5);
var generator = new NumericCodeGenerator();
string code = generator.Generate(profile);               // "00000"–"99999", leading zeros kept

// Legacy adapter excluding the final value: 00000–99998
var legacy = new NumericCodeProfile(5, maxExclusive: 99999);
```

OTP expiry, attempt limits, and delivery remain caller responsibilities.

## Numeric IDs (invoice identifier)

```csharp
// A ten-digit invoice ID that never starts with zero.
var profile = new NumericIdProfile(1_000_000_000L, 10_000_000_000L);
string invoiceId = new NumericCodeGenerator().Generate(profile);
```

## Coupon codes (order coupon code)

```csharp
var profile = new CouponCodeProfile(partLength: 6, partCount: 1, includeCheckDigit: true);
var generator = new CouponCodeGenerator();
string coupon = generator.Generate(profile);             // e.g. "K7MPQX"

if (generator.TryValidate(typed, profile, out string? normalized))
{
    // normalized is Trim + ToUpperInvariant of the accepted code
}
```

Check digits are position-aware and detect transcription typos before submission; they are not
authentication and add no entropy. `Normalize` is normalization only, never validation.

## Opaque tokens

```csharp
var generator = new OpaqueTokenGenerator();
string token = generator.GenerateToken(32);              // 43 unpadded Base64Url characters
string nonce = generator.GenerateHexNonce(8);            // 16 lowercase hex characters
```

Every call draws independent random bytes. Tokens are opaque secrets or identifiers — never log
them, and never use them where an ordered identifier is expected.

## Prefixed identifiers (order ID)

```csharp
var generator = new PrefixedIdentifierGenerator(totalLength: 20);
int suffixLength = generator.RemainingRandomLength("EU"); // 18
string orderId = generator.Generate("EU");               // exactly 20 characters
```

Prefixes are rejected, never sanitized, when they contain anything but ASCII letters and digits
or would leave fewer random characters than the configured minimum.

## Distinct batches

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

Duplicate detection happens after canonicalization; an exhausted budget returns the distinct
values gathered so far with `Status == Exhausted`, never duplicates and never a silent partial
success. There is no storage coordination — the owning service keeps its durable unique rule.

## Random choice

```csharp
string region = SecureChoice.Choose(new[] { "eu", "us" });
```

## GUID identifiers

```csharp
IStringIdGenerator ids = GuidStringIdGenerator.Dashed;   // 36 characters, "D" format
IStringIdGenerator compact = GuidStringIdGenerator.Compact; // 32 characters, "N" format
string id = ids.CreateStringId();
```

These are compatibility identifiers over BCL GUID generation; they are never secrets.
