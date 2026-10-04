using System.Buffers.Text;
using System.Globalization;
using HostLoom.Generators;
using Microsoft.Extensions.DependencyInjection;

// A Native AOT publish of this program must produce no trim or AOT warnings. The program checks
// its own results and exits with 1 on a mismatch, so running the native binary is the test: every
// generator resolves from the service collection and produces values that satisfy its contract
// without reflection-based dispatch.
var services = new ServiceCollection();
services.AddSingleton<RandomStringGenerator>();
services.AddSingleton<NumericCodeGenerator>();
services.AddSingleton<CouponCodeGenerator>();
services.AddSingleton<OpaqueTokenGenerator>();
services.AddSingleton(new PrefixedIdentifierGenerator(totalLength: 20));
services.AddSingleton<IStringIdGenerator>(GuidStringIdGenerator.Compact);
var container = services.BuildServiceProvider(
    new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }
);
await using var containerLifetime = container.ConfigureAwait(false);

var problems = new List<string>();

var randomStrings = container.GetRequiredService<RandomStringGenerator>();
var numericCodes = container.GetRequiredService<NumericCodeGenerator>();
var coupons = container.GetRequiredService<CouponCodeGenerator>();
var tokens = container.GetRequiredService<OpaqueTokenGenerator>();
var prefixed = container.GetRequiredService<PrefixedIdentifierGenerator>();
var stringIds = container.GetRequiredService<IStringIdGenerator>();

var confirmationCode = numericCodes.Generate(new NumericCodeProfile(digits: 5));
Console.WriteLine($"confirmation code length: {confirmationCode.Length}");
Expect(
    confirmationCode.Length == 5 && confirmationCode.All(static c => c is >= '0' and <= '9'),
    $"a five-digit confirmation code is five digits, got length {confirmationCode.Length}"
);

var couponProfile = new CouponCodeProfile(partLength: 6, partCount: 1, includeCheckDigit: true);
var coupon = coupons.Generate(couponProfile);
var couponValid = coupons.TryValidate(coupon, couponProfile, out var normalizedCoupon);
var canonicalCoupon = CouponCodeGenerator.Normalize(coupon);
Console.WriteLine($"coupon length: {coupon.Length}; normalized length: {canonicalCoupon.Length}");
Expect(
    couponValid
        && normalizedCoupon is not null
        && coupons.TryValidate(canonicalCoupon, couponProfile, out _),
    $"a single-part six-character coupon with a check digit round-trips through TryValidate and Normalize, got length {coupon.Length}"
);

var numericId = numericCodes.Generate(new NumericIdProfile(1_000_000_000L, 10_000_000_000L));
Console.WriteLine($"numeric id: {numericId}");
Expect(
    numericId.Length == 10
        && numericId[0] != '0'
        && long.TryParse(
            numericId,
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out var idValue
        )
        && idValue is >= 1_000_000_000L and < 10_000_000_000L,
    $"a ten-digit numeric id is ten characters and does not start with zero, got {numericId}"
);

var token = tokens.GenerateToken(byteLength: 32);
var tokenBytes = new byte[32];
var decodedLength = Base64Url.DecodeFromChars(token, tokenBytes);
Console.WriteLine($"token length: {token.Length}; decoded byte length: {decodedLength}");
Expect(
    token.Length == 43 && decodedLength == 32,
    $"a 32-byte token is 43 unpadded Base64Url characters and decodes back to 32 bytes, got {token.Length} characters decoding to {decodedLength} bytes"
);

var nonce = tokens.GenerateHexNonce(byteLength: 8);
Console.WriteLine($"hex nonce length: {nonce.Length}");
Expect(
    nonce.Length == 16 && nonce.All(static c => c is (>= '0' and <= '9') or (>= 'a' and <= 'f')),
    $"an 8-byte hex nonce is 16 lowercase hex characters, got length {nonce.Length}"
);

var orderId = prefixed.Generate("ORD");
Console.WriteLine($"prefixed order id: {orderId}");
Expect(
    orderId.Length == 20
        && orderId.StartsWith("ORD", StringComparison.Ordinal)
        && prefixed.RemainingRandomLength("ORD") == 17,
    $"a prefixed order id is exactly 20 characters starting with ORD, got {orderId}"
);

var compactId = stringIds.CreateStringId();
Console.WriteLine($"compact guid id: {compactId}");
Expect(
    compactId.Length == 32 && Guid.TryParseExact(compactId, "N", out _),
    $"the compact guid id parses as a Guid in N format, got {compactId}"
);

var invoiceBatch = DistinctBatch.Generate(
    _ => $"INV-{randomStrings.Generate(new RandomStringProfile(RandomAlphabet.AlphaNumeric, 10))}",
    1_000
);
var distinctInvoiceCount = invoiceBatch.Values.ToHashSet().Count;
Console.WriteLine(
    $"invoice batch: {invoiceBatch.Status}, {invoiceBatch.Values.Count} values "
        + $"({distinctInvoiceCount} distinct) in {invoiceBatch.Attempts} attempts"
);
Expect(
    invoiceBatch.Status == DistinctBatchStatus.Success
        && invoiceBatch.Values.Count == 1_000
        && distinctInvoiceCount == 1_000
        && invoiceBatch.Attempts >= 1_000,
    $"a distinct batch of 1,000 invoice codes reports Success with 1,000 distinct values, got {invoiceBatch.Status} with {distinctInvoiceCount} distinct"
);

IReadOnlyList<string> regions = ["eu", "us"];
var region = SecureChoice.Choose(regions);
Console.WriteLine($"chosen catalog region: {region}");
Expect(regions.Contains(region), $"SecureChoice returns a member of the list, got {region}");

Console.WriteLine(
    problems.Count == 0
        ? "generators verified: numeric codes, coupons, tokens, nonces, prefixed ids, guid ids, distinct batches, and secure choice all match"
        : "generator mismatches: " + string.Join("; ", problems)
);
return problems.Count == 0 ? 0 : 1;

void Expect(bool condition, string what)
{
    if (!condition)
    {
        problems.Add(what);
    }
}
