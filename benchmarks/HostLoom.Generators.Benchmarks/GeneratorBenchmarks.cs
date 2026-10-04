using BenchmarkDotNet.Attributes;

// CA1707: underscored benchmark names are how the results table stays readable.
// CA1822: BenchmarkDotNet discovers instance methods, so a stateless benchmark cannot be static.
// CA1055: "Url" in a benchmark name describes the encoding, not a URI return value.
#pragma warning disable CA1707, CA1822, CA1055

namespace HostLoom.Generators.Benchmarks;

/// <summary>
/// One generation call per benchmark, steady state, across every generator shape the package
/// ships: fixed-width numeric codes, single-part coupons, random strings at the three lengths an
/// application actually picks, an opaque Base64Url token, a large distinct batch, and one shared
/// <see cref="RandomStringGenerator"/> singleton driven concurrently. Profiles are built once as
/// fields so the numbers are the generation cost, not profile construction. There is no baseline
/// row: the audit supplies no latency or allocation targets, so these numbers are recorded for
/// comparison between runs rather than gated.
/// </summary>
[MemoryDiagnoser]
public class GeneratorBenchmarks
{
    private static readonly NumericCodeProfile FiveDigits = new(digits: 5);
    private static readonly CouponCodeProfile SixCharCoupon = new(partLength: 6, partCount: 1);
    private static readonly CouponCodeProfile EightCharCoupon = new(partLength: 8, partCount: 1);
    private static readonly RandomStringProfile AlphaNumeric10 = new(
        RandomAlphabet.AlphaNumeric,
        10
    );
    private static readonly RandomStringProfile AlphaNumeric20 = new(
        RandomAlphabet.AlphaNumeric,
        20
    );
    private static readonly RandomStringProfile AlphaNumeric32 = new(
        RandomAlphabet.AlphaNumeric,
        32
    );

    private readonly RandomStringGenerator _randomStrings = new();
    private readonly NumericCodeGenerator _numericCodes = new();
    private readonly CouponCodeGenerator _coupons = new();
    private readonly OpaqueTokenGenerator _tokens = new();

    [Benchmark(Description = "Five-digit numeric code")]
    public string NumericCode_FiveDigits() => _numericCodes.Generate(FiveDigits);

    [Benchmark(Description = "Six-character single-part coupon")]
    public string Coupon_SixChar_SinglePart() => _coupons.Generate(SixCharCoupon);

    [Benchmark(Description = "Eight-character single-part coupon")]
    public string Coupon_EightChar_SinglePart() => _coupons.Generate(EightCharCoupon);

    [Benchmark(Description = "Ten-character alphanumeric random string")]
    public string RandomString_AlphaNumeric_10() => _randomStrings.Generate(AlphaNumeric10);

    [Benchmark(Description = "Twenty-character alphanumeric random string")]
    public string RandomString_AlphaNumeric_20() => _randomStrings.Generate(AlphaNumeric20);

    [Benchmark(Description = "Thirty-two-character alphanumeric random string")]
    public string RandomString_AlphaNumeric_32() => _randomStrings.Generate(AlphaNumeric32);

    [Benchmark(Description = "32-byte unpadded Base64Url token")]
    public string Token_Base64Url_32Bytes() => _tokens.GenerateToken(byteLength: 32);

    /// <summary>
    /// A 10,000-item distinct batch of ten-character alphanumeric strings. The candidate space is
    /// 62^10, so retries should be rare and the row measures the batch bookkeeping — the set
    /// membership checks and result collection — as much as the generation itself.
    /// </summary>
    [Benchmark(Description = "Distinct batch of 10,000 invoice codes")]
    public DistinctBatchResult<string> DistinctBatch_10_000() =>
        DistinctBatch.Generate(_ => $"INV-{_randomStrings.Generate(AlphaNumeric10)}", 10_000);

    /// <summary>
    /// Concurrent-singleton case: one shared <see cref="RandomStringGenerator"/> instance — the
    /// lifetime the package documentation recommends — called from 1,000
    /// <see cref="Parallel.For{TFrom, TTo}"/> workers. The row answers whether the shared random
    /// source serializes under contention, and its allocation column includes the parallel
    /// scaffolding itself, so compare it against the single-threaded rows with that in mind.
    /// </summary>
    [Benchmark(
        Description = "Concurrent: one shared RandomStringGenerator singleton, 1,000 Parallel.For workers"
    )]
    public void Concurrent_RandomString_SharedSingleton() =>
        Parallel.For(0, 1_000, _ => _randomStrings.Generate(AlphaNumeric10));
}
