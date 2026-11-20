using System.Globalization;
using System.Threading.RateLimiting;
using HostLoom.RateLimiting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.RateLimiting;

namespace HostLoom.AspNetCore.RateLimiting;

internal sealed class EndpointRateLimitPolicy(
    DistributedRateLimiter limiter,
    IHttpContextAccessor accessor,
    Func<HttpContext, string?> selectKey,
    Func<OnRejectedContext, CancellationToken, ValueTask>? onRejected
) : IRateLimiterPolicy<int>
{
    // ASP.NET caches by policy and partition. Keep a single adapter per policy and resolve
    // the tenant during acquisition so the cache never holds tenant IDs or request contexts.
    private readonly RateLimitPartition<int> _partition = RateLimitPartition.Get(
        0,
        _ => new EndpointLimiter(limiter, accessor, selectKey)
    );

    public RateLimitPartition<int> GetPartition(HttpContext httpContext) => _partition;

    public Func<OnRejectedContext, CancellationToken, ValueTask> OnRejected { get; } =
        async (context, token) =>
        {
            SetRejectionResponse(context);
            if (onRejected is not null)
                await onRejected(context, token).ConfigureAwait(false);
        };

    private static void SetRejectionResponse(OnRejectedContext context)
    {
        var response = context.HttpContext.Response;
        if (context.Lease is IdentityRejection rejection)
            response.StatusCode = rejection.StatusCode;
        else if (
            context.Lease.TryGetMetadata(RateLimitMetadata.Unavailable, out bool unavailable)
            && unavailable
        )
            response.StatusCode = StatusCodes.Status503ServiceUnavailable;
        else
        {
            response.StatusCode = StatusCodes.Status429TooManyRequests;
            if (context.Lease.TryGetMetadata(MetadataName.RetryAfter, out var delay))
                response.Headers.RetryAfter = Math.Ceiling(delay.TotalSeconds)
                    .ToString(CultureInfo.InvariantCulture);
        }
    }

    private sealed class EndpointLimiter(
        DistributedRateLimiter limiter,
        IHttpContextAccessor accessor,
        Func<HttpContext, string?> selectKey
    ) : RateLimiter
    {
        public override TimeSpan? IdleDuration => null;

        public override RateLimiterStatistics? GetStatistics() => null;

        // The kernel's synchronous probe performs no store I/O. ASP.NET then calls AcquireAsync.
        protected override RateLimitLease AttemptAcquireCore(int permitCount) =>
            limiter.AttemptAcquire(permitCount);

        protected override ValueTask<RateLimitLease> AcquireAsyncCore(
            int permitCount,
            CancellationToken cancellationToken
        )
        {
            cancellationToken.ThrowIfCancellationRequested();
            var context =
                accessor.HttpContext
                ?? throw new InvalidOperationException("An active HTTP request is required.");
            var key = selectKey(context);
            if (limiter.IsValidPartitionKey(key))
                return limiter.AcquirePartitionAsync(key, permitCount, cancellationToken);

            return ValueTask.FromResult<RateLimitLease>(
                context.User.Identity?.IsAuthenticated == true
                    ? IdentityRejection.Forbidden
                    : IdentityRejection.Unauthenticated
            );
        }
    }

    private sealed class IdentityRejection(int statusCode) : RateLimitLease
    {
        internal static readonly IdentityRejection Unauthenticated = new(
            StatusCodes.Status401Unauthorized
        );
        internal static readonly IdentityRejection Forbidden = new(StatusCodes.Status403Forbidden);
        internal int StatusCode => statusCode;
        public override bool IsAcquired => false;
        public override IEnumerable<string> MetadataNames => [];

        public override bool TryGetMetadata(string metadataName, out object? metadata)
        {
            metadata = null;
            return false;
        }
    }
}
