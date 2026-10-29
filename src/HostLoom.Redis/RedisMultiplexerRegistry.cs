using System.Runtime.CompilerServices;
using StackExchange.Redis;

namespace HostLoom.Redis;

internal static class RedisMultiplexerRegistry
{
    private static readonly ConditionalWeakTable<IConnectionMultiplexer, MultiplexerState> States =
        new();

    public static SemaphoreSlim TrackingGate(IConnectionMultiplexer multiplexer) =>
        States.GetOrCreateValue(multiplexer).TrackingGate;

    public static bool TryClaimTransport(
        IConnectionMultiplexer multiplexer,
        RedisCacheInvalidationChannel owner
    )
    {
        var state = States.GetOrCreateValue(multiplexer);
        lock (state)
        {
            if (state.TransportOwner is not null && !ReferenceEquals(state.TransportOwner, owner))
            {
                return false;
            }

            state.TransportOwner = owner;
            return true;
        }
    }

    public static void ReleaseTransport(
        IConnectionMultiplexer multiplexer,
        RedisCacheInvalidationChannel owner
    )
    {
        var state = States.GetOrCreateValue(multiplexer);
        lock (state)
        {
            if (ReferenceEquals(state.TransportOwner, owner))
            {
                state.TransportOwner = null;
            }
        }
    }

    private sealed class MultiplexerState
    {
        public SemaphoreSlim TrackingGate { get; } = new(1, 1);

        public RedisCacheInvalidationChannel? TransportOwner { get; set; }
    }
}
