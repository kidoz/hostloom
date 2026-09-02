using StackExchange.Redis;

namespace HostLoom.Redis.Internal;

/// <summary>Turns the kernels' fully prefixed keys into Redis keys, wrapping the namespace in a hash tag when asked.</summary>
internal static class RedisKeys
{
    public static RedisKey ToRedisKey(string key, bool useHashTags)
    {
        if (!useHashTags)
        {
            return key;
        }

        var separator = key.IndexOf(':', StringComparison.Ordinal);
        return separator <= 0
            ? "{" + key + "}"
            : string.Concat("{", key.AsSpan(0, separator), "}", key.AsSpan(separator));
    }
}
