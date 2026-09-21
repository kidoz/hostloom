using HostLoom.Caching;
using StackExchange.Redis;

namespace HostLoom.Redis.Internal;

/// <summary>Which transport the invalidation channel ended up using.</summary>
public enum RedisInvalidationTransport
{
    /// <summary>Not yet decided; the subscription is still being established.</summary>
    Pending,

    /// <summary>Only the explicit channel; the requested mode could not be enabled.</summary>
    ExplicitOnly,

    /// <summary>The explicit channel plus <c>CLIENT TRACKING</c> redirected to the subscriber.</summary>
    Tracking,

    /// <summary>The explicit channel plus keyspace notifications for the filtered prefixes.</summary>
    Broadcast,
}

/// <summary>Pure parsing behind the invalidation transports, kept separate so it is testable without a server.</summary>
internal static class RedisInvalidationDecoder
{
    public const string TrackingChannel = "__redis__:invalidate";

    private static readonly Version TrackingSince = new(6, 0);

    /// <summary>Resolves <see cref="CacheInvalidationMode.Auto"/> against the server version.</summary>
    public static RedisInvalidationTransport Resolve(
        CacheInvalidationMode mode,
        Version? serverVersion
    ) =>
        mode switch
        {
            CacheInvalidationMode.Tracking => RedisInvalidationTransport.Tracking,
            CacheInvalidationMode.Broadcast => RedisInvalidationTransport.Broadcast,
            _ => serverVersion is not null && serverVersion >= TrackingSince
                ? RedisInvalidationTransport.Tracking
                : RedisInvalidationTransport.Broadcast,
        };

    /// <summary>A tracking message names one Redis key that was modified, deleted, expired, or evicted.</summary>
    public static bool TryParseTrackingKey(
        RedisKeyLayout layout,
        string? redisKey,
        out string consumerKey
    )
    {
        consumerKey = "";
        return redisKey is not null && layout.TryParseDataKey(redisKey, out consumerKey);
    }

    /// <summary>
    /// A keyspace notification arrives on <c>__keyspace@{db}__:{key}</c> with the event name as
    /// the message. Only events that mean "the value you cached is gone" count.
    /// </summary>
    public static bool TryParseKeyspaceEvent(
        RedisKeyLayout layout,
        string? channel,
        string? eventName,
        out string consumerKey
    )
    {
        consumerKey = "";
        if (channel is null || eventName is null)
        {
            return false;
        }

        if (eventName is not ("expired" or "evicted" or "del" or "set" or "unlink"))
        {
            return false;
        }

        var marker = channel.IndexOf("__:", StringComparison.Ordinal);
        if (marker < 0 || !channel.StartsWith("__keyspace@", StringComparison.Ordinal))
        {
            return false;
        }

        return layout.TryParseDataKey(channel[(marker + 3)..], out consumerKey);
    }

    /// <summary>The keyspace patterns broadcast mode subscribes to: the configured prefixes, or the namespace's entries.</summary>
    public static IReadOnlyList<string> KeyspacePatterns(
        RedisKeyLayout layout,
        int databaseIndex,
        IReadOnlyList<string> prefixFilters
    )
    {
        var prefixes = prefixFilters.Count == 0 ? [layout.DataPrefix] : prefixFilters;
        var patterns = new List<string>(prefixes.Count);
        foreach (var prefix in prefixes)
        {
            patterns.Add($"__keyspace@{databaseIndex}__:{prefix}*");
        }

        return patterns;
    }

    /// <summary>
    /// Finds the id of this process's pub/sub connection in <c>CLIENT LIST</c> output by its
    /// client name; that is the connection tracking invalidations are redirected to.
    /// </summary>
    public static long? FindSubscriberClientId(string? clientList, string clientName) =>
        FindSubscriberClientId(clientList, clientName, out _);

    /// <summary>
    /// As <see cref="FindSubscriberClientId(string?, string)"/>, also counting how many pub/sub
    /// clients carry the name. More than one means the name is shared with another process (or
    /// a connection the server has not reaped yet), and a redirect to the first would hand this
    /// process's invalidations to whichever of them the server listed first.
    /// </summary>
    public static long? FindSubscriberClientId(
        string? clientList,
        string clientName,
        out int matches
    )
    {
        matches = 0;
        if (clientList is null)
        {
            return null;
        }

        // A RESP3 verbatim string arrives as "<type>:<payload>", for example "txt:id=…".
        if (
            clientList.Length > 4
            && clientList[3] == ':'
            && !clientList.StartsWith("id=", StringComparison.Ordinal)
        )
        {
            clientList = clientList[4..];
        }

        long? found = null;
        foreach (var line in clientList.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            long? id = null;
            var named = false;
            var pubsub = false;
            foreach (var field in line.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            {
                var separator = field.IndexOf('=', StringComparison.Ordinal);
                if (separator <= 0)
                {
                    continue;
                }

                var key = field.AsSpan(0, separator);
                var value = field.AsSpan(separator + 1);
                if (key.SequenceEqual("id") && long.TryParse(value, out var parsed))
                {
                    id = parsed;
                }
                else if (key.SequenceEqual("name") && value.SequenceEqual(clientName))
                {
                    named = true;
                }
                else if (key.SequenceEqual("flags") && value.Contains('P'))
                {
                    pubsub = true;
                }
            }

            if (id is not null && named && pubsub)
            {
                matches++;
                found ??= id;
            }
        }

        return found;
    }

    /// <summary>
    /// The value of a one-parameter <c>CONFIG GET</c> reply, which is a name/value array in
    /// RESP2 and a map in RESP3, or null when the reply has neither shape.
    /// </summary>
    public static string? ReadConfigValue(RedisResult? result)
    {
        if (result is null || result.IsNull)
        {
            return null;
        }

        try
        {
            if (result.Resp3Type == ResultType.Map)
            {
                var map = result.ToDictionary();
                return map.Count == 1 ? (string?)map.Values.First() : null;
            }

            var pairs = (RedisResult[]?)result;
            return pairs is { Length: 2 } ? (string?)pairs[1] : null;
        }
        catch (InvalidCastException)
        {
            return null;
        }
    }

    /// <summary>
    /// Whether a <c>notify-keyspace-events</c> value delivers what broadcast mode listens for:
    /// keyspace messages (<c>K</c>) for generic and string commands, or for every command
    /// (<c>A</c>). Expiry and eviction (<c>x</c>, <c>e</c>) are covered by <c>A</c> too, and
    /// otherwise only shorten staleness, so they are not required.
    /// </summary>
    public static bool KeyspaceNotificationsCover(string? flags)
    {
        if (string.IsNullOrEmpty(flags) || !flags.Contains('K', StringComparison.Ordinal))
        {
            return false;
        }

        return flags.Contains('A', StringComparison.Ordinal)
            || (
                flags.Contains('g', StringComparison.Ordinal)
                && flags.Contains('$', StringComparison.Ordinal)
            );
    }
}
