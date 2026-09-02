namespace HostLoom.Redis.Internal;

/// <summary>
/// The shape of a namespace's keys on the server, so an invalidation that names a Redis key can
/// be turned back into the consumer key the in-process tier is indexed by.
/// </summary>
internal sealed class RedisKeyLayout
{
    public RedisKeyLayout(string @namespace, bool useHashTags, string? payloadVersion)
    {
        Namespace = @namespace;
        var prefix = useHashTags ? "{" + @namespace + "}" : @namespace;
        DataPrefix = prefix + ":cache:data:";
        VersionSuffix = payloadVersion is null ? "" : ":" + payloadVersion;
    }

    public string Namespace { get; }

    /// <summary>The prefix of every cache entry key as Redis sees it.</summary>
    public string DataPrefix { get; }

    /// <summary>The payload-version suffix appended to every entry key, or empty.</summary>
    public string VersionSuffix { get; }

    /// <summary>Recovers the consumer key from a Redis entry key; false for leases, tags, locks, and other namespaces.</summary>
    public bool TryParseDataKey(string redisKey, out string consumerKey)
    {
        consumerKey = "";
        if (!redisKey.StartsWith(DataPrefix, StringComparison.Ordinal))
        {
            return false;
        }

        var rest = redisKey.AsSpan(DataPrefix.Length);
        if (VersionSuffix.Length > 0)
        {
            if (!rest.EndsWith(VersionSuffix, StringComparison.Ordinal))
            {
                return false;
            }

            rest = rest[..^VersionSuffix.Length];
        }

        if (rest.IsEmpty)
        {
            return false;
        }

        consumerKey = rest.ToString();
        return true;
    }
}
