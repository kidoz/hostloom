using Microsoft.Extensions.Logging;

namespace HostLoom;

/// <summary>Stable event ids for every log line the inbox filter writes.</summary>
public static class InboxEvents
{
    /// <summary>Debug: a redelivered event was recognised and its handlers were not run.</summary>
    public static readonly EventId Duplicate = new(3310, "InboxDuplicate");

    /// <summary>Warning: the store could not say whether the delivery was seen, so the handlers ran.</summary>
    public static readonly EventId StoreUnavailable = new(3311, "InboxStoreUnavailable");
}
