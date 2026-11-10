using System.Diagnostics.Metrics;
using System.Net.Sockets;
using System.Security.Authentication;

namespace HostLoom.DataGrid.Transport;

internal enum DataGridTransportRole
{
    Peer,
    Client,
}

internal enum DataGridTransportDirection
{
    Inbound,
    Outbound,
}

internal enum DataGridTransportResource
{
    Connection,
    Handshake,
    Member,
    Principal,
    Request,
    Snapshot,
}

internal enum DataGridTransportRejection
{
    Capacity,
    Rate,
    Unauthorized,
    InvalidRequest,
}

internal sealed class DataGridTransportAdmissionException()
    : IOException("Transport admission refused.");

/// <summary>Fixed-label transport reservations and exchange outcomes for the experimental grid.</summary>
public static class DataGridTransportDiagnostics
{
    public const string MeterName = "HostLoom.DataGrid.Transport";
    public const string RoleTag = "hostloom.datagrid.transport.role";
    public const string DirectionTag = "hostloom.datagrid.transport.direction";
    public const string ResourceTag = "hostloom.datagrid.transport.resource";
    public const string ReasonTag = "hostloom.datagrid.transport.reason";
    public const string OutcomeTag = "hostloom.datagrid.transport.outcome";
    private static readonly Meter Meter = new(MeterName);
    private static readonly UpDownCounter<long> Active = Meter.CreateUpDownCounter<long>(
        "hostloom.datagrid.transport.reservations.active",
        "{reservation}",
        "Held transport reservations, not distinct sockets or principals."
    );
    private static readonly UpDownCounter<long> Bytes = Meter.CreateUpDownCounter<long>(
        "hostloom.datagrid.transport.reservations.bytes",
        "By",
        "Reserved encoded client work or inbound snapshot image bytes; not heap usage."
    );
    private static readonly Counter<long> Rejected = Meter.CreateCounter<long>(
        "hostloom.datagrid.transport.admission.rejected",
        "{rejection}",
        "Explicit capacity, rate, request-validation or authorization refusals at an admission boundary."
    );
    private static readonly Counter<long> Exchanges = Meter.CreateCounter<long>(
        "hostloom.datagrid.transport.exchanges",
        "{exchange}",
        "Accepted inbound sessions and outbound peer invocations by terminal transport outcome; not application success."
    );

    internal static void Change(
        DataGridTransportRole role,
        DataGridTransportDirection direction,
        DataGridTransportResource resource,
        int count,
        long bytes = 0
    )
    {
        KeyValuePair<string, object?> roleTag = new(RoleTag, Role(role)),
            directionTag = new(DirectionTag, Direction(direction)),
            resourceTag = new(ResourceTag, Resource(resource));
        Active.Add(count, roleTag, directionTag, resourceTag);
        if (bytes != 0)
            Bytes.Add(bytes, roleTag, directionTag, resourceTag);
    }

    internal static void Reject(
        DataGridTransportRole role,
        DataGridTransportDirection direction,
        DataGridTransportResource resource,
        DataGridTransportRejection reason
    )
    {
        System.Diagnostics.TagList tags = new()
        {
            { RoleTag, Role(role) },
            { DirectionTag, Direction(direction) },
            { ResourceTag, Resource(resource) },
            {
                ReasonTag,
                reason switch
                {
                    DataGridTransportRejection.Capacity => "capacity",
                    DataGridTransportRejection.Rate => "rate",
                    DataGridTransportRejection.Unauthorized => "unauthorized",
                    _ => "invalid_request",
                }
            },
        };
        Rejected.Add(1, tags);
    }

    internal static void Exchange(
        DataGridTransportRole role,
        DataGridTransportDirection direction,
        Exception? error
    ) =>
        Exchanges.Add(
            1,
            new(RoleTag, Role(role)),
            new(DirectionTag, Direction(direction)),
            new(
                OutcomeTag,
                error switch
                {
                    null => "completed",
                    DataGridTransportAdmissionException => "rejected",
                    DataGridCredentialRetiredException => "retired",
                    AuthenticationException => "authentication",
                    InvalidDataException => "protocol",
                    OperationCanceledException => "canceled",
                    IOException or SocketException or ObjectDisposedException => "io",
                    _ => "faulted",
                }
            )
        );

    private static string Role(DataGridTransportRole role) =>
        role == DataGridTransportRole.Peer ? "peer" : "client";

    private static string Direction(DataGridTransportDirection direction) =>
        direction == DataGridTransportDirection.Inbound ? "inbound" : "outbound";

    private static string Resource(DataGridTransportResource resource) =>
        resource switch
        {
            DataGridTransportResource.Connection => "connection",
            DataGridTransportResource.Handshake => "handshake",
            DataGridTransportResource.Member => "member",
            DataGridTransportResource.Principal => "principal",
            DataGridTransportResource.Request => "request",
            _ => "snapshot",
        };
}
