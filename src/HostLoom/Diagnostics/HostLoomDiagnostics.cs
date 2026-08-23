using System.Diagnostics;

namespace HostLoom;

public static class HostLoomDiagnostics
{
    public const string ActivitySourceName = "HostLoom";

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);
}
