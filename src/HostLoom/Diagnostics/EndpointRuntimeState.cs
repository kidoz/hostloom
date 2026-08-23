namespace HostLoom;

/// <summary>
/// Shared between the endpoint hosted service and the readiness check, because the hosted service
/// is registered as one of many <c>IHostedService</c> instances and cannot be resolved directly.
/// </summary>
internal sealed class EndpointRuntimeState
{
    private volatile bool _listening;
    private volatile int _endpointCount;

    public bool Listening => _listening;

    public int EndpointCount => _endpointCount;

    public void MarkListening(int endpointCount)
    {
        _endpointCount = endpointCount;
        _listening = true;
    }

    public void MarkStopped()
    {
        _listening = false;
        _endpointCount = 0;
    }
}
