namespace HostLoom;

public interface IMessageSerializer
{
    byte[] Serialize(object? value, Type type);

    object? Deserialize(ReadOnlySpan<byte> payload, Type type);

    byte[] Serialize<T>(T value) => Serialize(value, typeof(T));

    T? Deserialize<T>(ReadOnlySpan<byte> payload) => (T?)Deserialize(payload, typeof(T));
}
