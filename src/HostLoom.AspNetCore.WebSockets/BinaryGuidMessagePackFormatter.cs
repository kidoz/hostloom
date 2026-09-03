using System.Buffers;
using MessagePack;
using MessagePack.Formatters;

namespace HostLoom.AspNetCore.WebSockets;

/// <summary>
/// Writes a <see cref="Guid"/> as 16 big-endian bytes. MessagePack's built-in formatter spells a
/// identifier as 36 ASCII characters, which more than doubles the size of every event frame and
/// disagrees with the byte order the Protocol Buffers contract publishes for the same field.
/// </summary>
internal sealed class BinaryGuidMessagePackFormatter : IMessagePackFormatter<Guid>
{
    public static readonly BinaryGuidMessagePackFormatter Instance = new();

    public void Serialize(
        ref MessagePackWriter writer,
        Guid value,
        MessagePackSerializerOptions options
    )
    {
        Span<byte> bytes = stackalloc byte[16];
        _ = value.TryWriteBytes(bytes, bigEndian: true, out _);
        writer.Write(bytes);
    }

    public Guid Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        if (reader.ReadBytes() is not { Length: 16 } sequence)
        {
            throw new MessagePackSerializationException("A frame identifier must be 16 bytes.");
        }

        Span<byte> bytes = stackalloc byte[16];
        sequence.CopyTo(bytes);
        return new Guid(bytes, bigEndian: true);
    }
}

/// <summary>Applies <see cref="BinaryGuidMessagePackFormatter"/> to the optional frame identifiers.</summary>
internal sealed class NullableBinaryGuidMessagePackFormatter : IMessagePackFormatter<Guid?>
{
    public static readonly NullableBinaryGuidMessagePackFormatter Instance = new();

    public void Serialize(
        ref MessagePackWriter writer,
        Guid? value,
        MessagePackSerializerOptions options
    )
    {
        if (value is not { } identifier)
        {
            writer.WriteNil();
            return;
        }

        BinaryGuidMessagePackFormatter.Instance.Serialize(ref writer, identifier, options);
    }

    public Guid? Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options) =>
        reader.TryReadNil()
            ? null
            : BinaryGuidMessagePackFormatter.Instance.Deserialize(ref reader, options);
}
