using System.Text.Json;

namespace HostLoom.Logging;

/// <summary>
/// The captured-field loop every JSON-shaped formatter needs. The kind decides the token: numbers
/// and pre-rendered JSON go out raw because this library produced them itself, booleans and nulls
/// get their literals, and everything else is escaped text. One copy, so a formatter cannot start
/// writing a field kind differently from its sibling.
/// </summary>
internal static class LogFieldWriter
{
    /// <summary>
    /// Writes every captured field of <paramref name="record"/> as a property of the object the
    /// writer is currently inside. Values are slices of the entry's retained buffers, so this
    /// copies nothing.
    /// </summary>
    public static void WriteFields(this Utf8JsonWriter writer, in LogRecord record)
    {
        for (var i = 0; i < record.FieldCount; i++)
        {
            record.GetField(i, out var name, out var value, out var kind);
            switch (kind)
            {
                case LogFieldKind.Number:
                case LogFieldKind.Json:
                    // Tokens the library itself produced; re-validating them would be pure cost.
                    writer.WritePropertyName(name);
                    writer.WriteRawValue(value, skipInputValidation: true);
                    break;
                case LogFieldKind.Boolean:
                    writer.WriteBoolean(name, value[0] == (byte)'t');
                    break;
                case LogFieldKind.Null:
                    writer.WriteNull(name);
                    break;
                default:
                    writer.WriteString(name, value);
                    break;
            }
        }
    }
}
