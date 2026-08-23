namespace HostLoom;

internal static class MessageTypeName
{
    public static string For<T>() => For(typeof(T));

    public static string For(Type type)
    {
        var assembly = type.Assembly.GetName().Name;
        return $"{assembly}:{type.FullName ?? type.Name}";
    }
}
