using Confluent.Kafka;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace Pipeline.Kafka.Extensions;

public static class HeadersExtenstions
{
    public static Headers Add(this Headers @this, string key, string value)
    {
        @this.Add(key, Encoding.UTF8.GetBytes(value));
        return @this;
    }

    public static Headers TypeOf<T>(this Headers headers, T dto, string typeKey = "type") =>
        headers.Add(typeKey, typeof(T).Name);

    public static Headers Version(this Headers headers, string version, string versionKey = "version") =>
        headers.Add(versionKey, version);

    public static bool TryGetValue(this Headers @this, string key, [NotNullWhen(true)] out string? value)
    {
        if (@this.TryGetLastBytes(key, out var headerBytes))
        {
            value = Encoding.UTF8.GetString(headerBytes);
            return true;
        }

        value = null;
        return false;
    }
}
