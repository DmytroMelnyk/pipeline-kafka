using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace Pipeline.Kafka.SerDes;

public class JsonSerializer<T> : ISerializer<T>
{
    private readonly JsonSerializerOptions? _jsonSerializerOptions;

    public JsonSerializer(JsonSerializerOptions? jsonSerializerOptions = null) =>
        _jsonSerializerOptions = jsonSerializerOptions;

    public byte[]? Serialize(T data, SerializationContext context)
    {
        if (data == null)
        {
            return null;
        }

        return Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data, _jsonSerializerOptions));
    }
}
