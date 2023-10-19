using System.Text.Json;
using Confluent.Kafka;

namespace Pipeline.Kafka.SerDes;

public class JsonDeserializer<T> : IDeserializer<T>
{
    private readonly JsonSerializerOptions? _jsonSerializerOptions;

    public JsonDeserializer(JsonSerializerOptions? jsonSerializerOptions = null) =>
        _jsonSerializerOptions = jsonSerializerOptions;

    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull)
        {
            throw new ArgumentException("JsonDeserializer<T> may only be used to deserialize data that is not null.");
        }

        var retVal = JsonSerializer.Deserialize<T>(data, _jsonSerializerOptions);
        return retVal!;
    }
}
