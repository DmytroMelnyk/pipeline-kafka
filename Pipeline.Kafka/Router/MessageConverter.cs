using Confluent.Kafka;
using Pipeline.Kafka.Extensions;

namespace Pipeline.Kafka.Router;

internal class MessageConverter<TKey, TValue>
{
    private readonly Func<TValue, TKey> _keySelector;
    private readonly Action<TValue, Headers> _headersSelector;

    public MessageConverter(Func<TValue, TKey>? keySelector = null, Action<TValue, Headers>? headersSelector = null)
    {
        _keySelector = keySelector ?? IgnoreKey;
        _headersSelector = headersSelector ?? EmptyHeadersSelector;
    }

    public IKafkaMessage<TKey, Null> ToTombstoneUsingKeySelector(TValue value) =>
        MessageFactory.CreateKafkaMessage<TKey, Null>(_keySelector(value), null!, h => _headersSelector(value, h));

    public static IKafkaMessage<TKey, Null> ToTombstone(TKey key, Action<Headers>? configure = null) =>
        MessageFactory.CreateKafkaMessage<TKey, Null>(key, null!, configure);

    public IKafkaMessage<TKey, TValue> ToKafkaMessage(TValue value) =>
        MessageFactory.CreateKafkaMessage(_keySelector(value), value, h => _headersSelector(value, h));

    public static IKafkaMessage<TKey, TValue> ToKafkaMessage(TKey key, TValue value, Action<Headers>? configure = null) =>
        MessageFactory.CreateKafkaMessage(key, value, configure);

    public static IKafkaMessage<TKey, TValue> ToKafkaMessage(TKey key, TValue value, Headers headers, Timestamp? timestamp = null) =>
        MessageFactory.CreateKafkaMessage(key, value, headers, timestamp.GetValueOrDefault(Timestamp.Default));

#pragma warning disable S1172 // Unused method parameters should be removed
    private static TKey IgnoreKey(TValue _) => default!;
#pragma warning restore S1172 // Unused method parameters should be removed

    private static void EmptyHeadersSelector(TValue _, Headers __)
    {
        // Method intentionally left empty.
    }
}
