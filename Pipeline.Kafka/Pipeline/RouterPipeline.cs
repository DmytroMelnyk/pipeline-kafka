using Confluent.Kafka;
using Pipeline.Kafka.Client;
using Pipeline.Kafka.Config;

namespace Pipeline.Kafka.Pipeline;

internal class RouterPipeline<TKey, TValue> : IChainOfResponsibility<IKafkaMessage<TKey, TValue>>, IChainOfResponsibility<Message<byte[], byte[]>>
{
    private readonly string _topicName;
    private readonly IEnumerable<IPipelineLink<IKafkaMessage<TKey, TValue>>> _before;
    private readonly ISerializer<TKey> _keySerializer;
    private readonly ISerializer<TValue> _valueSerializer;
    private readonly IEnumerable<IPipelineLink<Message<byte[], byte[]>>> _after;
    private readonly IProducer<byte[], byte[]> _producer;

    public RouterPipeline(
        KafkaProducerOptions producerOptions,
        IEnumerable<IMessagePipelineLink<TKey, TValue>> before,
        ISerializer<TKey> keySerializer,
        ISerializer<TValue> valueSerializer,
        IEnumerable<IPipelineLink<Message<byte[], byte[]>>> after,
        IProducerFactory producerFactory)
    {
        _topicName = producerOptions.TopicName;
        _before = before;
        _keySerializer = keySerializer;
        _valueSerializer = valueSerializer;
        _after = after;
        _producer = producerFactory.GetProducerLazy(producerOptions).Value;
    }

    public Task PublishAsync(IKafkaMessage<TKey, TValue> message, CancellationToken cancellationToken) =>
        ((IChainOfResponsibility<IKafkaMessage<TKey, TValue>>) this).ExecuteAsyncImpl(_before.GetEnumerator(), message, cancellationToken);

    Task IChainOfResponsibility<IKafkaMessage<TKey, TValue>>.ExecuteHandlerAsync(IKafkaMessage<TKey, TValue> message, CancellationToken cancellationToken)
    {
        var msg = Convert(message);
        return ExecuteAsync(msg, cancellationToken);
    }

    private Message<byte[], byte[]> Convert(IKafkaMessage<TKey, TValue> message) => new()
    {
        Headers = message.Headers,
        Key = _keySerializer.Serialize(message.Key, new SerializationContext(MessageComponentType.Key, _topicName, message.Headers)),
        Value = _valueSerializer.Serialize(message.Value, new SerializationContext(MessageComponentType.Value, _topicName, message.Headers))
    };

    public Task ExecuteAsync(Message<byte[], byte[]> message, CancellationToken cancellationToken) =>
        ((IChainOfResponsibility<Message<byte[], byte[]>>) this).ExecuteAsyncImpl(_after.GetEnumerator(), message, cancellationToken);

    Task IChainOfResponsibility<Message<byte[], byte[]>>.ExecuteHandlerAsync(Message<byte[], byte[]> message, CancellationToken cancellationToken) =>
        _producer.ProduceAsync(_topicName, message, cancellationToken);
}
