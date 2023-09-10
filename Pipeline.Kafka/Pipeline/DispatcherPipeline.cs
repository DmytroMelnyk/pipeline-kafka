using Confluent.Kafka;
using Pipeline.Kafka.Extensions;

namespace Pipeline.Kafka.Pipeline;

internal class DispatcherPipeline<TKey, TValue> : IChainOfResponsibility<ConsumeResult<byte[], byte[]>>, IChainOfResponsibility<IKafkaConsumeResult<TKey, TValue>>
{
    private readonly IEnumerable<IPipelineLink<ConsumeResult<byte[], byte[]>>> _beforeChain;
    private readonly IDeserializer<TKey> _keyDeserializer;
    private readonly IDeserializer<TValue> _valueDeserializer;
    private readonly IEnumerable<IPipelineLink<IKafkaConsumeResult<TKey, TValue>>> _afterChain;
    private readonly IEnumerable<IMessageHandler<TKey, TValue>> _messageHandlers;

    public DispatcherPipeline(
        IEnumerable<IPipelineLink<ConsumeResult<byte[], byte[]>>> beforeChain,
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer,
        IEnumerable<IPipelineLink<TKey, TValue>> afterChain,
        IEnumerable<IMessageHandler<TKey, TValue>> messageHandlers)
    {
        _beforeChain = beforeChain;
        _keyDeserializer = keyDeserializer;
        _valueDeserializer = valueDeserializer;
        _afterChain = afterChain;
        _messageHandlers = messageHandlers;
    }

    public Task ExecuteAsync(ConsumeResult<byte[], byte[]> consumeResult, CancellationToken cancellationToken) =>
        ((IChainOfResponsibility<ConsumeResult<byte[], byte[]>>)this).ExecuteAsyncImpl(_beforeChain.GetEnumerator(), consumeResult, cancellationToken);

    Task IChainOfResponsibility<ConsumeResult<byte[], byte[]>>.ExecuteHandlerAsync(ConsumeResult<byte[], byte[]> consumeResult, CancellationToken cancellationToken)
    {
        var message = Convert(consumeResult);
        return ExecuteAsync(message, cancellationToken);
    }

    private IKafkaConsumeResult<TKey, TValue> Convert(ConsumeResult<byte[], byte[]> consumeResult)
    {
        var message = consumeResult.Message;
        return MessageFactory.CreateKafkaConsumeResult(
            _keyDeserializer.Deserialize(message.Key, message.Key == null, new SerializationContext(MessageComponentType.Key, consumeResult.Topic, message.Headers)),
            _valueDeserializer.Deserialize(message.Value, message.Value == null, new SerializationContext(MessageComponentType.Value, consumeResult.Topic, message.Headers)),
            message.Headers,
            message.Timestamp,
            consumeResult.TopicPartitionOffset
        );
    }

    public Task ExecuteAsync(IKafkaConsumeResult<TKey, TValue> message, CancellationToken cancellationToken) =>
        ((IChainOfResponsibility<IKafkaConsumeResult<TKey, TValue>>)this).ExecuteAsyncImpl(_afterChain.GetEnumerator(), message, cancellationToken);

    Task IChainOfResponsibility<IKafkaConsumeResult<TKey, TValue>>.ExecuteHandlerAsync(IKafkaConsumeResult<TKey, TValue> message, CancellationToken cancellationToken) =>
        Task.WhenAll(_messageHandlers.Select(x => x.Handle(message, cancellationToken)));
}
