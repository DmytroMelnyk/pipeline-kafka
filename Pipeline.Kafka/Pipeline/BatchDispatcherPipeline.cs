using Confluent.Kafka;
using Pipeline.Kafka.Extensions;

namespace Pipeline.Kafka.Pipeline;

internal class BatchDispatcherPipeline<TKey, TValue> : IChainOfResponsibility<IBatch<ConsumeResult<byte[], byte[]>>>, IChainOfResponsibility<IBatch<IKafkaConsumeResult<TKey, TValue>>>
{
    private readonly IEnumerable<IBatchPipelineLink<ConsumeResult<byte[], byte[]>>> _beforeChain;
    private readonly IDeserializer<TKey> _keyDeserializer;
    private readonly IDeserializer<TValue> _valueDeserializer;
    private readonly IEnumerable<IBatchPipelineLink<IKafkaConsumeResult<TKey, TValue>>> _afterChain;
    private readonly IEnumerable<IBatchMessageHandler<TKey, TValue>> _messageHandlers;

    public BatchDispatcherPipeline(
        IEnumerable<IBatchPipelineLink<ConsumeResult<byte[], byte[]>>> beforeChain,
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TValue> valueDeserializer,
        IEnumerable<IBatchPipelineLink<TKey, TValue>> afterChain,
        IEnumerable<IBatchMessageHandler<TKey, TValue>> messageHandlers)
    {
        _beforeChain = beforeChain;
        _keyDeserializer = keyDeserializer;
        _valueDeserializer = valueDeserializer;
        _afterChain = afterChain;
        _messageHandlers = messageHandlers;
    }

    public Task ExecuteAsync(IBatch<ConsumeResult<byte[], byte[]>> consumeResults, CancellationToken cancellationToken) =>
        ((IChainOfResponsibility<IBatch<ConsumeResult<byte[], byte[]>>>)this).ExecuteAsyncImpl(_beforeChain.GetEnumerator(), consumeResults, cancellationToken);

    Task IChainOfResponsibility<IBatch<ConsumeResult<byte[], byte[]>>>.ExecuteHandlerAsync(IBatch<ConsumeResult<byte[], byte[]>> consumeResults, CancellationToken cancellationToken)
    {
        var messages = Convert(consumeResults);
        return ExecuteAsync(messages, cancellationToken);
    }

    private IBatch<IKafkaConsumeResult<TKey, TValue>> Convert(IBatch<ConsumeResult<byte[], byte[]>> consumeResults) =>
        consumeResults.Select(Convert).ToBatch();

    private IKafkaConsumeResult<TKey, TValue> Convert(ConsumeResult<byte[], byte[]> consumeResult)
    {
        var message = consumeResult.Message;
        return MessageFactory.CreateKafkaConsumeResult(
            _keyDeserializer.Deserialize(message.Key, message.Key == null, new SerializationContext(MessageComponentType.Key, consumeResult.Topic, consumeResult.Message.Headers)),
            _valueDeserializer.Deserialize(message.Value, message.Value == null, new SerializationContext(MessageComponentType.Value, consumeResult.Topic, consumeResult.Message.Headers)),
            message.Headers,
            message.Timestamp,
            consumeResult.TopicPartitionOffset
        );
    }

    public Task ExecuteAsync(IBatch<IKafkaConsumeResult<TKey, TValue>> messages, CancellationToken cancellationToken) =>
        ((IChainOfResponsibility<IBatch<IKafkaConsumeResult<TKey, TValue>>>)this).ExecuteAsyncImpl(_afterChain.GetEnumerator(), messages, cancellationToken);

    Task IChainOfResponsibility<IBatch<IKafkaConsumeResult<TKey, TValue>>>.ExecuteHandlerAsync(IBatch<IKafkaConsumeResult<TKey, TValue>> messages, CancellationToken cancellationToken) =>
        Task.WhenAll(_messageHandlers.Select(x => x.Handle(messages, cancellationToken)));
}
