namespace Pipeline.Kafka;

public interface IMessageHandler<in TMessage>
{
    Task Handle(TMessage message, CancellationToken cancellationToken);
}

public interface IMessageHandler<in TKey, in TValue> : IMessageHandler<IKafkaMessage<TKey, TValue>>
{
}

public interface IBatchMessageHandler<in TMessage> : IMessageHandler<IBatch<TMessage>>
{
}

public interface IBatchMessageHandler<in TKey, in TValue> : IBatchMessageHandler<IKafkaMessage<TKey, TValue>>
{
}
