namespace Pipeline.Kafka;

public interface IPipelineLink<in TMessage>
{
    Task RunAsync(TMessage message, Func<Task> next, CancellationToken cancellationToken);
}

public interface IBatchPipelineLink<in TMessage> : IPipelineLink<IBatch<TMessage>>
{
}

public interface IPipelineLink<in TKey, in TValue> : IPipelineLink<IKafkaConsumeResult<TKey, TValue>>
{
}

public interface IMessagePipelineLink<in TKey, in TValue> : IPipelineLink<IKafkaMessage<TKey, TValue>>
{
}

public interface IBatchPipelineLink<in TKey, in TValue> : IBatchPipelineLink<IKafkaConsumeResult<TKey, TValue>>
{
}
