using Confluent.Kafka;

namespace Pipeline.Kafka;

public interface IKafkaMessage<out TKey, out TValue>
{
    TKey Key
    {
        get;
    }

    Headers Headers
    {
        get;
    }

    TValue Value
    {
        get;
    }

    Timestamp Timestamp
    {
        get;
    }
}

public interface IKafkaConsumeResult<out TKey, out TValue> : IKafkaMessage<TKey, TValue>
{
    TopicPartitionOffset TopicPartitionOffset
    {
        get;
    }
}
