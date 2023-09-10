using Confluent.Kafka;

namespace Pipeline.Kafka.Config;

public class KafkaConsumerOptions : ConsumerConfig
{
    public bool AddMachineName { get; set; }

    public required string TopicName { get; set; }

    public int? PoolSize { get; set; }

    public int? MaxBatchSize { get; set; }
}
