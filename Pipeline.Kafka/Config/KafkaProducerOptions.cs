using Confluent.Kafka;

namespace Pipeline.Kafka.Config;

public class KafkaProducerOptions : ProducerConfig
{
    public required string TopicName { get; init; }
}
