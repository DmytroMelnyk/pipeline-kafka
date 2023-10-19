using Confluent.Kafka;
using Pipeline.Kafka.Config;

namespace Pipeline.Kafka.Client;

internal interface IConsumerFactory
{
    IConsumer<byte[], byte[]> CreateConsumer(KafkaConsumerOptions options);
}
