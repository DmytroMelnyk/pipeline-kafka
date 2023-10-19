using Confluent.Kafka;
using Pipeline.Kafka.Config;

namespace Pipeline.Kafka.Client;

internal interface IProducerFactory
{
    Lazy<IProducer<byte[], byte[]>> GetProducerLazy(KafkaProducerOptions options);
}
