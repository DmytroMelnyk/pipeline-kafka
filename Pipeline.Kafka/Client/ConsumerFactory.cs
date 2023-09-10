using System.Collections.Concurrent;
using Confluent.Kafka;
using Pipeline.Kafka.Config;

namespace Pipeline.Kafka.Client;

internal class ConsumerFactory : IConsumerFactory, IDisposable
{
    private readonly ConcurrentQueue<IConsumer<byte[], byte[]>> _consumers = new();

    public IConsumer<byte[], byte[]> CreateConsumer(KafkaConsumerOptions options)
    {
        var consumer = new ConsumerBuilder<byte[], byte[]>(options)
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                // https://docs.confluent.io/kafka-clients/dotnet/current/overview.html#committing-during-a-rebalance
            })
            .Build();

        _consumers.Enqueue(consumer);
        return consumer;
    }

    public void Dispose()
    {
        foreach (var item in _consumers)
        {
            item.Close();
            item.Dispose();
        }
    }
}
