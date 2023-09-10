using Confluent.Kafka;
using Pipeline.Kafka.Config;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace Pipeline.Kafka.Client;

internal class ProducerFactory : IProducerFactory, IDisposable
{
    private readonly ConcurrentDictionary<ProducerConfig, Lazy<IProducer<byte[], byte[]>>> _producerBuilders = new(new ProducerConfigEqualityComparer());

    public Lazy<IProducer<byte[], byte[]>> GetProducerLazy(KafkaProducerOptions options)
    {
        return _producerBuilders
            .GetOrAdd(options, new Lazy<IProducer<byte[], byte[]>>(() => new ProducerBuilder<byte[], byte[]>(options).Build(), LazyThreadSafetyMode.ExecutionAndPublication));
    }

    public void Dispose()
    {
        foreach (var builder in _producerBuilders.Values.Where(x => x.IsValueCreated))
        {
            builder.Value.Dispose();
        }
    }

    private class ProducerConfigEqualityComparer : IEqualityComparer<ProducerConfig>
    {
        public bool Equals(ProducerConfig? x, ProducerConfig? y)
        {
            if (x is null)
            {
                return y is null;
            }

            if (y is null)
            {
                return false;
            }

            if (x.GetType() != y.GetType())
            {
                return false;
            }

            return x.SequenceEqual(y);
        }

        public int GetHashCode([DisallowNull] ProducerConfig obj)
        {
            return obj.BootstrapServers.GetHashCode();
        }
    }
}
