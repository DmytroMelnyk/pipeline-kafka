using Confluent.Kafka;

namespace Pipeline.Kafka.Extensions;

//https://github.com/confluentinc/confluent-kafka-dotnet/issues/1164
internal static class ConsumerExtensions
{
    public static IBatch<ConsumeResult<TKey, TValue>> ConsumeBatch<TKey, TValue>(this IConsumer<TKey, TValue> consumer, int maxBatchSize, CancellationToken cancellationToken)
    {
        var message = consumer.Consume(cancellationToken);
        return Enumerable
            .Range(1, maxBatchSize - 1)
            .Select(_ => consumer.Consume(TimeSpan.Zero))
            .Prepend(message)
            .TakeWhile(x => x?.Message is not null)
            .ToReadOnlyBatch();
    }
}
