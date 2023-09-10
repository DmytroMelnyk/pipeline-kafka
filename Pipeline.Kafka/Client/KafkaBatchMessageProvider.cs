using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Pipeline.Kafka.Config;
using Pipeline.Kafka.Dispatcher;
using Pipeline.Kafka.Extensions;

namespace Pipeline.Kafka.Client;

//https://blog.stephencleary.com/2020/05/backgroundservice-gotcha-startup.html
internal class KafkaBatchMessageProvider : BackgroundService
{
    private readonly string _topicName;
    private readonly int _maxBatchSize;
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly KafkaMessageDispatcher _messageDispatcher;
    private readonly ILogger<KafkaMessageProvider> _logger;

    public KafkaBatchMessageProvider(KafkaConsumerOptions consumerOptions, int maxBatchSize, IConsumerFactory consumerFactory, KafkaMessageDispatcher messageDispatcher, ILogger<KafkaMessageProvider> logger)
    {
        _logger = logger;
        _topicName = consumerOptions.TopicName;
        _maxBatchSize = maxBatchSize;
        _consumer = consumerFactory.CreateConsumer(consumerOptions);
        _messageDispatcher = messageDispatcher;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Factory.StartNew(async () =>
    {
        Thread.CurrentThread.Name = $"{_topicName} topic reader";

        foreach (var batch in GetMessageBatches(stoppingToken))
        {
            try
            {
                await _messageDispatcher.DispatchAsync(batch, stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                var paritionOffsets = batch.GroupBy(x => x.Partition.Value, x => x.Offset.Value, (partition, offsets) => $"{partition}: {offsets.Min()} - {offsets.Max()}").ToList();
                _logger.LogWarning(ex, "Unprocessed kafka message with partition-offsets {@partitionOffsets} from {topicName}", paritionOffsets, _topicName);
            }
        }
    }, stoppingToken, TaskCreationOptions.DenyChildAttach | TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();

    private IEnumerable<IBatch<ConsumeResult<byte[], byte[]>>> GetMessageBatches(CancellationToken token)
    {
        _consumer.Subscribe(_topicName);

        while (!token.IsCancellationRequested)
        {
            var batch = _consumer.ConsumeBatch(_maxBatchSize, token);
            yield return batch;
            foreach (var cr in batch)
            {
                _consumer.StoreOffset(cr);
            }
        }

        _consumer.Unsubscribe();
    }
}
