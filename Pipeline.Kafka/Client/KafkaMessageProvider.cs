using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Pipeline.Kafka.Config;
using Pipeline.Kafka.Dispatcher;

namespace Pipeline.Kafka.Client;

//https://blog.stephencleary.com/2020/05/backgroundservice-gotcha-startup.html
internal class KafkaMessageProvider : BackgroundService
{
    private readonly string _topicName;
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly KafkaMessageDispatcher _messageDispatcher;
    private readonly ILogger<KafkaMessageProvider> _logger;

    public KafkaMessageProvider(KafkaConsumerOptions consumerOptions, IConsumerFactory consumerFactory, KafkaMessageDispatcher messageDispatcher, ILogger<KafkaMessageProvider> logger)
    {
        _logger = logger;
        _topicName = consumerOptions.TopicName;
        _consumer = consumerFactory.CreateConsumer(consumerOptions);
        _messageDispatcher = messageDispatcher;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Factory.StartNew(async () =>
    {
        Thread.CurrentThread.Name = $"{_topicName} topic reader";

        foreach (var message in GetMessages(stoppingToken))
        {
            try
            {
                await _messageDispatcher.DispatchAsync(message, stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Unprocessed kafka message with offset: {offsetMessage}, partition: {partition} from {topicName}", message.Offset.Value, message.Partition.Value, message.Topic);
            }
        }
    }, stoppingToken, TaskCreationOptions.DenyChildAttach | TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();

    private IEnumerable<ConsumeResult<byte[], byte[]>> GetMessages(CancellationToken token)
    {
        _consumer.Subscribe(_topicName);

        while (!token.IsCancellationRequested)
        {
            var cr = _consumer.Consume(token);
            yield return cr;
            _consumer.StoreOffset(cr);
        }

        _consumer.Unsubscribe();
    }
}
