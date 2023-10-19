using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Pipeline.Kafka.Extensions;

namespace Pipeline.Kafka.Dispatcher;

internal class KafkaMessageDispatcher
{
    private readonly IOptionsSnapshot<KafkaMessageDispatcherOptions> _kafkaMessageDispatcherOptions;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<KafkaMessageDispatcher> _logger;

    public KafkaMessageDispatcher(IOptionsSnapshot<KafkaMessageDispatcherOptions> options, IServiceProvider serviceProvider, ILogger<KafkaMessageDispatcher> logger)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
        _kafkaMessageDispatcherOptions = options;
    }

    public Task DispatchAsync(ConsumeResult<byte[], byte[]> consumeResult, CancellationToken cancellationToken)
    {
        if (_kafkaMessageDispatcherOptions.Get(consumeResult.Topic).TryGetMessagePipeline(consumeResult, out var pipeline, out _))
        {
            return pipeline(_serviceProvider, consumeResult, cancellationToken);
        }

        _logger.LogTrace("No pipeline is registered for message at {topicName} with offset: {offsetMessage} and partition: {partition}",
            consumeResult.Topic, consumeResult.Offset.Value, consumeResult.Partition.Value);

        return Task.CompletedTask;
    }

    public async Task DispatchAsync(IBatch<ConsumeResult<byte[], byte[]>> consumeResults, CancellationToken cancellationToken)
    {
        Func<IServiceProvider, IBatch<ConsumeResult<byte[], byte[]>>, CancellationToken, Task>? pipeline = null;
        var groups = consumeResults
            .Where(cr =>
            {
                if (_kafkaMessageDispatcherOptions.Get(cr.Topic).TryGetMessagePipeline(cr, out _, out pipeline))
                {
                    return true;
                }

                _logger.LogTrace("No pipeline is registered for message at {topicName} with offset: {offsetMessage} and partition: {partition}",
                    cr.Topic, cr.Offset.Value, cr.Partition.Value);

                return false;
            })
            .GroupBy(_ => pipeline!);

        foreach (var group in groups)
        {
            await group.Key(_serviceProvider, group.ToBatch(), cancellationToken);
        }
    }
}
