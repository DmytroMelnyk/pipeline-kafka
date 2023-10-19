using Microsoft.Extensions.Logging;

namespace Pipeline.Kafka.PipelineLinks;

public sealed class BatchCompactedLink<TKey, TValue, TCompactionKey> : IBatchPipelineLink<TKey, TValue>
{
    private readonly ILogger<IBatchPipelineLink<TKey, TValue>> _logger;
    private readonly IEqualityComparer<TCompactionKey> _equalityComparer;
    private readonly Func<IKafkaMessage<TKey, TValue>, TCompactionKey> _compactionKeySelector;

    public BatchCompactedLink(ILogger<IBatchPipelineLink<TKey, TValue>> logger,
        Func<IKafkaMessage<TKey, TValue>, TCompactionKey> compactionKeySelector,
        IEqualityComparer<TCompactionKey>? equalityComparer = null)
    {
        _equalityComparer ??= EqualityComparer<TCompactionKey>.Default;
        _logger = logger;
        _compactionKeySelector = compactionKeySelector;
    }

    public Task RunAsync(IBatch<IKafkaConsumeResult<TKey, TValue>> message, Func<Task> next, CancellationToken cancellationToken)
    {
        if (message.IsReadOnly)
        {
            return next();
        }

        var indexesToRemove = message
            .Select((x, idx) => (compactionKey: _compactionKeySelector(x), idx))
            .GroupBy(x => x.compactionKey, x => x.idx, (_, indexes) => indexes.SkipLast(1), _equalityComparer)
            .SelectMany(x => x)
            .OrderDescending()
            .ToList();

        var removedConsumeResults = indexesToRemove
            .Select(idx => message[idx].TopicPartitionOffset)
            .ToList();

        foreach (var idx in indexesToRemove)
        {
            message.RemoveAt(idx);
        }

        _logger.LogInformation("Messages that were compacted: {@compactedTPOs}", removedConsumeResults);

        return next();
    }
}
