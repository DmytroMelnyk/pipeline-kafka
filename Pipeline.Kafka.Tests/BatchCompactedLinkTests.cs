
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using Pipeline.Kafka.Extensions;
using Pipeline.Kafka.PipelineLinks;
using static Pipeline.Kafka.Extensions.MessageFactory;

namespace Pipeline.Kafka.Tests;

[TestFixture]
internal sealed class BatchCompactedLinkTests
{
    [Test]
    public async Task RunAsync_WhenDuplicatesInBatch_ExpectedCompactionByKey()
    {
        var batch = new[]
        {
        CreateKafkaConsumeResult(1, "str1"),
        CreateKafkaConsumeResult(2, "str1"),
        CreateKafkaConsumeResult(3, "str3"),
        CreateKafkaConsumeResult(4, "str1"),
        CreateKafkaConsumeResult(5, "str2"),
        CreateKafkaConsumeResult(6, "str3"),
        CreateKafkaConsumeResult(7, "str1"),
        CreateKafkaConsumeResult(8, "str2"),
        CreateKafkaConsumeResult(9, "str1"),
        CreateKafkaConsumeResult(10, "str2"),
        CreateKafkaConsumeResult(11, "str1"),
        CreateKafkaConsumeResult(12, "str1"),
        CreateKafkaConsumeResult(13, "str1"),
    }.ToBatch();

        await new BatchCompactedLink<int, string, string>(NullLogger<IBatchPipelineLink<int, string>>.Instance, x => x.Value)
            .RunAsync(batch, () => Task.CompletedTask, CancellationToken.None);

        Assert.That(batch, Has.Count.EqualTo(3));
        Assert.That(batch.Select(x => x.Key), Is.EquivalentTo(new[] { 6, 10, 13 }));
        Assert.That(batch.Select(x => x.Value), Is.EquivalentTo(new[] { "str3", "str2", "str1" }));
    }
}
