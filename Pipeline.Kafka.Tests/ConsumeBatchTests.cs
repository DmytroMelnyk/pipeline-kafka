using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Pipeline.Kafka.Extensions;

namespace Pipeline.Kafka.Tests;

[TestFixture]
class ConsumeBatchTests
{
    [TestCase(1, 0)]
    [TestCase(2, 1)]
    [TestCase(3, 1)]
    public void ConsumeBatch_WhenProvidedBatchSize_ExpectedCallsToConsumeTimeSpanOverload(int batchSize, int callCount)
    {
        var consumerMock = new Mock<IConsumer<string, string>>();
        consumerMock.Setup(x => x.Consume(It.IsAny<CancellationToken>()))
            .Returns(new ConsumeResult<string, string>()
            {
                Message = new Message<string, string>()
            });

        var batch = consumerMock.Object.ConsumeBatch(batchSize, CancellationToken.None);
        consumerMock.Verify(x => x.Consume(It.IsAny<TimeSpan>()), Times.Exactly(callCount));
        Assert.That(batch, Has.Count.EqualTo(1));
    }

    [TestCase(1)]
    public void ConsumeBatch_WhenConsumed_ExpectedBatchIsReadOnly(int batchSize)
    {
        var consumerMock = new Mock<IConsumer<string, string>>();
        consumerMock.Setup(x => x.Consume(It.IsAny<CancellationToken>()))
            .Returns(new ConsumeResult<string, string>()
            {
                Message = new Message<string, string>()
            });

        var batch = consumerMock.Object.ConsumeBatch(batchSize, CancellationToken.None);

        Assert.That(batch.IsReadOnly, Is.True);
        Assert.That(() => batch.RemoveAt(0), Throws.Exception.InstanceOf<NotSupportedException>());
    }
}
