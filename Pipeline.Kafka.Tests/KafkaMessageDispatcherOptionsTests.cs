using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using NUnit.Framework;
using Pipeline.Kafka.Dispatcher;
using Pipeline.Kafka.Extensions;
using static Pipeline.Kafka.Dispatcher.DispatcherStrategy;

namespace Pipeline.Kafka.Tests;

[TestFixture]
public class KafkaMessageDispatcherOptionsTests
{
    [Test]
    public void KafkaMessageDispatcherOptions_WhenPipelineWasRegistered_ExpectedToGet()
    {
        var dispatcherOptions = new KafkaMessageDispatcherOptions();
        dispatcherOptions.RegisterMessagePipelineFor<Ignore, PersonV2>(Version("2").And(TypeOf("Person")), Array.Empty<Type>());
        var hasPipelineRegistered = dispatcherOptions
            .TryGetMessagePipeline(new ConsumeResult<byte[], byte[]>()
            {
                Topic = "Persons",
                Message = new Message<byte[], byte[]>
                {
                    Headers = new Headers().Add("version", "2").Add("type", "Person")
                }
            }, out _, out _);

        Assert.That(hasPipelineRegistered, Is.True);
    }
}
