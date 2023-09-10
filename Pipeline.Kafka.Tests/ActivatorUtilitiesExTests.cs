using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Pipeline.Kafka.Extensions;
using Pipeline.Kafka.Pipeline;

namespace Pipeline.Kafka.Tests;

[TestFixture]
class ActivatorUtilitiesExTests
{
    [Test]
    public void CreateFactory_WhenEveryServiceIsRegistered_ExpectedPipelineCreated()
    {
        var sp = new ServiceCollection()
            .AddSingleton<MyClass>()
            .AddSingleton(Deserializers.Utf8)
            .AddSingleton(Deserializers.Ignore)
            .BuildServiceProvider();

        var factory = ActivatorUtilitiesEx.CreateFactory<IEnumerable<IMessageHandler<Ignore, string>>, DispatcherPipeline<Ignore, string>>();
        var providerPipeline = factory(sp, new IMessageHandler<Ignore, string>[] { sp.GetRequiredService<MyClass>() });
        Assert.That(providerPipeline, Is.Not.Null);
    }

    public class MyClass : IMessageHandler<Ignore, string>
    {
        public Task Handle(IKafkaMessage<Ignore, string> message, CancellationToken cancellationToken) => throw new NotImplementedException();
    }
}
