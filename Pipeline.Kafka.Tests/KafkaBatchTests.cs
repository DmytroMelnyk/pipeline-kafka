using System.Collections.Generic;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using NUnit.Framework;
using Pipeline.Kafka.Client;
using Pipeline.Kafka.Config;
using Pipeline.Kafka.Dispatcher;
using Pipeline.Kafka.Extensions;
using static Pipeline.Kafka.Dispatcher.DispatcherStrategy;

namespace Pipeline.Kafka.Tests;

[TestFixture]
public class KafkaBatchTests
{
#pragma warning disable CS8618
    private ServiceProvider _serviceProvider;
    private Mock<IProducerFactory> _producerFactoryMock;
    private Mock<IProducer<byte[], byte[]>> _managerTopicProducerMock;
    private Mock<IConsumer<byte[], byte[]>> _personTopicConsumerMock;
    private Mock<IConsumerFactory> _consumerFactoryMock;
    private Mock<IBatchMessageHandler<Ignore, PersonV1>> _personV1MessageHandlerMock;
    private Mock<IBatchMessageHandler<Ignore, object>> _personV2MessageHandlerMock;
    private Mock<IBatchPipelineLink<Ignore, object>> _objectPipelineLink;
#pragma warning restore CS8618

    [SetUp]
    public void Setup()
    {
        var section = new ConfigurationBuilder()
            .AddJsonFile("appsettings.test.json", false)
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:Topics:Persons:PoolSize"] = "1",
                ["Kafka:Topics:Persons:MaxBatchSize"] = "10"
            })
            .Build()
            .GetRequiredSection("Kafka");

        var serviceCollection = new ServiceCollection();


        _managerTopicProducerMock = new Mock<IProducer<byte[], byte[]>>();
        _producerFactoryMock = new Mock<IProducerFactory>();
        _producerFactoryMock
            .Setup(x => x.GetProducerLazy(It.IsAny<KafkaProducerOptions>()))
            .Returns(new Lazy<IProducer<byte[], byte[]>>(_managerTopicProducerMock.Object));

        _personTopicConsumerMock = new Mock<IConsumer<byte[], byte[]>>();
        _consumerFactoryMock = new Mock<IConsumerFactory>();
        _consumerFactoryMock
            .Setup(x => x.CreateConsumer(It.IsAny<KafkaConsumerOptions>()))
            .Returns(_personTopicConsumerMock.Object);

        _personV1MessageHandlerMock = new Mock<IBatchMessageHandler<Ignore, PersonV1>>();
        _personV2MessageHandlerMock = new Mock<IBatchMessageHandler<Ignore, object>>();

        _objectPipelineLink = new Mock<IBatchPipelineLink<Ignore, object>>();
        _objectPipelineLink.Setup(x => x.RunAsync(It.IsAny<IBatch<IKafkaConsumeResult<Ignore, object>>>(), It.IsAny<Func<Task>>(), It.IsAny<CancellationToken>()))
            .Returns<IBatch<IKafkaMessage<Ignore, object>>, Func<Task>, CancellationToken>((_, next, __) => next());

        serviceCollection
            .AddSingleton(_producerFactoryMock.Object)
            .AddSingleton(_consumerFactoryMock.Object)
            .AddTransient(typeof(ILogger<>), typeof(NullLogger<>))
            .AddSingleton(typeof(IBatchPipelineLink<,>), typeof(GenericBatchLink<,>))
            .AddSingleton<IBatchPipelineLink<Ignore, PersonV1>>(_objectPipelineLink.Object)
            .AddSingleton<IBatchPipelineLink<Ignore, PersonV2>>(_objectPipelineLink.Object)
            .AddSingleton(_personV1MessageHandlerMock.Object)
            .AddSingleton<IBatchMessageHandler<Ignore, PersonV2>>(_personV2MessageHandlerMock.Object)
            .AddKafkaMessagingFor<KafkaTopics>(section)
            .ConfigureBatchPipelineFrom(x => x.Persons, x => x
                .To<Ignore, PersonV1>(TypeOf("Person").And(Version("1")))
                .To<PersonV2>(TypeOf("Person").And(Version("2"))));

        _serviceProvider = serviceCollection.BuildServiceProvider();
    }

    [Test]
    public void WhenKafkaWasRegistered_ExpectedAllOk()
    {
        var messageProviders = _serviceProvider.GetRequiredService<IEnumerable<IHostedService>>().ToList();
        Assert.That(messageProviders, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task IMessageHandler_WhenMessagePublished_ExpectedMessageWasRead()
    {
        _personTopicConsumerMock
            .SetupSequence(x => x.Consume(It.IsAny<CancellationToken>()))
            .Returns(() => Convert(_serviceProvider, default(Ignore)!, new PersonV1 { Id = 10 }, x => x.Add("type", "Person").Add("version", "1")))
            .Returns(() => new ConsumeResult<byte[], byte[]>());

        _personTopicConsumerMock
            .SetupSequence(x => x.Consume(It.IsAny<TimeSpan>()))
            .Returns(Convert(_serviceProvider, default(Ignore)!, new PersonV1 { Id = 11 }, x => x.Add("type", "Person").Add("version", "1")))
            .Returns(Convert(_serviceProvider, default(Ignore)!, new PersonV1 { Id = 12 }, x => x.Add("type", "Person").Add("version", "1")))
            .Returns(Convert(_serviceProvider, default(Ignore)!, new PersonV1 { Id = 13 }, x => x.Add("type", "Person").Add("version", "1")))
            .Returns(Convert(_serviceProvider, default(Ignore)!, new PersonV2 { Id = 20 }, x => x.Add("type", "Person").Add("version", "2")))
            .Returns(Convert(_serviceProvider, default(Ignore)!, new PersonV2 { Id = 21 }, x => x.Add("type", "Person").Add("version", "2")))
            .Returns(Convert(_serviceProvider, default(Ignore)!, new PersonV2 { Id = 22 }, x => x.Add("type", "Person").Add("version", "2")))
            .Returns(() => new ConsumeResult<byte[], byte[]>());

        var kafkaMessagePublishers = _serviceProvider.GetServices<IHostedService>();
        foreach (var kafkaMessagePublisher in kafkaMessagePublishers)
        {
            await kafkaMessagePublisher.StartAsync(CancellationToken.None);
        }

        Assert.That(() => _personV1MessageHandlerMock.Verify(x => x.Handle(It.IsAny<IBatch<IKafkaMessage<Ignore, PersonV1>>>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce()), Throws.Nothing.After(1000, 10));
        Assert.That(() => _personV2MessageHandlerMock.Verify(x => x.Handle(It.IsAny<IBatch<IKafkaMessage<Ignore, PersonV2>>>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce()), Throws.Nothing.After(1000, 10));

        foreach (var kafkaMessagePublisher in kafkaMessagePublishers)
        {
            await kafkaMessagePublisher.StopAsync(CancellationToken.None);
        }

        var p1Link = (GenericBatchLink<Ignore, PersonV1>)_serviceProvider
            .GetServices<IBatchPipelineLink<Ignore, PersonV1>>()
            .First(x => x is GenericBatchLink<Ignore, PersonV1>);

        var p2Link = (GenericBatchLink<Ignore, PersonV2>)_serviceProvider
            .GetServices<IBatchPipelineLink<Ignore, PersonV2>>()
            .First(x => x is GenericBatchLink<Ignore, PersonV2>);

        Assert.That(p1Link.Counter, Is.EqualTo(1));
        Assert.That(p2Link.Counter, Is.EqualTo(1));
        _personV1MessageHandlerMock.Verify(x => x.Handle(It.Is<IBatch<IKafkaMessage<Ignore, PersonV1>>>(m => m.Count == 4), It.IsAny<CancellationToken>()));
        _personV2MessageHandlerMock.Verify(x => x.Handle(It.Is<IBatch<IKafkaMessage<Ignore, object>>>(m => m.Count == 3), It.IsAny<CancellationToken>()));
        _personTopicConsumerMock.Verify(x => x.Subscribe("Person-stage"), Times.Exactly(1));
        _objectPipelineLink.Verify(x => x.RunAsync(It.IsAny<IBatch<IKafkaConsumeResult<Ignore, object>>>(), It.IsAny<Func<Task>>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    private static ConsumeResult<byte[], byte[]> Convert<TKey, TValue>(IServiceProvider sp, TKey key, TValue value, Action<Headers>? configure = null, string topicName = "Person-stage")
    {
        var keySerializer = sp.GetRequiredService<ISerializer<TKey>>();
        var valueSerializer = sp.GetRequiredService<ISerializer<TValue>>();
        var headers = new Headers();
        configure?.Invoke(headers);

        return new ConsumeResult<byte[], byte[]>
        {
            Topic = topicName,
            Message = new Message<byte[], byte[]>
            {
                Key = keySerializer.Serialize(key, SerializationContext.Empty),
                Value = valueSerializer.Serialize(value, SerializationContext.Empty),
                Headers = headers,
                Timestamp = Timestamp.Default
            }
        };
    }
}

public class BatchObsoleteNotificator : IBatchPipelineLink<Ignore, object>
{
    public Task RunAsync(IBatch<IKafkaConsumeResult<Ignore, object>> messages, Func<Task> next, CancellationToken cancellationToken)
    {
        Console.WriteLine("Obsolete message");
        return next();
    }
}

public class GenericBatchLink<TKey, TValue> : IBatchPipelineLink<TKey, TValue>
{
    public int Counter = 0;

    public Task RunAsync(IBatch<IKafkaConsumeResult<TKey, TValue>> messages, Func<Task> next, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref Counter);
        return next();
    }
}
