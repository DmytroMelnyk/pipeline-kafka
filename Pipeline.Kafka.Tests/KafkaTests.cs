using System.Text;
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
public class KafkaTests
{
#pragma warning disable CS8618
    private ServiceProvider _serviceProvider;
    private Mock<IProducerFactory> _producerFactoryMock;
    private Mock<IProducer<byte[], byte[]>> _managerTopicProducerMock;
    private Mock<IConsumer<byte[], byte[]>> _personTopicConsumerMock;
    private Mock<IConsumerFactory> _consumerFactoryMock;
    private Mock<IMessageHandler<Ignore, PersonV1>> _personMessageHandlerMock;
    private Mock<IPipelineLink<Ignore, object>> _objectPipelineLink;
    private IConfigurationSection _section;
#pragma warning restore CS8618

    [SetUp]
    public void Setup()
    {
        _section = new ConfigurationBuilder()
            .AddJsonFile("appsettings.test.json", false)
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:Topics:Persons:PoolSize"] = "5"
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

        _personMessageHandlerMock = new Mock<IMessageHandler<Ignore, PersonV1>>();
        _objectPipelineLink = new Mock<IPipelineLink<Ignore, object>>();
        _objectPipelineLink.Setup(x => x.RunAsync(It.IsAny<IKafkaConsumeResult<Ignore, PersonV1>>(), It.IsAny<Func<Task>>(), It.IsAny<CancellationToken>()))
            .Returns<IKafkaMessage<Ignore, PersonV1>, Func<Task>, CancellationToken>((_, next, __) => next());

        serviceCollection
            .AddSingleton(_producerFactoryMock.Object)
            .AddSingleton(_consumerFactoryMock.Object)
            .AddTransient(typeof(ILogger<>), typeof(NullLogger<>))
            .AddSingleton(typeof(IPipelineLink<,>), typeof(GenericLink<,>))
            .AddTransient<IPipelineLink<Ignore, PersonV1>, ObsoleteNotificator>()
            .AddSingleton<IPipelineLink<Ignore, PersonV1>>(_objectPipelineLink.Object)
            .AddSingleton(_personMessageHandlerMock.Object)
            .AddScoped<IMessageHandler<Ignore, PersonV2>, PersonHandler>()
            .AddKafkaMessagingFor<KafkaTopics>(_section)
            .ConfigurePipelineFrom(x => x.Persons, x => x
                .To<Ignore, PersonV1>(TypeOf("Person").And(Version("1")))
                .To<PersonV2>(TypeOf("Person").And(Version("2"))))
            .ConfigurePublisherTo<ManagerV2>(x => x.Managers, x => x.With(m => new { m.Id, Type = nameof(ManagerV1) }, (_, h) => h.Add("type", "V2")))
            .ConfigurePublisherTo<ManagerV1>(x => x.Managers, x => x.IgnoreKey((_, h) => h.Add("type", "V1")));

        _serviceProvider = serviceCollection.BuildServiceProvider();
    }

    [Test]
    public void WhenKafkaWasRegisteredExpectedAllOk()
    {
        var messageProviders = _serviceProvider.GetRequiredService<IEnumerable<IHostedService>>().ToList();
        Assert.That(messageProviders, Has.Count.EqualTo(5));
    }

    [Test]
    public async Task IMessagePublisherWhenPublishingToTheSameTopicDifferentEntitiesExpectedAllPublished()
    {
        var messagePublisher = _serviceProvider.GetRequiredService<IMessagePublisher>();
        await messagePublisher.PublishAsync(new ManagerV1 { Id = 1 }, CancellationToken.None);
        await messagePublisher.PublishAsync(new ManagerV2 { Id = 1 }, CancellationToken.None);

        _managerTopicProducerMock.Verify(x => x.ProduceAsync(
            "Manager-stage",
            It.Is<Message<byte[], byte[]>>(m => Encoding.UTF8.GetString(m.Headers.Single().GetValueBytes()) == "V1"),
            CancellationToken.None), Times.Once);

        _managerTopicProducerMock.Verify(x => x.ProduceAsync(
            "Manager-stage",
            It.Is<Message<byte[], byte[]>>(m => Encoding.UTF8.GetString(m.Headers.Single().GetValueBytes()) == "V2"),
            CancellationToken.None), Times.Once);
    }

    [Test]
    public async Task IMessageHandlerWhenMessagePublishedExpectedMessageWasRead()
    {
        _personTopicConsumerMock
            .Setup(x => x.Consume(It.IsAny<CancellationToken>()))
            .Returns(() => Convert(_serviceProvider, "Person-stage", default(Ignore)!, new PersonV1 { Id = 2 }, x => x.Add("type", "Person").Add("version", "1")));

        var kafkaMessagePublishers = _serviceProvider.GetServices<IHostedService>();
        foreach (var kafkaMessagePublisher in kafkaMessagePublishers)
        {
            await kafkaMessagePublisher.StartAsync(CancellationToken.None);
        }

        Assert.That(() => _personMessageHandlerMock.Verify(x => x.Handle(It.Is<IKafkaMessage<Ignore, PersonV1>>(m => m.Value.Id == 2), It.IsAny<CancellationToken>()), Times.AtLeastOnce()), Throws.Nothing.After(1000, 10));

        foreach (var kafkaMessagePublisher in kafkaMessagePublishers)
        {
            await kafkaMessagePublisher.StopAsync(CancellationToken.None);
        }
        var link = (GenericLink<Ignore, PersonV1>) _serviceProvider
            .GetServices<IPipelineLink<Ignore, PersonV1>>()
            .First(x => x is GenericLink<Ignore, PersonV1>);

        _personTopicConsumerMock.Verify(x => x.Subscribe("Person-stage"), Times.Exactly(5));
        _objectPipelineLink.Verify(x => x.RunAsync(It.IsAny<IKafkaConsumeResult<Ignore, PersonV1>>(), It.IsAny<Func<Task>>(), It.IsAny<CancellationToken>()), Times.Exactly(link.Counter));
    }

    private static ConsumeResult<byte[], byte[]> Convert<TKey, TValue>(IServiceProvider sp, string topicName, TKey key, TValue value, Action<Headers>? configure = null)
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

public class KafkaTopics
{
    public required KafkaConsumerOptions Persons
    {
        get; init;
    }

    public required KafkaProducerOptions Managers
    {
        get; init;
    }
}

public class ObsoleteNotificator : IPipelineLink<Ignore, object>
{
    public Task RunAsync(IKafkaConsumeResult<Ignore, object> message, Func<Task> next, CancellationToken cancellationToken)
    {
        Console.WriteLine("Obsolete message");
        return next();
    }
}

public class GenericLink<TKey, TValue> : IPipelineLink<TKey, TValue>
{
    public int Counter;

    public Task RunAsync(IKafkaConsumeResult<TKey, TValue> message, Func<Task> next, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref Counter);
        return next();
    }
}

public class PersonHandler : IMessageHandler<Ignore, PersonV1>, IMessageHandler<Ignore, PersonV2>
{
    public Task Handle(IKafkaMessage<Ignore, PersonV1> message, CancellationToken cancellationToken)
    {
        Console.WriteLine("PersonV1");
        return Task.CompletedTask;
    }

    public Task Handle(IKafkaMessage<Ignore, PersonV2> message, CancellationToken cancellationToken) => throw new NotImplementedException();
}

public class PersonHandler2 : IMessageHandler<Ignore, PersonV1>
{
    public Task Handle(IKafkaMessage<Ignore, PersonV1> message, CancellationToken cancellationToken)
    {
        Console.WriteLine("PersonV1");
        return Task.CompletedTask;
    }
}

public class PersonV1
{
    public required int Id
    {
        get; set;
    }

    public string? Name
    {
        get; set;
    }
}

public class ManagerV2 : PersonV2
{
    public string? Title
    {
        get; set;
    }
}

public class ManagerV1 : PersonV1
{
    public string? Title
    {
        get; set;
    }
}

public class PersonV2
{
    public required int Id
    {
        get; set;
    }

    public string? FirstName
    {
        get; set;
    }

    public string? LastName
    {
        get; set;
    }
}
