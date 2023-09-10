using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using static Pipeline.Kafka.Dispatcher.DispatcherStrategy;
using Pipeline.Kafka.Extensions;
using Pipeline.Kafka.Config;
using Pipeline.Kafka.Client;
using Pipeline.Kafka.Dispatcher;

namespace Pipeline.Kafka.Tests;

[TestFixture]
public class KafkaPipelineTests
{
#pragma warning disable CS8618
    private ServiceProvider _serviceProvider;
    private Mock<IProducerFactory> _producerFactoryMock;
    private Mock<IProducer<byte[], byte[]>> _deadLetterTopicProducerMock;
    private Mock<IConsumer<byte[], byte[]>> _personTopicConsumerMock;
    private Mock<IConsumerFactory> _consumerFactoryMock;
    private Mock<IMessageHandler<Ignore, PersonV2>> _personMessageHandlerMock;
    private IConfigurationSection _section;
#pragma warning restore CS8618

    [SetUp]
    public void Setup()
    {
        _section = new ConfigurationBuilder()
            .AddJsonFile("appsettings.test.json", false)
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:Topics:Deadletter:TopicName"] = "deadletter"
            })
            .Build()
            .GetRequiredSection("Kafka");

        var serviceCollection = new ServiceCollection();

        _deadLetterTopicProducerMock = new Mock<IProducer<byte[], byte[]>>();
        _producerFactoryMock = new Mock<IProducerFactory>();
        _producerFactoryMock
            .Setup(x => x.GetProducerLazy(It.IsAny<KafkaProducerOptions>()))
            .Returns(new Lazy<IProducer<byte[], byte[]>>(_deadLetterTopicProducerMock.Object));

        _personTopicConsumerMock = new Mock<IConsumer<byte[], byte[]>>();
        _consumerFactoryMock = new Mock<IConsumerFactory>();
        _consumerFactoryMock
            .Setup(x => x.CreateConsumer(It.IsAny<KafkaConsumerOptions>()))
            .Returns(_personTopicConsumerMock.Object);

        _personMessageHandlerMock = new Mock<IMessageHandler<Ignore, PersonV2>>();

        serviceCollection
            .AddTransient(typeof(ILogger<>), typeof(NullLogger<>))
            .AddSingleton(_producerFactoryMock.Object)
            .AddSingleton(_deadLetterTopicProducerMock.Object)
            .AddSingleton(_consumerFactoryMock.Object)
            .AddSingleton(_personMessageHandlerMock.Object)
            .AddSingleton<IPipelineLink<Ignore, PersonV2>>(sp =>
            {
                var _deadLetterLink = new Mock<DeadLetterValidationLink<PersonV2>>(sp.GetRequiredService<IMessagePublisher>());
                _deadLetterLink.CallBase = true;
                _deadLetterLink
                    .SetupSequence(x => x.IsValid)
                    .Returns(true)
                    .Returns(false);

                return _deadLetterLink.Object;
            })
            .AddKafkaMessagingFor<KafkaTopicsDeadLetter>(_section)
            .ConfigurePipelineFrom(x => x.Persons, x => x.To<PersonV2>(TypeOf("Person").And(Version("2"))))
            .ConfigurePublisherTo<object>(x => x.Deadletter, x => x.IgnoreKey());

        _serviceProvider = serviceCollection.BuildServiceProvider();
    }

    [Test]
    public async Task DeadLetterValidationLink_WhenMessageIsNotValid_ExpectedProduceToDeadLetterTopic()
    {
        _personTopicConsumerMock
            .SetupSequence(x => x.Consume(It.IsAny<CancellationToken>()))
            .Returns(() => Convert(_serviceProvider, "Person-stage", default(Ignore)!, new PersonV2 { Id = 2 }, x => x.Add("type", "Person").Add("version", "2")))
            .Returns(() => Convert(_serviceProvider, "Person-stage", default(Ignore)!, new PersonV2 { Id = 3 }, x => x.Add("type", "Person").Add("version", "2")))
            .Returns(() =>
            {
                Thread.Sleep(10000);
                return null!;
            });

        var deadLetters = new List<Headers>();
        _deadLetterTopicProducerMock.Setup(x => x.ProduceAsync("deadletter", It.IsAny<Message<byte[], byte[]>>(), It.IsAny<CancellationToken>()))
            .Callback<string, Message<byte[], byte[]>, CancellationToken>((_, x, _) => deadLetters.Add(x.Headers));

        var persons = new List<PersonV2>();
        _personMessageHandlerMock.Setup(x => x.Handle(It.IsAny<IKafkaMessage<Ignore, PersonV2>>(), It.IsAny<CancellationToken>()))
            .Callback<IKafkaMessage<Ignore, PersonV2>, CancellationToken>((msg, _) => persons.Add(msg.Value));

        var kafkaMessagePublishers = _serviceProvider.GetServices<IHostedService>();
        foreach (var kafkaMessagePublisher in kafkaMessagePublishers)
        {
            await kafkaMessagePublisher.StartAsync(CancellationToken.None);
        }

        Assert.That(() => persons.FirstOrDefault()?.Id, Is.EqualTo(2).After(1000, 10));
        foreach (var kafkaMessagePublisher in kafkaMessagePublishers)
        {
            await kafkaMessagePublisher.StopAsync(CancellationToken.None);
        }

        Assert.That(deadLetters, Has.Count.EqualTo(1));
        Assert.That(deadLetters.First().TryGetValue("dead-letter-topic-partition-offset", out var headerValue), Is.True);
        Assert.That(headerValue, Contains.Substring("Person-stage"));
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

public abstract class DeadLetterValidationLink<TValue> : IPipelineLink<Ignore, TValue>
{
    private readonly IMessagePublisher _messagePublisher;

    public DeadLetterValidationLink(IMessagePublisher messagePublisher)
    {
        _messagePublisher = messagePublisher;
    }

    public abstract bool IsValid { get; }

    public Task RunAsync(IKafkaConsumeResult<Ignore, TValue> message, Func<Task> next, CancellationToken cancellationToken)
    {
        if (IsValid)
        {
            return next();
        }

        return _messagePublisher.PublishAsync<Ignore, object>(message.Key, message.Value!, message.Headers.Add("dead-letter-topic-partition-offset", message.TopicPartitionOffset.ToString()), cancellationToken);
    }
}

public class KafkaTopicsDeadLetter
{
    public required KafkaConsumerOptions Persons { get; init; }

    public required KafkaProducerOptions Managers { get; init; }

    public required KafkaProducerOptions Deadletter { get; init; }
}
