using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using Pipeline.Kafka.Config;
using Pipeline.Kafka.Dispatcher;
using Pipeline.Kafka.Extensions;
using static Pipeline.Kafka.Dispatcher.DispatcherStrategy;

namespace Pipeline.Kafka.Tests;

[TestFixture]
public class KafkaIntegrationTests
{
#pragma warning disable CS8618
    private ServiceProvider _serviceProvider;
#pragma warning restore CS8618

    [SetUp]
    public void Setup()
    {
        var section = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:Topics:TestTopic:TopicName"] = "TradingCore_Markets-prod",
                ["Kafka:Topics:TestTopic:BootstrapServers"] = "kafka.betlab.private:9092",
                ["Kafka:Topics:TestTopic:AutoOffsetReset"] = "Earliest",
                ["Kafka:Topics:TestTopic:MaxBatchSize"] = "100",
                ["Kafka:Topics:TestTopic:GroupId"] = "test-group-id-2",
                ["Kafka:Topics:TestPublishTopic:TopicName"] = "Test-prod",
                ["Kafka:Topics:TestPublishTopic:BootstrapServers"] = "localhost:9092",
                ["Kafka:Topics:DeadLetterTopic:TopicName"] = "Dead-prod",
                ["Kafka:Topics:DeadLetterTopic:BootstrapServers"] = "localhost:9092",
            })
            .Build()
            .GetRequiredSection("Kafka");

        var serviceCollection = new ServiceCollection();

        serviceCollection
            .AddTransient(typeof(ILogger<>), typeof(NullLogger<>))
            .AddSingleton<IBatchMessageHandler<Ignore, object>, BatchTestHandler>()
            .AddKafkaMessagingFor<KafkaTopics2>(section)
            .ConfigureBatchPipelineFrom(x => x.TestTopic, x => x
                .To<Ignore, object>(TypeOf("MarketHolder").And(Version("3", "structureVersion")))
                .ToTombstone<Ignore>(TypeOf("MarketHolder").And(Version("3", "structureVersion")), x => x.Using<CleanupHandler>()))
            .ConfigurePublisherTo<string>(x => x.TestPublishTopic, x => x.With(m => new { Name = m, Super = "yes" }))
            .ConfigurePublisherTo<string>(x => x.DeadLetterTopic, x => x.IgnoreKey())
            .ConfigurePublisherTo<object>(x => x.DeadLetterTopic, x => x.IgnoreKey());

        _serviceProvider = serviceCollection.BuildServiceProvider();
    }

    [Test]
    [Ignore("for local run")]
    public async Task TestBatchReading()
    {
        //await _serviceProvider.GetRequiredService<IHostedService>().StartAsync(CancellationToken.None);
        //await Task.Delay(100000);

        await _serviceProvider.GetRequiredService<IMessagePublisher>().PublishAsync<Ignore, object>(null!, "some string", configure: null, CancellationToken.None);
        await _serviceProvider.GetRequiredService<IMessagePublisher>().PublishAsync("some string", CancellationToken.None);
    }
}

public class KafkaTopics2
{
    public required KafkaConsumerOptions TestTopic { get; set; }

    public required KafkaProducerOptions TestPublishTopic { get; set; }

    public required KafkaProducerOptions DeadLetterTopic { get; set; }
}

public class BatchTestHandler : IBatchMessageHandler<Ignore, object>
{
    public Task Handle(IBatch<IKafkaMessage<Ignore, object>> messages, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Batch handler: {messages.Count}");
        return Task.CompletedTask;
    }
}

public class CleanupHandler : IBatchMessageHandler<Ignore, Null>
{
    public Task Handle(IBatch<IKafkaMessage<Ignore, Null>> messages, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Cleanup handler: {messages.Count}");
        return Task.CompletedTask;
    }
}
