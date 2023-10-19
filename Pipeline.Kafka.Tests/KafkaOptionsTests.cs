using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Configuration;
using NUnit.Framework;
using Pipeline.Kafka.Config;
using Pipeline.Kafka.Extensions;

namespace Pipeline.Kafka.Tests;

[TestFixture]
internal sealed class KafkaOptionsTests
{
    [Test]
    public void GetConfigFor_WhenNotIdempotentOperations_ExpectedConfigsAreEqual()
    {
        var config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.test.json", false)
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:Topics:Persons:AddMachineName"] = "true"
            })
            .Build();

        var kafkaConfig = config.GetKafkaOptions<KafkaTopics>("Kafka");
        var personsTopicConfig = kafkaConfig!.GetConfigFor(x => x.Persons);
        Assert.That(personsTopicConfig.GroupId, Is.Not.EqualTo("GroupId").And.StartsWith("GroupId"));
        Assert.That(personsTopicConfig, Is.EqualTo(kafkaConfig.GetConfigFor(x => x.Persons)));
    }

    [Test]
    public void GetConfigFor_WhenVariablesDefined_ExpectedVariablesSubstituted()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:Variables:env"] = "prod",
                ["Kafka:Topics:ReadTopic:AddMachineName"] = "true",
                ["Kafka:Topics:WriteTopic:TopicName"] = "Trading_Core-${env}"
            })
            .Build();

        var kafkaConfig = config.GetKafkaOptions<KafkaDefaults>("Kafka");
        var writeTopicConfig = kafkaConfig!.GetConfigFor(x => x.WriteTopic);
        Assert.That(writeTopicConfig.TopicName, Is.EqualTo("Trading_Core-prod"));
    }

    [Test]
    public void GetConfigFor_WhenDefaultKafkaClientOptionsDefined_ExpectedOptionsSubstituted()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:DefaultKafkaClientOptions:BootstrapServers"] = "localhost:9092",
                ["Kafka:DefaultKafkaClientOptions:EnableDeliveryReports"] = "true",
                ["Kafka:Topics:ReadTopic:AddMachineName"] = "true",
            })
            .Build();

        var kafkaConfig = config.GetKafkaOptions<KafkaDefaults>("Kafka");
        var readTopicConfig = kafkaConfig!.GetConfigFor(x => x.ReadTopic);
        var writeTopicConfig = kafkaConfig!.GetConfigFor(x => x.WriteTopic);
        Assert.That(writeTopicConfig.EnableDeliveryReports, Is.True);
        Assert.That(writeTopicConfig.BootstrapServers, Is.EqualTo("localhost:9092"));
        Assert.That(readTopicConfig.BootstrapServers, Is.EqualTo("localhost:9092"));
    }

    [SuppressMessage("Minor Code Smell", "S3459:Unassigned members should be removed", Justification = "<Pending>")]
    [SuppressMessage("Major Code Smell", "S1144:Unused private types or members should be removed", Justification = "<Pending>")]
    private sealed class KafkaDefaults
    {
        public required KafkaConsumerOptions ReadTopic
        {
            get; set;
        }

        public required KafkaProducerOptions WriteTopic
        {
            get; set;
        }
    }
}
