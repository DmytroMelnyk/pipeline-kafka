using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Pipeline.Kafka.Dispatcher;
using Pipeline.Kafka.Router;
using Pipeline.Kafka.SerDes;

namespace Pipeline.Kafka.Extensions;

public static class ServiceCollectionExtensions
{
    public static KafkaMessagingBuilder<TKafkaConfig> AddKafkaMessagingFor<TKafkaConfig>(this IServiceCollection @this, IConfigurationSection kafkaConfigSection)
    {
        var kafkaConfig = kafkaConfigSection.GetKafkaOptions<TKafkaConfig>();

        @this.TryAddSingleton(Deserializers.Null);
        @this.TryAddSingleton(Deserializers.Utf8);
        @this.TryAddSingleton(Deserializers.Int32);
        @this.TryAddSingleton(Deserializers.Int64);
        @this.TryAddSingleton(Deserializers.Ignore);
        @this.TryAddSingleton(Deserializers.ByteArray);
        @this.TryAddSingleton(typeof(IDeserializer<>), typeof(JsonDeserializer<>));

        @this.TryAddSingleton(Serializers.Null);
        @this.TryAddSingleton(Serializers.Utf8);
        @this.TryAddSingleton(Serializers.Int32);
        @this.TryAddSingleton(Serializers.Int64);
        @this.TryAddSingleton(Serializers.ByteArray);
        @this.TryAddSingleton(typeof(ISerializer<>), typeof(JsonSerializer<>));

        @this.TryAddSingleton<IMessagePublisher, MessagePublisher>();
        return new KafkaMessagingBuilder<TKafkaConfig>(@this, kafkaConfig);
    }
}
