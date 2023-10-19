using Microsoft.Extensions.Configuration;
using Pipeline.Kafka.Config;

namespace Pipeline.Kafka.Extensions;

public static class IConfigurationOptionsExtensions
{
    public static KafkaOptions<TKafkaConfig> GetKafkaOptions<TKafkaConfig>(this IConfiguration @this, string? sectionName = null)
    {
        var kafkaSection = string.IsNullOrEmpty(sectionName) ? @this : @this.GetRequiredSection(sectionName);

        var variables = kafkaSection.GetSection("Variables").Get<Dictionary<string, string>>();
        var kafkaSectionList = kafkaSection
            .AsEnumerable(true)
            .Select(x => new KeyValuePair<string, string?>(x.Key, Substitute(variables, x.Value)))
            .ToList();

        var defaultKafkaClientOptions = kafkaSection.GetSection("DefaultKafkaClientOptions").Get<Dictionary<string, string>>();
        var producersAddon = GetDefaultValuesForExistingTopics<TKafkaConfig, KafkaProducerOptions>(defaultKafkaClientOptions);
        kafkaSectionList.AddRange(producersAddon);
        var consumersAddon = GetDefaultValuesForExistingTopics<TKafkaConfig, KafkaConsumerOptions>(defaultKafkaClientOptions);
        kafkaSectionList.AddRange(consumersAddon);

        kafkaSectionList.RemoveAll(x => x.Key.Contains("DefaultKafkaClientOptions") || x.Key.Contains("Variables"));

        var kafkaSectionUpdated = new ConfigurationBuilder()
            .AddInMemoryCollection(kafkaSectionList)
            .Build();

        return kafkaSectionUpdated.Get<KafkaOptions<TKafkaConfig>>(x =>
        {
            x.BindNonPublicProperties = true;
            x.ErrorOnUnknownConfiguration = true;
        })!;
    }

    private static IEnumerable<KeyValuePair<string, string?>> GetDefaultValuesForExistingTopics<TKafkaConfig, TKafkaOptions>(Dictionary<string, string>? defaultKafkaClientOptions)
    {
        if (defaultKafkaClientOptions == null)
        {
            return Enumerable.Empty<KeyValuePair<string, string?>>();
        }

        var defaultKafkaOptions = typeof(TKafkaOptions).GetProperties().Join(defaultKafkaClientOptions, x => x.Name, x => x.Key, (_, x) => x).ToList();
        return typeof(TKafkaConfig)
            .GetProperties()
            .Where(x => x.PropertyType == typeof(TKafkaOptions))
            .Select(x => x.Name)
            .SelectMany(current => defaultKafkaOptions.Select(@default => new KeyValuePair<string, string?>($"{nameof(KafkaOptions<TKafkaConfig>.Topics)}:{current}:{@default.Key}", @default.Value)));
    }

    private static string? Substitute(Dictionary<string, string>? variables, string? value)
    {
        if (value == null)
        {
            return null;
        }

        if (variables == null)
        {
            return value;
        }

        return variables.Aggregate(value, (variable, pair) => variable.Replace($"${{{pair.Key}}}", pair.Value));
    }
}
