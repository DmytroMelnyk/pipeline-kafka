using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.CompilerServices;
using Confluent.Kafka;

namespace Pipeline.Kafka.Config;

public class KafkaOptionsBase
{
    protected KafkaOptionsBase()
    {
    }

    protected static readonly string ApplicationName = Assembly.GetEntryAssembly()!.GetName().Name!;
}

public class KafkaOptions<T> : KafkaOptionsBase
{
    private readonly HashSet<ClientConfig> _initialized = new();

    public required T Topics
    {
        private get; init;
    }

    public KafkaProducerOptions GetConfigFor(Func<T, KafkaProducerOptions> configGetter, [CallerArgumentExpression(nameof(configGetter))] string configGetterArg = "")
    {
        var config = configGetter(Topics);
        if (config is null)
        {
            KafkaOptions<T>.ThrowMisconfiguration(configGetter, "was not configured.");
        }

        Initialize(config);
        return config;
    }

    public KafkaConsumerOptions GetConfigFor(Func<T, KafkaConsumerOptions> configGetter, [CallerArgumentExpression(nameof(configGetter))] string configGetterArg = "")
    {
        var config = configGetter(Topics);
        if (config is null)
        {
            KafkaOptions<T>.ThrowMisconfiguration(configGetter, "was not configured.");
        }

        Initialize(config);
        return config;
    }

    [DoesNotReturn]
    internal static void ThrowMisconfiguration<TConfig>(Func<T, TConfig> configGetter, string description, [CallerArgumentExpression(nameof(configGetter))] string configGetterArg = "") => throw new InvalidOperationException($"Config section: {configGetterArg}: {description}.");

    private void Initialize(KafkaProducerOptions config)
    {
        if (!TryInitializeBase(config))
        {
            return;
        }

        // https://aivarsk.com/2021/11/01/low-latency-kafka-producers/
        config.SocketNagleDisable = true;
        config.LingerMs = 0;
    }

    private void Initialize(KafkaConsumerOptions config)
    {
        if (!TryInitializeBase(config))
        {
            return;
        }

        // auto commit
        config.EnableAutoCommit = true;
        config.AutoCommitIntervalMs = 500;
        config.EnableAutoOffsetStore = false;

        // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
        // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
        config.PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky;

        if (config.AddMachineName)
        {
            config.GroupId += $"-{Environment.MachineName}";
        }
    }

    private bool TryInitializeBase<TConfig>(TConfig config)
        where TConfig : ClientConfig
    {
        if (!_initialized.Add(config))
        {
            return false;
        }

        // https://kafka.js.org/docs/configuration#client-id
        config.ClientId = ApplicationName;
        return true;
    }
}
