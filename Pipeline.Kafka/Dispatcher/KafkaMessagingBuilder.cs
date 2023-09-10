using Pipeline.Kafka.Router;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Pipeline.Kafka.Client;
using Pipeline.Kafka.Config;
using static Pipeline.Kafka.Dispatcher.DispatcherStrategy;

namespace Pipeline.Kafka.Dispatcher;

public class KafkaMessagingBuilder<TKafkaConfig>
{
    private readonly IServiceCollection _serviceCollection;
    private readonly KafkaOptions<TKafkaConfig> _kafkaOptions;

    internal KafkaMessagingBuilder(IServiceCollection serviceCollection, KafkaOptions<TKafkaConfig> kafkaOptions)
    {
        _kafkaOptions = kafkaOptions;
        serviceCollection.TryAddSingleton<KafkaMessageDispatcher>();
        serviceCollection.TryAddSingleton<IProducerFactory, ProducerFactory>();
        serviceCollection.TryAddSingleton<IConsumerFactory, ConsumerFactory>();
        _serviceCollection = serviceCollection;
    }

    public KafkaMessagingBuilder<TKafkaConfig> ConfigurePipelineFrom(Func<TKafkaConfig, KafkaConsumerOptions> topicSelector, Action<ConsumeResultMapping> configureMapping)
    {
        var consumerOptions = _kafkaOptions.GetConfigFor(topicSelector);
        RegisterPool(consumerOptions, sp => ActivatorUtilities
            .CreateInstance<KafkaMessageProvider>(sp, consumerOptions));

        configureMapping(new ConsumeResultMapping(_serviceCollection, consumerOptions.TopicName));
        return this;
    }

    public KafkaMessagingBuilder<TKafkaConfig> ConfigureBatchPipelineFrom(Func<TKafkaConfig, KafkaConsumerOptions> topicSelector, Action<BatchConsumeResultMapping> configureMapping)
    {
        var consumerOptions = _kafkaOptions.GetConfigFor(topicSelector);
        if (!consumerOptions.MaxBatchSize.HasValue)
        {
            _kafkaOptions.ThrowMisconfiguration(topicSelector, $"{nameof(consumerOptions.MaxBatchSize)} must be configured in order to use batching");
        }

        RegisterPool(consumerOptions, sp => ActivatorUtilities
            .CreateInstance<KafkaBatchMessageProvider>(sp, consumerOptions, consumerOptions.MaxBatchSize));

        configureMapping(new BatchConsumeResultMapping(_serviceCollection, consumerOptions.TopicName));
        return this;
    }

    /// <summary>
    /// To publish method after configuring publisher like this you need to use <see cref="IMessagePublisher.PublishAsync{TValue}(TValue, CancellationToken)"/>.
    /// <br>You can also use <see cref="IMessagePublisher.PublishAsync{TKey, TValue}(TKey, TValue, Action{Headers}?, CancellationToken)"/> but with the type of key defined by the <paramref name="configureMapping"/></br>
    /// You can also register:
    /// <br>- <see cref="IMessagePipelineLink{TKey, TValue}"/> as <see cref="IMessagePipelineLink{TKey, TValue}">IMessagePipelineLink</see>&lt;TKey, <typeparamref name="TValue"/>&gt; where TKey defined in <paramref name="configureMapping"/></br>
    /// <br>- <see cref="IPipelineLink{TMessage}">IPipelineLink</see>&lt;<see cref="Message{TKey, TValue}">Message</see>&lt;<see cref="byte[]"/>, <see cref="byte[]"/>&gt;&gt;</br>
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <param name="topicSelector"></param>
    /// <param name="configureMapping"></param>
    /// <returns></returns>
    public KafkaMessagingBuilder<TKafkaConfig> ConfigurePublisherTo<TValue>(Func<TKafkaConfig, KafkaProducerOptions> topicSelector, Action<DtoMapping<TValue>> configureMapping)
    {
        var producerOptions = _kafkaOptions.GetConfigFor(topicSelector);
        configureMapping(new DtoMapping<TValue>(_serviceCollection, producerOptions));
        return this;
    }

    /// <summary>
    /// To publish method after configuring publisher like this you need to use <see cref="IMessagePublisher.PublishAsync{TKey, TValue}(TKey, TValue, Action{Headers}?, CancellationToken)"/>
    /// You can also register:
    /// <br>- <see cref="IMessagePipelineLink{TKey, TValue}"/> as <see cref="IMessagePipelineLink{TKey, TValue}">IMessagePipelineLink</see>&lt;<typeparamref name="TKey"/>, <typeparamref name="TValue"/>&gt;</br>
    /// <br>- <see cref="IPipelineLink{TMessage}">IPipelineLink</see>&lt;<see cref="Message{TKey, TValue}">Message</see>&lt;<see cref="byte[]"/>, <see cref="byte[]"/>&gt;&gt;</br>
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <param name="topicSelector"></param>
    /// <returns></returns>
    public KafkaMessagingBuilder<TKafkaConfig> ConfigurePublisherTo<TKey, TValue>(Func<TKafkaConfig, KafkaProducerOptions> topicSelector) =>
        ConfigurePublisherTo<TValue>(topicSelector, x => x.ConfigureServiceCollectionForPublisher<TKey>());

    private void RegisterPool<TMessageProvider>(KafkaConsumerOptions consumerOptions, Func<IServiceProvider, TMessageProvider> factory)
        where TMessageProvider : class, IHostedService
    {
        // https://github.com/dotnet/runtime/issues/38751#issuecomment-1140062372
        var poolSize = consumerOptions.PoolSize.GetValueOrDefault(1);
        for (int i = 0; i < poolSize; i++)
        {
            _serviceCollection.AddSingleton<IHostedService>(factory);
        }
    }

    public class ConsumeResultMapping
    {
        private readonly IServiceCollection _serviceCollection;
        private readonly string _topicName;

        public ConsumeResultMapping(IServiceCollection serviceCollection, string topicName)
        {
            _serviceCollection = serviceCollection;
            _topicName = topicName;
        }

        /// <summary>
        /// After registering processor like this you need to register handlers <see cref="IMessageHandler{TKey, TValue}"/> as
        /// <see cref="IMessageHandler{TKey, TValue}">IMessageHandler</see>&lt;<typeparamref name="TKey"/>, <typeparamref name="TValue"/>&gt;.
        /// You can also register:
        /// <br>- <see cref="IPipelineLink{TMessage}">IPipelineLink</see>&lt;<see cref="ConsumeResult{TKey, TValue}">ConsumeResult</see>&lt;<see cref="byte[]"/>, <see cref="byte[]"/>&gt;&gt;</br>
        /// <br>- <see cref="IPipelineLink{TKey, TValue}"/> as <see cref="IPipelineLink{TKey, TValue}">IPipelineLink</see>&lt;<typeparamref name="TKey"/>, <typeparamref name="TValue"/>&gt;</br>
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="messageSelectionStrategy">You can find predefined strategies at <see cref="DispatcherStrategy"/></param>
        /// <returns></returns>
        public ConsumeResultMapping To<TKey, TValue>(ConsumeResultSelectionStrategy messageSelectionStrategy) =>
            ConfigureServiceCollectionForProcessor<TKey, TValue>(messageSelectionStrategy.And(ValueNotNull), Array.Empty<Type>());

        /// <summary>
        /// After registering processor like this you need to register handlers <see cref="IMessageHandler{TKey, TValue}"/> as
        /// <see cref="IMessageHandler{TKey, TValue}">IMessageHandler</see>&lt;<see cref="Ignore"/>, <typeparamref name="TValue"/>&gt;.
        /// You can also register:
        /// <br>- <see cref="IPipelineLink{TMessage}">IPipelineLink</see>&lt;<see cref="ConsumeResult{TKey, TValue}">ConsumeResult</see>&lt;<see cref="byte[]"/>, <see cref="byte[]"/>&gt;&gt;</br>
        /// <br>- <see cref="IPipelineLink{TKey, TValue}"/> as <see cref="IPipelineLink{TKey, TValue}">IPipelineLink</see>&lt;<see cref="Ignore"/>, <typeparamref name="TValue"/>&gt;</br>
        /// </summary>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="messageSelectionStrategy">You can find predefined strategies at <see cref="DispatcherStrategy"/></param>
        /// <returns></returns>
        public ConsumeResultMapping To<TValue>(ConsumeResultSelectionStrategy messageSelectionStrategy) =>
            ConfigureServiceCollectionForProcessor<Ignore, TValue>(messageSelectionStrategy.And(ValueNotNull), Array.Empty<Type>());

        /// <summary>
        /// After registering processor like this you need to register handlers <see cref="IMessageHandler{TKey, TValue}"/> as
        /// <see cref="IMessageHandler{TKey, TValue}">IMessageHandler</see>&lt;<typeparamref name="TKey"/>, <see cref="Null"/>&gt;.
        /// You can also register:
        /// <br>- <see cref="IPipelineLink{TMessage}">IPipelineLink</see>&lt;<see cref="ConsumeResult{TKey, TValue}">ConsumeResult</see>&lt;<see cref="byte[]"/>, <see cref="byte[]"/>&gt;&gt;</br>
        /// <br>- <see cref="IPipelineLink{TKey, TValue}"/> as <see cref="IPipelineLink{TKey, TValue}">IPipelineLink</see>&lt;<typeparamref name="TKey"/>, <see cref="Null"/>&gt;</br>
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="messageSelectionStrategy">You can find predefined strategies at <see cref="DispatcherStrategy"/></param>
        /// <returns></returns>
        public ConsumeResultMapping ToTombstone<TKey>(ConsumeResultSelectionStrategy messageSelectionStrategy, Action<ConcreteHandlerMapping<IMessageHandler<TKey, Null>, TKey, Null>>? configure = null)
        {
            var handlers = new ConcreteHandlerMapping<IMessageHandler<TKey, Null>, TKey, Null>();
            configure?.Invoke(handlers);
            return ConfigureServiceCollectionForProcessor<TKey, Null>(messageSelectionStrategy.And(Tombstones), handlers.Handlers);
        }

        private ConsumeResultMapping ConfigureServiceCollectionForProcessor<TKey, TValue>(ConsumeResultSelectionStrategy messageSelectionStrategy, Type[] concreteHandlers)
        {
            _serviceCollection
                .Configure<KafkaMessageDispatcherOptions>(_topicName, x => x.RegisterMessagePipelineFor<TKey, TValue>(messageSelectionStrategy, concreteHandlers));

            return this;
        }
    }

    public class BatchConsumeResultMapping
    {
        private readonly IServiceCollection _serviceCollection;
        private readonly string _topicName;

        public BatchConsumeResultMapping(IServiceCollection serviceCollection, string topicName)
        {
            _serviceCollection = serviceCollection;
            _topicName = topicName;
        }

        /// <summary>
        /// After registering processor like this you need to register handlers <see cref="IBatchMessageHandler{TKey, TValue}"/> as
        /// <see cref="IBatchMessageHandler{TKey, TValue}">IBatchMessageHandler</see>&lt;<typeparamref name="TKey"/>, <typeparamref name="TValue"/>&gt;.
        /// You can also register:
        /// <br>- <see cref="IBatchPipelineLink{TMessage}">IBatchPipelineLink</see>&lt;<see cref="ConsumeResult{TKey, TValue}">ConsumeResult</see>&lt;<see cref="byte[]"/>, <see cref="byte[]"/>&gt;&gt;</br>
        /// <br>- <see cref="IBatchPipelineLink{TKey, TValue}"/> as <see cref="IBatchPipelineLink{TKey, TValue}">IBatchPipelineLink</see>&lt;<typeparamref name="TKey"/>, <typeparamref name="TValue"/>&gt;</br>
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="messageSelectionStrategy">You can find predefined strategies at <see cref="DispatcherStrategy"/></param>
        /// <returns></returns>
        public BatchConsumeResultMapping To<TKey, TValue>(ConsumeResultSelectionStrategy messageSelectionStrategy) =>
            ConfigureServiceCollectionForBatchProcessor<TKey, TValue>(messageSelectionStrategy.And(ValueNotNull), Array.Empty<Type>());

        /// <summary>
        /// After registering processor like this you need to register handlers <see cref="IBatchMessageHandler{TKey, TValue}"/> as
        /// <see cref="IBatchMessageHandler{TKey, TValue}">IBatchMessageHandler</see>&lt;<see cref="Ignore"/>, <typeparamref name="TValue"/>&gt;.
        /// You can also register:
        /// <br>- <see cref="IBatchPipelineLink{TMessage}">IBatchPipelineLink</see>&lt;<see cref="ConsumeResult{TKey, TValue}">ConsumeResult</see>&lt;<see cref="byte[]"/>, <see cref="byte[]"/>&gt;&gt;</br>
        /// <br>- <see cref="IBatchPipelineLink{TKey, TValue}"/> as <see cref="IBatchPipelineLink{TKey, TValue}">IBatchPipelineLink</see>&lt;<see cref="Ignore"/>, <typeparamref name="TValue"/>&gt;</br>
        /// </summary>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="messageSelectionStrategy">You can find predefined strategies at <see cref="DispatcherStrategy"/></param>
        /// <returns></returns>
        public BatchConsumeResultMapping To<TValue>(ConsumeResultSelectionStrategy messageSelectionStrategy) =>
            ConfigureServiceCollectionForBatchProcessor<Ignore, TValue>(messageSelectionStrategy.And(ValueNotNull), Array.Empty<Type>());

        /// <summary>
        /// After registering processor like this you need to register handlers <see cref="IBatchMessageHandler{TKey, TValue}"/> as
        /// <see cref="IBatchMessageHandler{TKey, TValue}">IBatchMessageHandler</see>&lt;<typeparamref name="TKey"/>, <see cref="Null"/>&gt;.
        /// You can also register:
        /// <br>- <see cref="IBatchPipelineLink{TMessage}">IBatchPipelineLink</see>&lt;<see cref="ConsumeResult{TKey, TValue}">ConsumeResult</see>&lt;<see cref="byte[]"/>, <see cref="byte[]"/>&gt;&gt;</br>
        /// <br>- <see cref="IBatchPipelineLink{TKey, TValue}"/> as <see cref="IBatchPipelineLink{TKey, TValue}">IBatchPipelineLink</see>&lt;<typeparamref name="TKey"/>, <see cref="Null"/>&gt;</br>
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="messageSelectionStrategy">You can find predefined strategies at <see cref="DispatcherStrategy"/></param>
        /// <returns></returns>
        public BatchConsumeResultMapping ToTombstone<TKey>(ConsumeResultSelectionStrategy messageSelectionStrategy, Action<ConcreteHandlerMapping<IBatchMessageHandler<TKey, Null>, TKey, Null>>? configure = null)
        {
            var registrator = new ConcreteHandlerMapping<IBatchMessageHandler<TKey, Null>, TKey, Null>();
            configure?.Invoke(registrator);
            return ConfigureServiceCollectionForBatchProcessor<TKey, Null>(messageSelectionStrategy.And(Tombstones), registrator.Handlers);
        }

        private BatchConsumeResultMapping ConfigureServiceCollectionForBatchProcessor<TKey, TValue>(ConsumeResultSelectionStrategy messageSelectionStrategy, Type[] concreteHandlers)
        {
            _serviceCollection
                .Configure<KafkaMessageDispatcherOptions>(_topicName, x => x.RegisterMessagePipelineFor<TKey, TValue>(messageSelectionStrategy, concreteHandlers));

            return this;
        }
    }

    public class DtoMapping<TValue>
    {
        private readonly IServiceCollection _serviceCollection;
        private readonly KafkaProducerOptions _producerOptions;

        public DtoMapping(IServiceCollection serviceCollection, KafkaProducerOptions producerOptions)
        {
            _serviceCollection = serviceCollection;
            _producerOptions = producerOptions;
        }

        public void With<TKey>(Func<TValue, TKey> keySelector, Action<TValue, Headers>? headersSelector = null) =>
            ConfigureServiceCollectionForPublisher(keySelector, headersSelector);

        public void IgnoreKey(Action<TValue, Headers>? headersSelector = null) =>
            ConfigureServiceCollectionForPublisher<Ignore>(headersSelector: headersSelector);

        internal void ConfigureServiceCollectionForPublisher<TKey>(Func<TValue, TKey>? keySelector = null, Action<TValue, Headers>? headersSelector = null)
        {
            var messageConverter = new MessageConverter<TKey, TValue>(keySelector, headersSelector);
            var kafkaMessagePublisher = new KafkaRouter<TKey, TValue>(messageConverter, _producerOptions);
            _serviceCollection.AddSingleton<IKafkaRouter<TKey, TValue>>(kafkaMessagePublisher);
            _serviceCollection.AddSingleton<IKafkaRouterByValue<TValue>>(kafkaMessagePublisher);
        }
    }

    public class ConcreteHandlerMapping<TMessage, TKey, TValue>
    {
        private readonly List<Type> _handlers = new();

        public ConcreteHandlerMapping<TMessage, TKey, TValue> Using<THandler>() where THandler : TMessage
        {
            _handlers.Add(typeof(THandler));
            return this;
        }

        internal Type[] Handlers => _handlers.ToArray();
    }
}
