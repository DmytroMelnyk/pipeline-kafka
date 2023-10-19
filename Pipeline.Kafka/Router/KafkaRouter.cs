using Confluent.Kafka;
using Pipeline.Kafka.Config;
using Pipeline.Kafka.Extensions;
using Pipeline.Kafka.Pipeline;

namespace Pipeline.Kafka.Router;

internal interface IKafkaRouterByValue<in TValue>
{
    Task PublishAsync(IServiceProvider sp, TValue value, CancellationToken cancellationToken);

    Task PublishTombstoneAsync(IServiceProvider sp, TValue value, CancellationToken cancellationToken);
}

internal interface IKafkaRouter<in TKey, in TValue>
{
    Task PublishAsync(IServiceProvider sp, TKey key, TValue value, Action<Headers>? configure, CancellationToken cancellationToken);

    Task PublishAsync(IServiceProvider sp, TKey key, TValue value, Headers headers, CancellationToken cancellationToken);

    Task PublishTombstoneAsync(IServiceProvider sp, TKey key, Action<Headers>? configure, CancellationToken cancellationToken);
}

internal class KafkaRouter<TKey, TValue> : IKafkaRouterByValue<TValue>, IKafkaRouter<TKey, TValue>
{
    private static readonly Func<IServiceProvider, KafkaProducerOptions, RouterPipeline<TKey, TValue>> _factoryByOptions =
        ActivatorUtilitiesEx.CreateFactory<KafkaProducerOptions, RouterPipeline<TKey, TValue>>();
    private static readonly Func<IServiceProvider, KafkaProducerOptions, RouterPipeline<TKey, Null>> _tombstoneFactoryByOptions =
        ActivatorUtilitiesEx.CreateFactory<KafkaProducerOptions, RouterPipeline<TKey, Null>>();

    private readonly MessageConverter<TKey, TValue> _converter;
    private readonly Func<IServiceProvider, RouterPipeline<TKey, TValue>> _routerPipelineFactory;
    private readonly Func<IServiceProvider, RouterPipeline<TKey, Null>> _tombstoneRouterPipelineFactory;

    public KafkaRouter(MessageConverter<TKey, TValue> converter, KafkaProducerOptions kafkaProducerOptions)
    {
        _converter = converter;
        _routerPipelineFactory = sp => _factoryByOptions(sp, kafkaProducerOptions);
        _tombstoneRouterPipelineFactory = sp => _tombstoneFactoryByOptions(sp, kafkaProducerOptions);
    }

    public Task PublishAsync(IServiceProvider sp, TValue value, CancellationToken cancellationToken) =>
        _routerPipelineFactory(sp).PublishAsync(_converter.ToKafkaMessage(value), cancellationToken);

    public Task PublishTombstoneAsync(IServiceProvider sp, TValue value, CancellationToken cancellationToken) =>
        _tombstoneRouterPipelineFactory(sp).PublishAsync(_converter.ToTombstoneUsingKeySelector(value), cancellationToken);

    public Task PublishAsync(IServiceProvider sp, TKey key, TValue value, Action<Headers>? configure, CancellationToken cancellationToken) =>
        _routerPipelineFactory(sp).PublishAsync(MessageConverter<TKey, TValue>.ToKafkaMessage(key, value, configure), cancellationToken);

    public Task PublishAsync(IServiceProvider sp, TKey key, TValue value, Headers headers, CancellationToken cancellationToken) =>
        _routerPipelineFactory(sp).PublishAsync(MessageConverter<TKey, TValue>.ToKafkaMessage(key, value, headers), cancellationToken);

    public Task PublishTombstoneAsync(IServiceProvider sp, TKey key, Action<Headers>? configure, CancellationToken cancellationToken) =>
        _tombstoneRouterPipelineFactory(sp).PublishAsync(MessageConverter<TKey, TValue>.ToTombstone(key, configure), cancellationToken);

}
