using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;

namespace Pipeline.Kafka.Router;

internal class MessagePublisher : IMessagePublisher
{
    private readonly IServiceProvider _serviceProvider;

    public MessagePublisher(IServiceProvider serviceProvider) => _serviceProvider = serviceProvider;

    public async Task PublishAsync<TValue>(TValue message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        await using (var scope = _serviceProvider.CreateAsyncScope())
        {
            var sp = scope.ServiceProvider;
            await Task.WhenAll(sp.GetServices<IKafkaRouterByValue<TValue>>().Select(x => x.PublishAsync(sp, message, cancellationToken)));
        }
    }

    public async Task PublishAsync<TKey, TValue>(TKey key, TValue message, Action<Headers>? configure, CancellationToken cancellationToken)
    {
        await using (var scope = _serviceProvider.CreateAsyncScope())
        {
            var sp = scope.ServiceProvider;
            await Task.WhenAll(sp.GetServices<IKafkaRouter<TKey, TValue>>().Select(x => x.PublishAsync(sp, key, message, configure, cancellationToken)));
        }
    }

    public async Task PublishAsync<TKey, TValue>(TKey key, TValue message, Headers headers, CancellationToken cancellationToken)
    {
        await using (var scope = _serviceProvider.CreateAsyncScope())
        {
            var sp = scope.ServiceProvider;
            await Task.WhenAll(sp.GetServices<IKafkaRouter<TKey, TValue>>().Select(x => x.PublishAsync(sp, key, message, headers, cancellationToken)));
        }
    }

    public async Task PublishTombstoneAsync<TValue>(TValue message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        await using (var scope = _serviceProvider.CreateAsyncScope())
        {
            var sp = scope.ServiceProvider;
            await Task.WhenAll(sp.GetServices<IKafkaRouterByValue<TValue>>().Select(x => x.PublishTombstoneAsync(sp, message, cancellationToken)));
        }
    }

    public async Task PublishTombstoneAsync<TKey, TValue>(TKey key, Action<Headers>? configure, CancellationToken cancellationToken)
    {
        await using (var scope = _serviceProvider.CreateAsyncScope())
        {
            var sp = scope.ServiceProvider;
            await Task.WhenAll(sp.GetServices<IKafkaRouter<TKey, TValue>>().Select(x => x.PublishTombstoneAsync(sp, key, configure, cancellationToken)));
        }
    }
}
