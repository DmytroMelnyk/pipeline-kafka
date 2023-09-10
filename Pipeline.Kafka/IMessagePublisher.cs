using Confluent.Kafka;

namespace Pipeline.Kafka;

public interface IMessagePublisher
{
    /// <summary>
    /// Can be used to publish tumbstones
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <param name="key"></param>
    /// <param name="message"></param>
    /// <param name="configure"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task PublishAsync<TKey, TValue>(TKey key, TValue message, Action<Headers>? configure, CancellationToken cancellationToken);

    Task PublishAsync<TKey, TValue>(TKey key, TValue message, Headers headers, CancellationToken cancellationToken);

    Task PublishTombstoneAsync<TKey, TValue>(TKey key, Action<Headers>? configure, CancellationToken cancellationToken);

    Task PublishAsync<TValue>(TValue message, CancellationToken cancellationToken);

    Task PublishTombstoneAsync<TValue>(TValue message, CancellationToken cancellationToken);
}
