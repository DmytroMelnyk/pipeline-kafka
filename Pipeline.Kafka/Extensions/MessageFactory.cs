using Confluent.Kafka;

namespace Pipeline.Kafka.Extensions;

public class MessageFactory
{
    public static IKafkaMessage<TKey, T> CreateKafkaMessage<TKey, T>(TKey key, T value, Action<Headers>? configure = null, Timestamp? timestamp = default)
    {
        var headers = new Headers();
        configure?.Invoke(headers);
        return CreateKafkaMessage(key, value, headers, timestamp.GetValueOrDefault(Timestamp.Default));
    }

    public static IKafkaMessage<TKey, T> CreateKafkaMessage<TKey, T>(TKey key, T value, Headers headers, Timestamp timestamp) => new KafkaMessage<TKey, T>
    {
        Timestamp = timestamp,
        Value = value,
        Key = key,
        Headers = headers
    };

    public static IKafkaMessage<Ignore, T> CreateKafkaMessage<T>(T value, Action<Headers>? configure = null) =>
        CreateKafkaMessage(default(Ignore)!, value, configure, Timestamp.Default);

    public static IKafkaConsumeResult<TKey, T> CreateKafkaConsumeResult<TKey, T>(TKey key, T value, Action<Headers>? configure = null, Timestamp? timestamp = default, TopicPartitionOffset? topicPartitionOffset = null)
    {
        var headers = new Headers();
        configure?.Invoke(headers);
        return CreateKafkaConsumeResult(key, value, headers, timestamp.GetValueOrDefault(Timestamp.Default), topicPartitionOffset!);
    }

    public static IKafkaConsumeResult<TKey, T> CreateKafkaConsumeResult<TKey, T>(TKey key, T value, Headers headers, Timestamp timestamp, TopicPartitionOffset topicPartitionOffset) => new KafkaConsumeResult<TKey, T>
    {
        Timestamp = timestamp,
        Value = value,
        Key = key,
        Headers = headers,
        TopicPartitionOffset = topicPartitionOffset
    };

    private record KafkaMessage<TKey, TValue> : IKafkaMessage<TKey, TValue>
    {
        public required TKey Key
        {
            get; init;
        }

        public required Headers Headers
        {
            get; init;
        }

        public required TValue Value
        {
            get; init;
        }

        public Timestamp Timestamp
        {
            get; init;
        }
    }

    private sealed record KafkaConsumeResult<TKey, TValue> : KafkaMessage<TKey, TValue>, IKafkaConsumeResult<TKey, TValue>
    {
        public required TopicPartitionOffset TopicPartitionOffset
        {
            get; init;
        }
    }
}
