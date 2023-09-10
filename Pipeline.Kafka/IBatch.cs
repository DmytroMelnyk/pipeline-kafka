namespace Pipeline.Kafka;

public interface IBatch<out T> : IReadOnlyList<T>
{
    bool IsReadOnly { get; }

    void RemoveAt(int index);
}
