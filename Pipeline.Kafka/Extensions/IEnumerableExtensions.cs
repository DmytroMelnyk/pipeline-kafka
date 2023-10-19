using System.Collections;

namespace Pipeline.Kafka.Extensions;

public static class IEnumerableExtensions
{
    public static IBatch<T> ToReadOnlyBatch<T>(this IEnumerable<T> @this) => new Batch<T>(@this.ToList().AsReadOnly());

    public static IBatch<T> ToBatch<T>(this IEnumerable<T> @this) => new Batch<T>(@this.ToList());

    private sealed class Batch<T> : IBatch<T>
    {
        private readonly IList<T> _list;

        public Batch(IList<T> values)
        {
            ArgumentNullException.ThrowIfNull(values);
            _list = values;
        }

        public T this[int index] => _list[index];

        public bool IsReadOnly => _list.IsReadOnly;

        public int Count => _list.Count;

        public IEnumerator<T> GetEnumerator() => _list.GetEnumerator();

        public void RemoveAt(int index) => _list.RemoveAt(index);

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
