using System.Diagnostics.CodeAnalysis;
using Confluent.Kafka;
using Pipeline.Kafka.Extensions;

namespace Pipeline.Kafka.Dispatcher;

public delegate bool ConsumeResultSelectionStrategy(ConsumeResult<byte[], byte[]> consumeResult, Type handlerKeyType, Type handlerValueType);

public static class DispatcherStrategy
{
    /// <summary>
    /// Strategy that looks for message type in header
    /// </summary>
    /// <param name="type">if not set it will be equal to message type name</param>
    /// <param name="typeKey">keyword that is responsible for type metadata</param>
    /// <returns></returns>
    [SuppressMessage("Major Code Smell", "S1172:Unused method parameters should be removed", Justification = "<Pending>")]
    public static ConsumeResultSelectionStrategy TypeOf(string? type = null, string typeKey = "type")
    {
        return type is null ? SelfDescribed : Header(typeKey, type);
        bool SelfDescribed(ConsumeResult<byte[], byte[]> consumeResult, Type handlerKeyType, Type handlerValueType) =>
            consumeResult.Message.Headers.TryGetValue(typeKey, out var header) && header == handlerValueType.Name;
    }

    /// <summary>
    /// Strategy that skips null messages (tombstones)
    /// </summary>
    public static ConsumeResultSelectionStrategy ValueNotNull
    {
        get
        {
            return ValueNotNullImpl;
            static bool ValueNotNullImpl(ConsumeResult<byte[], byte[]> cr, Type _, Type __) => cr.Message.Value != null;
        }
    }

    /// <summary>
    /// Strategy that reads tombstones
    /// </summary>
    public static ConsumeResultSelectionStrategy Tombstones
    {
        get
        {
            return TombstonesImpl;
            static bool TombstonesImpl(ConsumeResult<byte[], byte[]> cr, Type _, Type __) => cr.Message.Value == null;
        }
    }

    /// <summary>
    /// Strategy that reads all messages
    /// </summary>
    public static ConsumeResultSelectionStrategy ReadAll
    {
        get
        {
            return ReadAllImpl;
            static bool ReadAllImpl(ConsumeResult<byte[], byte[]> _, Type __, Type ___) => true;
        }
    }

    /// <summary>
    /// Strategy that looks for message version in header
    /// </summary>
    /// <param name="type">if not set it will be equal to message type name</param>
    /// <param name="typeKey">keyword that is responsible for type metadata</param>
    /// <returns></returns>
    public static ConsumeResultSelectionStrategy Version(string version, string versionKey = "version") =>
        Header(versionKey, version);

    private static ConsumeResultSelectionStrategy Header(string headerKey, string headerValue)
    {
        return HeaderStrategy;
        bool HeaderStrategy(ConsumeResult<byte[], byte[]> consumeResult, Type handlerKeyType, Type handlerValueType) => consumeResult.Message.Headers.TryGetValue(headerKey, out var header) && header == headerValue;
    }

    public static ConsumeResultSelectionStrategy And(this ConsumeResultSelectionStrategy left, ConsumeResultSelectionStrategy right) =>
        (consume, handlerKeyType, handlerValueType) => left(consume, handlerKeyType, handlerValueType) && right(consume, handlerKeyType, handlerValueType);

    public static ConsumeResultSelectionStrategy Or(this ConsumeResultSelectionStrategy left, ConsumeResultSelectionStrategy right) =>
        (consume, handlerKeyType, handlerValueType) => left(consume, handlerKeyType, handlerValueType) || right(consume, handlerKeyType, handlerValueType);
}
