using System.Diagnostics.CodeAnalysis;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Pipeline.Kafka.Extensions;
using Pipeline.Kafka.Pipeline;

namespace Pipeline.Kafka.Dispatcher;

public class KafkaMessageDispatcherOptions
{
    private readonly List<IPipelineProvider> _handlers = new();

    public void RegisterMessagePipelineFor<TKey, TValue>(ConsumeResultSelectionStrategy messageSelectionStrategy, Type[] concreteHandlers)
    {
        _handlers.Add(new PipelineProvider<TKey, TValue>(messageSelectionStrategy, concreteHandlers));
    }

    public bool TryGetMessagePipeline(ConsumeResult<byte[], byte[]> consumeResult,
        [NotNullWhen(true)] out Func<IServiceProvider, ConsumeResult<byte[], byte[]>, CancellationToken, Task>? pipeline,
        [NotNullWhen(true)] out Func<IServiceProvider, IBatch<ConsumeResult<byte[], byte[]>>, CancellationToken, Task>? batchPipeline
        )
    {
        pipeline = default;
        batchPipeline = default;
        var handler = _handlers.SingleOrDefault(x => x.MessageSelectionStrategy(consumeResult));

        if (handler is not null)
        {
            pipeline = handler.Pipeline;
            batchPipeline = handler.BatchPipeline;
            return true;
        }

        return false;
    }

    private interface IPipelineProvider
    {
        bool MessageSelectionStrategy(ConsumeResult<byte[], byte[]> cr);

        Task Pipeline(IServiceProvider sp, ConsumeResult<byte[], byte[]> consumeResult, CancellationToken cancellationToken);

        Task BatchPipeline(IServiceProvider sp, IBatch<ConsumeResult<byte[], byte[]>> consumeResults, CancellationToken cancellationToken);
    }

    private class PipelineProvider<TKey, TValue> : IPipelineProvider
    {
        private static readonly Func<IServiceProvider, IEnumerable<IMessageHandler<TKey, TValue>>, DispatcherPipeline<TKey, TValue>> _factory =
            ActivatorUtilitiesEx.CreateFactory<IEnumerable<IMessageHandler<TKey, TValue>>, DispatcherPipeline<TKey, TValue>>();

        private static readonly Func<IServiceProvider, IEnumerable<IBatchMessageHandler<TKey, TValue>>, BatchDispatcherPipeline<TKey, TValue>> _batchFactory =
            ActivatorUtilitiesEx.CreateFactory<IEnumerable<IBatchMessageHandler<TKey, TValue>>, BatchDispatcherPipeline<TKey, TValue>>();


        private readonly ConsumeResultSelectionStrategy _messageSelectionStrategy;

        private readonly Type[] _concreteHandlers;

        public PipelineProvider(ConsumeResultSelectionStrategy messageSelectionStrategy, params Type[] concreteHandlers)
        {
            _messageSelectionStrategy = messageSelectionStrategy;
            _concreteHandlers = concreteHandlers;
        }

        protected virtual IEnumerable<T> GetMessageHandlers<T>(IServiceProvider sp) => _concreteHandlers.Any() ? _concreteHandlers.Select(sp.GetRequiredService).Cast<T>() : sp.GetServices<T>();

        public bool MessageSelectionStrategy(ConsumeResult<byte[], byte[]> cr) => _messageSelectionStrategy(cr, typeof(TKey), typeof(TValue));

        public async Task Pipeline(IServiceProvider sp, ConsumeResult<byte[], byte[]> consumeResult, CancellationToken cancellationToken)
        {
            await using (var scope = sp.CreateAsyncScope())
            {
                var pipeline = _factory(scope.ServiceProvider, GetMessageHandlers<IMessageHandler<TKey, TValue>>(scope.ServiceProvider));
                await pipeline.ExecuteAsync(consumeResult, cancellationToken);
            }
        }

        public async Task BatchPipeline(IServiceProvider sp, IBatch<ConsumeResult<byte[], byte[]>> consumeResults, CancellationToken cancellationToken)
        {
            await using (var scope = sp.CreateAsyncScope())
            {
                var pipeline = _batchFactory(scope.ServiceProvider, GetMessageHandlers<IBatchMessageHandler<TKey, TValue>>(scope.ServiceProvider));
                await pipeline.ExecuteAsync(consumeResults, cancellationToken);
            }
        }
    }
}
